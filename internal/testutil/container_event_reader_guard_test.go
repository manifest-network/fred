package testutil

// This repository-level guard keeps the Docker container event loop's reader
// from waiting on anything but its own stream (ENG-799). dockerd skips events
// for a subscriber that falls behind, and a skipped "kill" would make the
// following "die" count against the tenant's terminal budget. Production logs
// synchronously to stdout, so a single log call on the event path is enough
// to stall it under output backpressure.
//
// The reader, containerEventReader, holds no logger and no backend, so the
// compiler already keeps b.logger out of its reach. Its file is also a closed
// world, which this guard enforces:
//
//   - it imports only readerImports;
//   - every name it references is declared in the file, imported, one of
//     readerBuiltins, or one of the few package docker names in
//     readerPackageNames, so it reaches neither the Backend nor a package
//     helper (print and println write to stderr and are not among the
//     builtins; neither are close, panic and recover);
//   - it declares no function type, interface type or function literal, the
//     shapes through which a caller could still hand it a logger;
//   - it sends on a channel only as a case of a select that has a default
//     case, so it never waits on the recorder or the reporter;
//   - it receives only as a case of a select in consume, and only from the
//     subscription's two streams (consume's <-chan ContainerEvent and
//     <-chan error parameters) and its own stop field, none of which consume
//     rebinds; and it has no range statement, since a range over a channel
//     waits on it and the guard cannot see types.
//
// A name declared anywhere in the file counts as declared everywhere in it.
// That over-approximation can hide a package name only behind a declaration
// of the very same name in this file, in plain sight.
//
// Go lets any file of package docker add methods to the reader's types, so
// the closure is checked package-wide too (readerPackageFindings): no other
// production file declares a method on, or an alias of, a type the reader
// file declares or the reader's event type; the event type is a plain struct
// of predeclared values; its action names are literal constants; and its one
// metric is a client_golang collector that no other code reassigns, so every
// method the reader calls on it is client_golang's. The reader's one
// in-repo import, failurecause, imports nothing, prints nothing and declares
// no channel, so no method of its event session can log or wait
// (readerDependencyFindings).
//
// The relay that feeds the reader, DockerClient.ContainerEvents, runs on the
// same event path but must hold the Docker SDK client, so it is held to an
// allowlist of calls instead (relayCalls). The one package docker method it
// calls, dockerSDKView.Events, is resolved and held to calling only the SDK
// function its view was built with, cli.Events of a *client.Client
// (relayHelperFindings). What stays trusted is foreign code: the Docker SDK
// and client_golang.
//
// TestContainerEventReaderGuardsFire proves every rule fires. Which function
// may hold the event session is pinned by terminal_budget_authority_test.go.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The reader's file and method are eventReaderSite, the one site
// terminal_budget_authority_test.go lets hold the event session.
const (
	containerEventRelayFile   = "internal/backend/docker/lifecycle.go"
	containerEventRelayMethod = "DockerClient.ContainerEvents"
	// containerEventRelayHelper is the one package docker method the relay
	// calls, as d.client.Events.
	containerEventRelayHelper = "dockerSDKView.Events"
	dockerFiltersImportPath   = "github.com/docker/docker/api/types/filters"
	dockerClientImportPath    = "github.com/docker/docker/client"
	// readerStopField is the reader's stop signal, one of the three channels
	// it may wait on.
	readerStopField = "stop"
)

var (
	// readerImports are the only packages the reader's file may import.
	readerImports = []string{"sync/atomic", failurecauseImportPath}
	// readerBuiltins are the only predeclared names the reader's file may use.
	readerBuiltins = []string{
		"_", "bool", "error", "false", "float64", "int", "len", "make", "nil", "string", "true", "uint64",
	}
	// readerPackageNames are the only names from the rest of package docker
	// the reader's file may use: the event type and its actions, and the drop
	// counter it updates, a client_golang collector whose updates are atomic.
	// The queue depth is sampled by the recorder and the dispatcher, on the
	// dispatcher's queue.
	readerPackageNames = []string{
		"ContainerEvent", "containerEventStart", "containerEventKill", "containerEventDie",
		"eventLoopDeathsDropped",
	}
	// readerStreamElements are the element types of consume's channel
	// parameters that are the subscription's streams.
	readerStreamElements = []string{"ContainerEvent", "error"}
	// readerValueTypes are the field and constant types the reader's event
	// type and actions may have: values with no methods.
	readerValueTypes = []string{"bool", "float64", "int", "string", "uint64"}
	// metricImports are the packages whose constructors may build the metrics
	// the reader updates.
	metricImports = []string{
		"github.com/prometheus/client_golang/prometheus",
		"github.com/prometheus/client_golang/prometheus/promauto",
	}
	// relayCalls are the only calls DockerClient.ContainerEvents may make, by
	// callee as written, with a package qualifier resolved to its import path.
	relayCalls = []string{
		dockerFiltersImportPath + ".NewArgs", dockerFiltersImportPath + ".Arg",
		"filter.Add",      // filters.Args.Add: the relay binds filter only to filters.NewArgs
		"d.client.Events", // containerEventRelayHelper, checked by relayHelperFindings
		"ctx.Done",        // the caller's context.Context, which the relay never rebinds
		"close", "make", "string",
	}
	// outputBuiltins write to the process's standard error.
	outputBuiltins = []string{"print", "println"}
)

func TestContainerEventReaderIsClosed(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	pkg := parseRepoDir(t, fset, root, path.Dir(eventReaderSite.file))
	reader := pkg[eventReaderSite.file]
	if reader == nil || !declaresFunction(reader, eventReaderSite.function) {
		t.Fatalf("%s no longer declares %s; point this guard at the reader", eventReaderSite.file, eventReaderSite.function)
	}
	relay := pkg[containerEventRelayFile]
	if relay == nil || !declaresFunction(relay, containerEventRelayMethod) {
		t.Fatalf("%s no longer declares %s; point this guard at the relay", containerEventRelayFile, containerEventRelayMethod)
	}
	findings := containerEventReaderFindings(eventReaderSite.file, reader, fset)
	findings = append(findings, readerPackageFindings(eventReaderSite.file, pkg, fset)...)
	findings = append(findings, readerDependencyFindings(parseRepoDir(t, fset, root, failurecauseDir), fset)...)
	findings = append(findings, containerEventRelayFindings(containerEventRelayFile, relay, fset)...)
	findings = append(findings, relayHelperFindings(pkg, fset)...)
	if len(findings) > 0 {
		t.Errorf("the container event path can reach a logger or wait (ENG-799):\n  %s", strings.Join(findings, "\n  "))
	}
}

func TestContainerEventReaderGuardsFire(t *testing.T) {
	const readerType = "type containerEventReader struct{}\n"
	tests := []struct {
		name string
		src  string
		want string
	}{
		{"slog", "package docker\nimport \"log/slog\"\n" + readerType +
			`func (r containerEventReader) enqueue() { slog.Warn("dropped") }`, "imports log/slog"},
		{"slog under an alias", "package docker\nimport l \"log/slog\"\n" + readerType +
			`func (r containerEventReader) enqueue() { l.Warn("dropped") }`, "imports log/slog"},
		{"fmt to stdout", "package docker\nimport \"fmt\"\n" + readerType +
			`func (r containerEventReader) enqueue() { fmt.Println("dropped") }`, "imports fmt"},
		{"the backend's logger", `package docker
type containerEventReader struct{ b *Backend }
func (r containerEventReader) enqueue() { r.b.logger.Warn("dropped") }`, "names Backend"},
		{"a package helper that logs", "package docker\n" + readerType +
			`func (r containerEventReader) enqueue() { warnDroppedDeath("c") }`, "names warnDroppedDeath"},
		{"a metric label lookup", "package docker\n" + readerType +
			`func (r containerEventReader) enqueue() { dieEventDroppedTotal.WithLabelValues("event_loop").Inc() }`,
			"names dieEventDroppedTotal"},
		{"println to stderr", "package docker\n" + readerType +
			`func (r containerEventReader) enqueue() { println("dropped") }`, "names println"},
		{"closing the dispatcher's queue", `package docker
type containerEventReader struct{ deaths chan<- int }
func (r containerEventReader) stop() { close(r.deaths) }`, "names close"},
		{"a callback field", `package docker
type containerEventReader struct{ onDrop func() }
func (r containerEventReader) enqueue() { r.onDrop() }`, "declares a function type"},
		{"a callback parameter", "package docker\n" + readerType +
			`func (r containerEventReader) enqueue(onDrop func()) { onDrop() }`, "declares a function type"},
		{"a logger behind an interface", `package docker
type containerEventReader struct{ log interface{ Warn(string, ...any) } }
func (r containerEventReader) enqueue() { r.log.Warn("dropped") }`, "declares an interface type"},
		{"a function literal", "package docker\n" + readerType +
			`func (r containerEventReader) enqueue() { report := func() {}; report() }`, "declares a function literal"},
		{"a blocking send", `package docker
type containerEventReader struct{ deaths chan<- int }
func (r containerEventReader) enqueue() { r.deaths <- 1 }`, "sends outside a select with a default case"},
		{"a send that waits for shutdown", `package docker
type containerEventReader struct{ deaths chan<- int; stop <-chan struct{} }
func (r containerEventReader) consume() {
	select {
	case r.deaths <- 1:
	case <-r.stop:
	}
}`, "sends outside a select with a default case"},
		{"a bare receive", `package docker
type containerEventReader struct{ stop <-chan struct{} }
func (r containerEventReader) consume() { <-r.stop }`, "receives outside consume's select over its streams"},
		{"waiting on the reporter's wakeup", `package docker
type containerEventReader struct{ stop, wake <-chan struct{} }
func (r containerEventReader) consume() {
	select {
	case <-r.stop:
	case <-r.wake:
	}
}`, "receives outside consume's select over its streams"},
		{"a non-blocking receive outside consume", `package docker
type containerEventReader struct{ stop <-chan struct{} }
func (r containerEventReader) enqueue() {
	select {
	case <-r.stop:
	default:
	}
}`, "receives outside consume's select over its streams"},
		{"a channel parameter that is not a stream", `package docker
type containerEventReader struct{}
func (r containerEventReader) consume(events <-chan ContainerEvent, wake <-chan struct{}) {
	select {
	case <-events:
	case <-wake:
	}
}`, "receives outside consume's select over its streams"},
		{"a rebound stream", `package docker
type containerEventReader struct{}
func (r containerEventReader) consume(events <-chan ContainerEvent) {
	events = nil
	select {
	case <-events:
	}
}`, "rebinds events"},
		{"a range over a channel", "package docker\n" + readerType +
			`func (r containerEventReader) drain(events <-chan ContainerEvent) { for range events {} }`, "ranges"},
	}
	for _, test := range tests {
		t.Run("reader: "+test.name, func(t *testing.T) {
			findings := parseAndCheck(t, test.src, containerEventReaderFindings)
			if len(findings) != 1 || !strings.Contains(findings[0], test.want) {
				t.Fatalf("want exactly one finding containing %q, got %q", test.want, findings)
			}
		})
	}

	// The reader's types and dependencies, package-wide: other.go is a second
	// file of package docker beside reader.go.
	const readerFile = `package docker
type containerEventReader struct{}
type containerDeathOverflow struct{}
func (o *containerDeathOverflow) record() {}
`
	packageTests := []struct {
		name  string
		other string
		want  string
	}{
		{"a logging method on the overflow record in another file", `package docker
import "log/slog"
func (o *containerDeathOverflow) warn() { slog.Warn("dropped") }`, "declares containerDeathOverflow.warn outside reader.go"},
		{"a method on the reader in another file", `package docker
func (r containerEventReader) warn() {}`, "declares containerEventReader.warn outside reader.go"},
		{"a method through an alias", `package docker
type loud = containerDeathOverflow
func (l *loud) warn() {}`, "aliases containerDeathOverflow outside reader.go"},
		{"a method on the event type", `package docker
type ContainerEvent struct{ ContainerID string }
func (e ContainerEvent) warn() {}`, "declares ContainerEvent.warn outside reader.go"},
		{"a callback on the event type", `package docker
type ContainerEvent struct{ ContainerID string; onDie func() }`, "gives ContainerEvent a field of type func()"},
		{"an embedded field on the event type", `package docker
type ContainerEvent struct{ *Backend; ContainerID string }`, "embeds *Backend in ContainerEvent"},
		{"a typed action", `package docker
type action string
const containerEventDie action = "die"`, "declares containerEventDie with type action"},
		{"a metric the package wraps", `package docker
var eventLoopDeathsDropped = newLoudCounter()`, "eventLoopDeathsDropped is not a client_golang collector"},
		{"a metric swapped after startup", `package docker
import "github.com/prometheus/client_golang/prometheus/promauto"
var eventLoopDeathsDropped = promauto.NewCounter(opts)
func init() { eventLoopDeathsDropped = loudCounter{} }`, "assigns eventLoopDeathsDropped"},
	}
	for _, test := range packageTests {
		t.Run("package: "+test.name, func(t *testing.T) {
			files, fset := parseSyntheticFiles(t, map[string]string{"reader.go": readerFile, "other.go": test.other})
			findings := readerPackageFindings("reader.go", files, fset)
			if len(findings) != 1 || !strings.Contains(findings[0], test.want) {
				t.Fatalf("want exactly one finding containing %q, got %q", test.want, findings)
			}
		})
	}

	dependencyTests := []struct {
		name string
		src  string
		want string
	}{
		{"a logging import", "package failurecause\nimport \"log/slog\"\nfunc (s *eventSession) ObserveExit() { slog.Warn(\"exit\") }",
			"imports log/slog"},
		{"println", "package failurecause\nfunc (s *eventSession) ObserveExit() { println(\"exit\") }", "names println"},
		{"a channel", "package failurecause\ntype eventSession struct{ done chan struct{} }", "declares a channel type"},
	}
	for _, test := range dependencyTests {
		t.Run("dependency: "+test.name, func(t *testing.T) {
			files, fset := parseSyntheticFiles(t, map[string]string{"provenance.go": test.src})
			findings := readerDependencyFindings(files, fset)
			if len(findings) != 1 || !strings.Contains(findings[0], test.want) {
				t.Fatalf("want exactly one finding containing %q, got %q", test.want, findings)
			}
		})
	}

	const relayHeader = "package docker\nimport (\n\t\"context\"\n\t\"fmt\"\n\t\"log/slog\"\n\t\"os\"\n)\n"
	relayTests := []struct {
		name string
		body string
		want string
	}{
		{"slog", `slog.Debug("docker event")`, "calls log/slog.Debug"},
		{"fmt to stdout", `fmt.Println("docker event")`, "calls fmt.Println"},
		{"os.Stderr", `_, _ = os.Stderr.WriteString("docker event")`, "calls os.Stderr.WriteString"},
		{"a logger field", `d.logger.Warn("docker event")`, "calls d.logger.Warn"},
		{"println", `println("docker event")`, "calls println"},
		{"a package helper", `logDockerEvent("docker event")`, "calls logDockerEvent"},
		{"a method declared elsewhere", `d.report("docker event")`, "calls d.report"},
		{"a filter that is not the SDK's", `filter := loudFilter{}; filter.Add("label", "x")`, "binds filter"},
		{"a rebound receiver", `d := loudClient{}; _ = d`, "rebinds d"},
	}
	for _, test := range relayTests {
		t.Run("relay: "+test.name, func(t *testing.T) {
			src := relayHeader + "func (d *DockerClient) ContainerEvents(ctx context.Context) {\n" +
				"\tgo func() { " + test.body + " }()\n}"
			findings := parseAndCheck(t, src, containerEventRelayFindings)
			if len(findings) != 1 || !strings.Contains(findings[0], test.want) {
				t.Fatalf("want exactly one finding containing %q, got %q", test.want, findings)
			}
		})
	}

	const sdkView = `package docker
import (
	"context"

	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/client"
)
type DockerClient struct{ client dockerSDKView }
type dockerSDKView struct {
	events func(context.Context, events.ListOptions) (<-chan events.Message, <-chan error)
}
func newDockerSDKView(cli *client.Client) dockerSDKView { return dockerSDKView{events: cli.Events} }
func (v dockerSDKView) Events(ctx context.Context, opts events.ListOptions) (<-chan events.Message, <-chan error) {
	return v.events(ctx, opts)
}
`
	helperTests := []struct {
		name, old, replacement, want string
	}{
		{"a helper that logs", "\treturn v.events(ctx, opts)", "\tprintln(\"events\")\n\treturn v.events(ctx, opts)",
			"dockerSDKView.Events calls println"},
		{"a view built around a wrapper", "dockerSDKView{events: cli.Events}", "dockerSDKView{events: loudEvents(cli)}",
			"sets dockerSDKView.events to loudEvents(cli)"},
		{"a view built without the SDK client", "(cli *client.Client)", "(cli loudClient)",
			"sets dockerSDKView.events to cli.Events"},
		{"a client field that is not the view", "struct{ client dockerSDKView }", "struct{ client loudView }",
			"DockerClient.client is not a dockerSDKView"},
		{"an events field swapped later", "func newDockerSDKView", "func (v *dockerSDKView) wrap() { v.events = nil }\nfunc newDockerSDKView",
			"assigns v.events"},
		{"no helper left to resolve", "func (v dockerSDKView) Events", "func (v dockerSDKView) Watch",
			"no longer declares dockerSDKView.Events"},
	}
	for _, test := range helperTests {
		t.Run("relay helper: "+test.name, func(t *testing.T) {
			src := strings.Replace(sdkView, test.old, test.replacement, 1)
			if src == sdkView {
				t.Fatalf("control %q does not apply to the view's source", test.name)
			}
			files, fset := parseSyntheticFiles(t, map[string]string{"docker_sdk.go": src})
			findings := relayHelperFindings(files, fset)
			if len(findings) != 1 || !strings.Contains(findings[0], test.want) {
				t.Fatalf("want exactly one finding containing %q, got %q", test.want, findings)
			}
		})
	}

	// Negative controls: the sanctioned shapes must stay silent.
	allowed := []struct {
		name  string
		src   string
		check func(rel string, file *ast.File, fset *token.FileSet) []string
	}{
		{"the reader's shape", `package docker
import (
	"sync/atomic"

	fc "github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
)
type containerEventReader struct {
	stop     <-chan struct{}
	deaths   chan<- fc.Provenance
	overflow *containerDeathOverflow
}
func (r containerEventReader) consume(events <-chan ContainerEvent, errs <-chan error) (delivered bool, streamErr error) {
	session := fc.NewEventSession()
	for {
		select {
		case <-r.stop:
			return delivered, nil
		case event, ok := <-events:
			if !ok {
				return delivered, nil
			}
			delivered = true
			switch event.Action {
			case containerEventStart:
				session.ObserveStart(event.ContainerID)
			case containerEventDie:
				r.enqueue(session.ObserveExit(event.ContainerID))
			}
		case err := <-errs:
			return delivered, err
		}
	}
}
func (r containerEventReader) enqueue(death fc.Provenance) {
	select {
	case r.deaths <- death:
	default:
		r.overflow.record()
	}
}
type containerDeathOverflow struct {
	unreported atomic.Uint64
	wake       chan struct{}
}
func newContainerDeathOverflow() *containerDeathOverflow {
	return &containerDeathOverflow{wake: make(chan struct{}, 1)}
}
func (o *containerDeathOverflow) record() {
	eventLoopDeathsDropped.Inc()
	o.unreported.Add(1)
	select {
	case o.wake <- struct{}{}:
	default:
	}
}
func (o *containerDeathOverflow) wakeups() <-chan struct{} { return o.wake }`, containerEventReaderFindings},
		{"the relay's shape, beside a function that logs", `package docker
import (
	"context"
	"log/slog"

	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
)
func (d *DockerClient) ContainerEvents(ctx context.Context) (<-chan ContainerEvent, <-chan error) {
	filter := filters.NewArgs(filters.Arg("type", string(events.ContainerEventType)))
	if d.backendName != "" {
		filter.Add("label", "backend="+d.backendName)
	}
	messages, errs := d.client.Events(ctx, events.ListOptions{Filters: filter})
	out, errCh := make(chan ContainerEvent), make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errCh)
		for {
			select {
			case <-ctx.Done():
				return
			case message, ok := <-messages:
				if !ok {
					return
				}
				select {
				case out <- ContainerEvent{ContainerID: message.Actor.ID, Action: string(message.Action)}:
				case <-ctx.Done():
					return
				}
			case err := <-errs:
				errCh <- err
				return
			}
		}
	}()
	return out, errCh
}
func (d *DockerClient) Other() { slog.Warn("elsewhere in the file") }`, containerEventRelayFindings},
	}
	for _, control := range allowed {
		if findings := parseAndCheck(t, control.src, control.check); len(findings) != 0 {
			t.Errorf("sanctioned shape %q reported: %q", control.name, findings)
		}
	}

	allowedPackages := []struct {
		name  string
		files map[string]string
		check func(files map[string]*ast.File, fset *token.FileSet) []string
	}{
		{"the reader's types beside the rest of the package", map[string]string{
			"reader.go": readerFile,
			"backend.go": `package docker
import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)
type ContainerEvent struct {
	ContainerID string
	Action      string
}
const (
	containerEventStart = "start"
	containerEventKill  = "kill"
	containerEventDie   = "die"
)
var (
	containerDeathQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{Name: "depth"})
	dieEventDroppedTotal     = promauto.NewCounterVec(prometheus.CounterOpts{Name: "dropped"}, []string{"source"})
	eventLoopDeathsDropped   = dieEventDroppedTotal.WithLabelValues("event_loop")
)
func (b *Backend) reportDrops(o *containerDeathOverflow) { slog.Warn("dropped", "count", o) }`,
		}, func(files map[string]*ast.File, fset *token.FileSet) []string {
			return readerPackageFindings("reader.go", files, fset)
		}},
		{"the SDK view", map[string]string{"docker_sdk.go": sdkView}, relayHelperFindings},
		{"failurecause's shape", map[string]string{"provenance.go": `package failurecause
type eventSession struct{ runs map[string]uint8 }
func NewEventSession() *eventSession { return &eventSession{runs: make(map[string]uint8)} }
func (s *eventSession) ObserveStart(id string) { s.runs[id] = 1 }`}, readerDependencyFindings},
	}
	for _, control := range allowedPackages {
		files, fset := parseSyntheticFiles(t, control.files)
		if findings := control.check(files, fset); len(findings) != 0 {
			t.Errorf("sanctioned shape %q reported: %q", control.name, findings)
		}
	}
}

// reporter appends position-prefixed findings for one rule set.
func reporter(fset *token.FileSet, findings *[]string) func(rel string, node ast.Node, format string, args ...any) {
	return func(rel string, node ast.Node, format string, args ...any) {
		*findings = append(*findings, fmt.Sprintf("%s (%s): ", fset.Position(node.Pos()), rel)+fmt.Sprintf(format, args...))
	}
}

// containerEventReaderFindings reports everything in the reader's file that
// reaches outside its closed world; see the file comment.
func containerEventReaderFindings(rel string, file *ast.File, fset *token.FileSet) []string {
	var findings []string
	reportAt := reporter(fset, &findings)
	report := func(node ast.Node, format string, args ...any) { reportAt(rel, node, format, args...) }
	imported := make(map[string]bool)
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			report(spec, "has an unreadable import %s", spec.Path.Value)
			continue
		}
		name := path.Base(importPath)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imported[name] = true
		if !slices.Contains(readerImports, importPath) {
			report(spec, "imports %s, outside the reader's closed world", importPath)
		}
	}
	declared, declaring := fileDeclarations(file)
	selected := selectedIdents(file)
	nonBlocking := nonBlockingSends(file)
	streamReceives := sanctionedReceives(file, report)
	signatures := make(map[*ast.FuncType]bool)
	for _, decl := range file.Decls {
		if function, ok := decl.(*ast.FuncDecl); ok {
			signatures[function.Type] = true
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.ImportSpec:
			return false
		case *ast.FuncLit:
			report(typed, "declares a function literal")
			return false
		case *ast.FuncType:
			if !signatures[typed] {
				report(typed, "declares a function type")
				return false
			}
		case *ast.InterfaceType:
			report(typed, "declares an interface type")
			return false
		case *ast.SendStmt:
			if !nonBlocking[typed] {
				report(typed, "sends outside a select with a default case")
			}
		case *ast.UnaryExpr:
			if typed.Op == token.ARROW && !streamReceives[typed] {
				report(typed, "receives outside consume's select over its streams")
			}
		case *ast.RangeStmt:
			report(typed, "ranges, which waits when the operand is a channel")
		case *ast.Ident:
			if typed == file.Name || declaring[typed] || selected[typed] {
				return true
			}
			name := typed.Name
			if declared[name] || imported[name] ||
				slices.Contains(readerBuiltins, name) || slices.Contains(readerPackageNames, name) {
				return true
			}
			report(typed, "names %s, outside the reader's closed world", name)
		}
		return true
	})
	return findings
}

// sanctionedReceives are the receives the reader may wait on: the comm cases
// of a select in eventReaderSite.function whose channel is one of its stream
// parameters or its receiver's stop field. It reports any rebinding in that
// method of the names those channels are reached through.
func sanctionedReceives(file *ast.File, report func(node ast.Node, format string, args ...any)) map[*ast.UnaryExpr]bool {
	receives := make(map[*ast.UnaryExpr]bool)
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok || function.Body == nil || functionKey(function) != eventReaderSite.function {
			continue
		}
		bound := make(map[string]bool)
		streams := make(map[string]bool)
		for _, param := range function.Type.Params.List {
			channel, ok := param.Type.(*ast.ChanType)
			if !ok || channel.Dir != ast.RECV {
				continue
			}
			element, ok := channel.Value.(*ast.Ident)
			if !ok || !slices.Contains(readerStreamElements, element.Name) {
				continue
			}
			for _, name := range param.Names {
				streams[name.Name], bound[name.Name] = true, true
			}
		}
		receiver := ""
		if function.Recv != nil && len(function.Recv.List[0].Names) == 1 {
			receiver = function.Recv.List[0].Names[0].Name
			bound[receiver] = true
		}
		isStream := func(channel ast.Expr) bool {
			switch typed := channel.(type) {
			case *ast.Ident:
				return streams[typed.Name]
			case *ast.SelectorExpr:
				owner, ok := typed.X.(*ast.Ident)
				return ok && receiver != "" && owner.Name == receiver && typed.Sel.Name == readerStopField
			}
			return false
		}
		rebinds := func(node ast.Node, target ast.Expr) {
			if root := rootIdent(target); root != nil && bound[root.Name] {
				report(node, "rebinds %s, a channel consume waits on", root.Name)
			}
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.AssignStmt:
				for _, target := range typed.Lhs {
					rebinds(typed, target)
				}
			case *ast.ValueSpec:
				for _, name := range typed.Names {
					rebinds(typed, name)
				}
			case *ast.UnaryExpr:
				if typed.Op == token.AND {
					rebinds(typed, typed.X)
				}
			case *ast.SelectStmt:
				for _, clause := range typed.Body.List {
					comm, ok := clause.(*ast.CommClause)
					if !ok {
						continue
					}
					if receive := commReceive(comm.Comm); receive != nil && isStream(receive.X) {
						receives[receive] = true
					}
				}
			}
			return true
		})
	}
	return receives
}

// commReceive is the receive a select case waits on, if it is one.
func commReceive(comm ast.Stmt) *ast.UnaryExpr {
	var expr ast.Expr
	switch typed := comm.(type) {
	case *ast.ExprStmt:
		expr = typed.X
	case *ast.AssignStmt:
		if len(typed.Rhs) == 1 {
			expr = typed.Rhs[0]
		}
	}
	if receive, ok := expr.(*ast.UnaryExpr); ok && receive.Op == token.ARROW {
		return receive
	}
	return nil
}

// rootIdent is the variable an assignable expression is reached through.
func rootIdent(expr ast.Expr) *ast.Ident {
	for {
		switch typed := expr.(type) {
		case *ast.Ident:
			return typed
		case *ast.SelectorExpr:
			expr = typed.X
		case *ast.IndexExpr:
			expr = typed.X
		case *ast.StarExpr:
			expr = typed.X
		case *ast.ParenExpr:
			expr = typed.X
		default:
			return nil
		}
	}
}

// receiverBase is the type name a method receiver or type expression is
// declared on.
func receiverBase(expr ast.Expr) string {
	for {
		switch typed := expr.(type) {
		case *ast.Ident:
			return typed.Name
		case *ast.StarExpr:
			expr = typed.X
		case *ast.ParenExpr:
			expr = typed.X
		case *ast.IndexExpr:
			expr = typed.X
		case *ast.IndexListExpr:
			expr = typed.X
		default:
			return ""
		}
	}
}

// packageValue is one top-level constant or variable of a package.
type packageValue struct {
	rel   string
	tok   token.Token
	spec  *ast.ValueSpec
	index int
}

// readerPackageFindings closes the reader over the rest of its package: the
// methods of its types, its event type, its action constants and its
// metrics; see the file comment. files are the package's production files by
// repository-relative path.
func readerPackageFindings(readerRel string, files map[string]*ast.File, fset *token.FileSet) []string {
	var findings []string
	report := reporter(fset, &findings)
	readerTypes := make(map[string]bool)
	for _, decl := range files[readerRel].Decls {
		if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.TYPE {
			for _, spec := range gen.Specs {
				readerTypes[spec.(*ast.TypeSpec).Name.Name] = true
			}
		}
	}
	others := slices.DeleteFunc(slices.Sorted(maps.Keys(files)), func(rel string) bool { return rel == readerRel })
	values := make(map[string]packageValue)
	for _, rel := range others {
		for _, decl := range files[rel].Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				switch spec := spec.(type) {
				case *ast.TypeSpec:
					if slices.Contains(readerPackageNames, spec.Name.Name) {
						readerTypes[spec.Name.Name] = true
						eventTypeFindings(rel, spec, report)
					}
				case *ast.ValueSpec:
					for index, name := range spec.Names {
						values[name.Name] = packageValue{rel, gen.Tok, spec, index}
					}
				}
			}
		}
	}
	for _, rel := range others {
		for _, decl := range files[rel].Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Recv == nil || len(decl.Recv.List) == 0 {
					continue
				}
				if base := receiverBase(decl.Recv.List[0].Type); readerTypes[base] {
					report(rel, decl, "declares %s.%s outside %s: the reader's types keep their methods in its closed world",
						base, decl.Name.Name, readerRel)
				}
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok || !typeSpec.Assign.IsValid() || slices.Contains(readerPackageNames, typeSpec.Name.Name) {
						continue
					}
					if base := receiverBase(typeSpec.Type); readerTypes[base] {
						report(rel, typeSpec, "aliases %s outside %s, which lets another file declare its methods", base, readerRel)
					}
				}
			}
		}
	}
	collectors := make(map[string]bool)
	for _, name := range readerPackageNames {
		value, ok := values[name]
		if !ok {
			continue
		}
		switch value.tok {
		case token.CONST:
			constantFindings(name, value, report)
		case token.VAR:
			collectors[name] = true
			if !isMetricCollector(name, values, files, make(map[string]bool)) {
				report(value.rel, value.spec, "%s is not a client_golang collector, so the reader's update could call package code", name)
			}
		}
	}
	for _, rel := range others {
		ast.Inspect(files[rel], func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.AssignStmt:
				for _, target := range typed.Lhs {
					if root := rootIdent(target); root != nil && collectors[root.Name] {
						report(rel, typed, "assigns %s outside its declaration; the reader's metrics stay client_golang's", root.Name)
					}
				}
			case *ast.UnaryExpr:
				if root := rootIdent(typed.X); typed.Op == token.AND && root != nil && collectors[root.Name] {
					report(rel, typed, "takes the address of %s; the reader's metrics stay client_golang's", root.Name)
				}
			}
			return true
		})
	}
	return findings
}

// eventTypeFindings holds a reader type declared outside the reader's file,
// the event type, to a plain struct of method-free values.
func eventTypeFindings(rel string, spec *ast.TypeSpec, report func(rel string, node ast.Node, format string, args ...any)) {
	name := spec.Name.Name
	if spec.Assign.IsValid() || spec.TypeParams != nil {
		report(rel, spec, "declares %s as an alias or a generic type; the reader's event type is a plain struct", name)
		return
	}
	structure, ok := spec.Type.(*ast.StructType)
	if !ok {
		report(rel, spec, "declares %s as %s; the reader's event type is a plain struct", name, types.ExprString(spec.Type))
		return
	}
	for _, field := range structure.Fields.List {
		if len(field.Names) == 0 {
			report(rel, field, "embeds %s in %s, whose methods the reader could call", types.ExprString(field.Type), name)
			continue
		}
		if ident, ok := field.Type.(*ast.Ident); !ok || !slices.Contains(readerValueTypes, ident.Name) {
			report(rel, field, "gives %s a field of type %s; the reader's event type holds only %v",
				name, types.ExprString(field.Type), readerValueTypes)
		}
	}
}

// constantFindings holds an action constant to an untyped or basic-typed
// literal, which has no methods.
func constantFindings(name string, value packageValue, report func(rel string, node ast.Node, format string, args ...any)) {
	if value.spec.Type != nil {
		if ident, ok := value.spec.Type.(*ast.Ident); !ok || !slices.Contains(readerValueTypes, ident.Name) {
			report(value.rel, value.spec, "declares %s with type %s; the reader's constants are basic literals",
				name, types.ExprString(value.spec.Type))
			return
		}
	}
	if value.index >= len(value.spec.Values) {
		report(value.rel, value.spec, "declares %s without a literal value", name)
		return
	}
	if _, ok := value.spec.Values[value.index].(*ast.BasicLit); !ok {
		report(value.rel, value.spec, "declares %s as %s; the reader's constants are basic literals",
			name, types.ExprString(value.spec.Values[value.index]))
	}
}

// isMetricCollector reports whether a package variable is built by a
// client_golang constructor, directly or through a method of another such
// variable, so that its value and its methods are client_golang's.
func isMetricCollector(name string, values map[string]packageValue, files map[string]*ast.File, seen map[string]bool) bool {
	value, ok := values[name]
	if !ok || value.tok != token.VAR || seen[name] {
		return false
	}
	seen[name] = true
	packageOf := make(map[string]string)
	for importPath, local := range importNames(files[value.rel]) {
		packageOf[local] = importPath
	}
	if value.spec.Type != nil {
		expr := value.spec.Type
		if star, ok := expr.(*ast.StarExpr); ok {
			expr = star.X
		}
		selector, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		qualifier, ok := selector.X.(*ast.Ident)
		return ok && slices.Contains(metricImports, packageOf[qualifier.Name])
	}
	if len(value.spec.Values) != len(value.spec.Names) {
		return false
	}
	call, ok := value.spec.Values[value.index].(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	qualifier, ok := selector.X.(*ast.Ident)
	if !ok {
		return false
	}
	if importPath, ok := packageOf[qualifier.Name]; ok {
		return slices.Contains(metricImports, importPath)
	}
	return isMetricCollector(qualifier.Name, values, files, seen)
}

// readerDependencyFindings holds the reader's in-repo dependency to code that
// can neither log nor wait: no imports, no output builtins, no channels.
func readerDependencyFindings(files map[string]*ast.File, fset *token.FileSet) []string {
	var findings []string
	report := reporter(fset, &findings)
	for _, rel := range slices.Sorted(maps.Keys(files)) {
		file := files[rel]
		for importPath := range importNames(file) {
			report(rel, file, "imports %s; the reader's dependencies import nothing", importPath)
		}
		selected := selectedIdents(file)
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.ImportSpec:
				return false
			case *ast.Ident:
				if !selected[typed] && slices.Contains(outputBuiltins, typed.Name) {
					report(rel, typed, "names %s, which the reader would reach", typed.Name)
				}
			case *ast.ChanType:
				report(rel, typed, "declares a channel type, on which the reader could wait")
			}
			return true
		})
	}
	return findings
}

// relayCallee renders a call's callee as written, with a package qualifier
// resolved to its import path.
func relayCallee(fun ast.Expr, packageOf map[string]string) string {
	if selector, ok := fun.(*ast.SelectorExpr); ok {
		if qualifier, ok := selector.X.(*ast.Ident); ok {
			if importPath, ok := packageOf[qualifier.Name]; ok {
				return importPath + "." + selector.Sel.Name
			}
		}
	}
	return types.ExprString(fun)
}

// containerEventRelayFindings holds DockerClient.ContainerEvents, and it
// alone, to relayCalls; the rest of its file may log.
func containerEventRelayFindings(rel string, file *ast.File, fset *token.FileSet) []string {
	var findings []string
	reportAt := reporter(fset, &findings)
	report := func(node ast.Node, format string, args ...any) { reportAt(rel, node, format, args...) }
	packageOf := make(map[string]string)
	for importPath, name := range importNames(file) {
		packageOf[name] = importPath
	}
	isFilterConstructor := func(expr ast.Expr) bool {
		call, ok := expr.(*ast.CallExpr)
		return ok && relayCallee(call.Fun, packageOf) == dockerFiltersImportPath+".NewArgs"
	}
	// binds checks a binding of a name the allowlist reaches a callee
	// through: d and ctx are the method's receiver and parameter and are
	// never rebound, and filter is bound only to filters.NewArgs.
	binds := func(node ast.Node, target ast.Expr, value ast.Expr) {
		root := rootIdent(target)
		if root == nil {
			return
		}
		switch root.Name {
		case "d", "ctx":
			report(node, "rebinds %s, through which the relay reaches its callees", root.Name)
		case "filter":
			if root != target || value == nil || !isFilterConstructor(value) {
				report(node, "binds filter other than to filters.NewArgs")
			}
		}
	}
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok || function.Body == nil || functionKey(function) != containerEventRelayMethod {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.CallExpr:
				if _, ok := typed.Fun.(*ast.FuncLit); ok {
					return true
				}
				if callee := relayCallee(typed.Fun, packageOf); !slices.Contains(relayCalls, callee) {
					report(typed, "calls %s, outside the relay's allowlist", callee)
				}
			case *ast.AssignStmt:
				for index, target := range typed.Lhs {
					var value ast.Expr
					if typed.Tok == token.DEFINE && len(typed.Rhs) == len(typed.Lhs) {
						value = typed.Rhs[index]
					}
					binds(typed, target, value)
				}
			case *ast.ValueSpec:
				for _, name := range typed.Names {
					binds(typed, name, nil)
				}
			case *ast.RangeStmt:
				for _, target := range []ast.Expr{typed.Key, typed.Value} {
					if target != nil {
						binds(typed, target, nil)
					}
				}
			case *ast.Field:
				for _, name := range typed.Names {
					binds(typed, name, nil)
				}
			case *ast.UnaryExpr:
				if typed.Op == token.AND {
					binds(typed, typed.X, nil)
				}
			}
			return true
		})
	}
	return findings
}

// relayHelperFindings resolves the relay's d.client.Events to
// dockerSDKView.Events and holds it to calling the SDK's Events alone:
// DockerClient.client is a dockerSDKView, the helper calls only its view's
// events function, and every view in the package takes that function from a
// *client.Client's Events method.
func relayHelperFindings(files map[string]*ast.File, fset *token.FileSet) []string {
	var findings []string
	report := reporter(fset, &findings)
	var helper *ast.FuncDecl
	var helperRel string
	for _, rel := range slices.Sorted(maps.Keys(files)) {
		file := files[rel]
		packageOf := make(map[string]string)
		for importPath, name := range importNames(file) {
			packageOf[name] = importPath
		}
		for _, decl := range file.Decls {
			clients := make(map[string]bool)
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if functionKey(decl) == containerEventRelayHelper {
					helper, helperRel = decl, rel
				}
				for _, param := range decl.Type.Params.List {
					if isDockerClient(param.Type, packageOf) {
						for _, name := range param.Names {
							clients[name.Name] = true
						}
					}
				}
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					if typeSpec, ok := spec.(*ast.TypeSpec); ok && typeSpec.Name.Name == "DockerClient" {
						clientFieldFindings(rel, typeSpec, report)
					}
				}
			}
			ast.Inspect(decl, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CompositeLit:
					if ident, ok := typed.Type.(*ast.Ident); ok && ident.Name == "dockerSDKView" {
						viewLiteralFindings(rel, typed, clients, report)
					}
				case *ast.AssignStmt:
					for _, target := range typed.Lhs {
						if selector, ok := target.(*ast.SelectorExpr); ok && selector.Sel.Name == "events" {
							report(rel, typed, "assigns %s; a dockerSDKView takes its events function only when built",
								types.ExprString(selector))
						}
					}
				}
				return true
			})
		}
	}
	if helper == nil || helper.Body == nil {
		return append(findings, fmt.Sprintf("package docker no longer declares %s; re-resolve the relay's d.client.Events",
			containerEventRelayHelper))
	}
	receiver := "_"
	if len(helper.Recv.List[0].Names) == 1 {
		receiver = helper.Recv.List[0].Names[0].Name
	}
	packageOf := make(map[string]string)
	for importPath, name := range importNames(files[helperRel]) {
		packageOf[name] = importPath
	}
	ast.Inspect(helper.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if callee := relayCallee(call.Fun, packageOf); callee != receiver+".events" {
				report(helperRel, call, "%s calls %s; the relay's helper calls only the SDK's Events", containerEventRelayHelper, callee)
			}
		}
		return true
	})
	return findings
}

// isDockerClient reports whether a parameter type is the Docker SDK's
// *client.Client.
func isDockerClient(expr ast.Expr, packageOf map[string]string) bool {
	star, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := star.X.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Client" {
		return false
	}
	qualifier, ok := selector.X.(*ast.Ident)
	return ok && packageOf[qualifier.Name] == dockerClientImportPath
}

// clientFieldFindings pins DockerClient.client, through which the relay
// calls d.client.Events, to dockerSDKView.
func clientFieldFindings(rel string, spec *ast.TypeSpec, report func(rel string, node ast.Node, format string, args ...any)) {
	if structure, ok := spec.Type.(*ast.StructType); ok {
		for _, field := range structure.Fields.List {
			for _, name := range field.Names {
				if name.Name != "client" {
					continue
				}
				if ident, ok := field.Type.(*ast.Ident); !ok || ident.Name != "dockerSDKView" {
					report(rel, field, "DockerClient.client is not a dockerSDKView; re-resolve the relay's d.client.Events")
				}
				return
			}
		}
	}
	report(rel, spec, "DockerClient.client is not a dockerSDKView; re-resolve the relay's d.client.Events")
}

// viewLiteralFindings holds a dockerSDKView literal's events function to the
// Events method of a *client.Client parameter of the enclosing function.
func viewLiteralFindings(rel string, literal *ast.CompositeLit, clients map[string]bool,
	report func(rel string, node ast.Node, format string, args ...any),
) {
	for _, element := range literal.Elts {
		pair, ok := element.(*ast.KeyValueExpr)
		if !ok {
			report(rel, element, "builds a dockerSDKView without field keys")
			continue
		}
		if key, ok := pair.Key.(*ast.Ident); !ok || key.Name != "events" {
			continue
		}
		selector, ok := pair.Value.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "Events" {
			if owner, ok := selector.X.(*ast.Ident); ok && clients[owner.Name] {
				continue
			}
		}
		report(rel, pair, "sets dockerSDKView.events to %s; the relay's helper calls only a *client.Client's Events",
			types.ExprString(pair.Value))
	}
}

// fileDeclarations returns every name the file declares, at any scope, and
// the identifiers that declare a name or a field, which reference nothing.
// A method's name is selected, never referenced, so it declares no name.
func fileDeclarations(file *ast.File) (declared map[string]bool, declaring map[*ast.Ident]bool) {
	declared, declaring = make(map[string]bool), make(map[*ast.Ident]bool)
	declare := func(idents ...*ast.Ident) {
		for _, ident := range idents {
			declared[ident.Name] = true
			declaring[ident] = true
		}
	}
	declareFields := func(list *ast.FieldList) {
		if list == nil {
			return
		}
		for _, field := range list.List {
			declare(field.Names...)
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.FuncDecl:
			declaring[typed.Name] = true
			if typed.Recv == nil {
				declared[typed.Name.Name] = true
			}
			declareFields(typed.Recv)
			declareFields(typed.Type.Params)
			declareFields(typed.Type.Results)
		case *ast.StructType:
			for _, field := range typed.Fields.List {
				for _, name := range field.Names {
					declaring[name] = true
				}
			}
		case *ast.TypeSpec:
			declare(typed.Name)
		case *ast.ValueSpec:
			declare(typed.Names...)
		case *ast.AssignStmt:
			if typed.Tok == token.DEFINE {
				for _, target := range typed.Lhs {
					if ident, ok := target.(*ast.Ident); ok {
						declare(ident)
					}
				}
			}
		case *ast.RangeStmt:
			if typed.Tok == token.DEFINE {
				for _, target := range []ast.Expr{typed.Key, typed.Value} {
					if ident, ok := target.(*ast.Ident); ok {
						declare(ident)
					}
				}
			}
		case *ast.LabeledStmt:
			declaring[typed.Label] = true
		case *ast.BranchStmt:
			if typed.Label != nil {
				declaring[typed.Label] = true
			}
		}
		return true
	})
	return declared, declaring
}

// selectedIdents are the identifiers that name a field or a method rather
// than reference a declaration: selector names and composite-literal keys.
func selectedIdents(file *ast.File) map[*ast.Ident]bool {
	selected := make(map[*ast.Ident]bool)
	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.SelectorExpr:
			selected[typed.Sel] = true
		case *ast.CompositeLit:
			for _, element := range typed.Elts {
				if pair, ok := element.(*ast.KeyValueExpr); ok {
					if key, ok := pair.Key.(*ast.Ident); ok {
						selected[key] = true
					}
				}
			}
		}
		return true
	})
	return selected
}

// nonBlockingSends are the sends that are cases of a select with a default
// case, so they never wait.
func nonBlockingSends(file *ast.File) map[*ast.SendStmt]bool {
	sends := make(map[*ast.SendStmt]bool)
	ast.Inspect(file, func(node ast.Node) bool {
		statement, ok := node.(*ast.SelectStmt)
		if !ok {
			return true
		}
		var cases []*ast.SendStmt
		hasDefault := false
		for _, clause := range statement.Body.List {
			comm, ok := clause.(*ast.CommClause)
			if !ok {
				continue
			}
			if comm.Comm == nil {
				hasDefault = true
			}
			if send, ok := comm.Comm.(*ast.SendStmt); ok {
				cases = append(cases, send)
			}
		}
		if hasDefault {
			for _, send := range cases {
				sends[send] = true
			}
		}
		return true
	})
	return sends
}

func declaresFunction(file *ast.File, key string) bool {
	for _, decl := range file.Decls {
		if function, ok := decl.(*ast.FuncDecl); ok && functionKey(function) == key {
			return true
		}
	}
	return false
}

// parseRepoDir parses one package directory's production files, build tags
// regardless, keyed by repository-relative path.
func parseRepoDir(t *testing.T, fset *token.FileSet, root, dir string) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(dir)))
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	files := make(map[string]*ast.File)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		rel := path.Join(dir, name)
		file, err := parser.ParseFile(fset, filepath.Join(root, filepath.FromSlash(rel)), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", rel, err)
		}
		files[rel] = file
	}
	if len(files) == 0 {
		t.Fatalf("%s has no production files; point this guard at the package", dir)
	}
	return files
}

// parseAndCheck runs one rule set over synthetic source placed at the file the
// rule set guards.
func parseAndCheck(t *testing.T, src string, check func(rel string, file *ast.File, fset *token.FileSet) []string) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "synthetic.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse synthetic source: %v", err)
	}
	return check("synthetic.go", file, fset)
}

// parseSyntheticFiles parses synthetic sources standing for the files of one
// package.
func parseSyntheticFiles(t *testing.T, srcs map[string]string) (map[string]*ast.File, *token.FileSet) {
	t.Helper()
	fset := token.NewFileSet()
	files := make(map[string]*ast.File)
	for rel, src := range srcs {
		file, err := parser.ParseFile(fset, rel, src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse synthetic %s: %v", rel, err)
		}
		files[rel] = file
	}
	return files, fset
}
