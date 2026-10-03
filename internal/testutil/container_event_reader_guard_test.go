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
//     case, so it never waits on the dispatcher or the reporter.
//
// A name declared anywhere in the file counts as declared everywhere in it.
// That over-approximation can hide a package name only behind a declaration
// of the very same name in this file, in plain sight.
//
// The relay that feeds the reader, DockerClient.ContainerEvents, runs on the
// same event path but must hold the Docker SDK client, so it is held to a
// narrower rule: it names no logging package, no fmt or os output, no output
// builtin and nothing called logger.
//
// TestContainerEventReaderGuardsFire proves every rule fires. Which function
// may hold the event session is pinned by terminal_budget_authority_test.go.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
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
)

var (
	// readerImports are the only packages the reader's file may import.
	readerImports = []string{"sync/atomic", failurecauseImportPath}
	// readerBuiltins are the only predeclared names the reader's file may use.
	readerBuiltins = []string{
		"_", "bool", "error", "false", "float64", "int", "len", "make", "nil", "string", "true", "uint64",
	}
	// readerPackageNames are the only names from the rest of package docker
	// the reader's file may use: the event type and its actions, and the two
	// metric series it updates, both client_golang collectors whose updates
	// are atomic.
	readerPackageNames = []string{
		"ContainerEvent", "containerEventStart", "containerEventKill", "containerEventDie",
		"containerDeathQueueDepth", "eventLoopDeathsDropped",
	}
	// loggerNames are field or method names the relay may not select.
	loggerNames = []string{"logger", "Logger"}
	// outputBuiltins write to the process's standard error.
	outputBuiltins = []string{"print", "println"}
)

func TestContainerEventReaderIsClosed(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	reader := parseRepoFile(t, fset, root, eventReaderSite.file)
	if !declaresFunction(reader, eventReaderSite.function) {
		t.Fatalf("%s no longer declares %s; point this guard at the reader", eventReaderSite.file, eventReaderSite.function)
	}
	relay := parseRepoFile(t, fset, root, containerEventRelayFile)
	if !declaresFunction(relay, containerEventRelayMethod) {
		t.Fatalf("%s no longer declares %s; point this guard at the relay", containerEventRelayFile, containerEventRelayMethod)
	}
	findings := containerEventReaderFindings(eventReaderSite.file, reader, fset)
	findings = append(findings, containerEventRelayFindings(containerEventRelayFile, relay, fset)...)
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
func (r containerEventReader) enqueue() {
	select {
	case r.deaths <- 1:
	case <-r.stop:
	}
}`, "sends outside a select with a default case"},
	}
	for _, test := range tests {
		t.Run("reader: "+test.name, func(t *testing.T) {
			findings := parseAndCheck(t, test.src, containerEventReaderFindings)
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
		{"slog", `slog.Debug("docker event")`, "names slog.Debug"},
		{"fmt to stdout", `fmt.Println("docker event")`, "names fmt.Println"},
		{"os.Stderr", `_, _ = os.Stderr.WriteString("docker event")`, "names os.Stderr"},
		{"a logger field", `d.logger.Warn("docker event")`, "selects logger"},
		{"println", `println("docker event")`, "names println"},
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
		containerDeathQueueDepth.Set(float64(len(r.deaths)))
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
func (o *containerDeathOverflow) drain(events <-chan ContainerEvent) (count int) {
	var last string
loop:
	for event := range events {
		last = event.ContainerID
		count++
		if last == "" {
			break loop
		}
	}
	return count
}`, containerEventReaderFindings},
		{"the relay's shape, beside a function that logs", `package docker
import (
	"context"
	"fmt"
	"log/slog"

	"github.com/docker/docker/api/types/events"
)
func (d *DockerClient) ContainerEvents(ctx context.Context) (<-chan ContainerEvent, <-chan error) {
	messages, errs := d.client.Events(ctx, events.ListOptions{})
	out, errCh := make(chan ContainerEvent), make(chan error, 1)
	go func() {
		defer close(out)
		for message := range messages {
			select {
			case out <- ContainerEvent{ContainerID: message.Actor.ID}:
			case <-ctx.Done():
				return
			}
		}
		errCh <- fmt.Errorf("docker events: %w", <-errs)
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
}

// containerEventReaderFindings reports everything in the reader's file that
// reaches outside its closed world; see the file comment.
func containerEventReaderFindings(rel string, file *ast.File, fset *token.FileSet) []string {
	var findings []string
	report := func(node ast.Node, format string, args ...any) {
		findings = append(findings, fmt.Sprintf("%s (%s): ", fset.Position(node.Pos()), rel)+fmt.Sprintf(format, args...))
	}
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

// containerEventRelayFindings applies the relay's narrower rule to
// DockerClient.ContainerEvents alone; the rest of its file may log.
func containerEventRelayFindings(rel string, file *ast.File, fset *token.FileSet) []string {
	var findings []string
	report := func(node ast.Node, format string, args ...any) {
		findings = append(findings, fmt.Sprintf("%s (%s): ", fset.Position(node.Pos()), rel)+fmt.Sprintf(format, args...))
	}
	packageOf := make(map[string]string)
	for importPath, name := range importNames(file) {
		packageOf[name] = importPath
	}
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if !ok || functionKey(function) != containerEventRelayMethod {
			continue
		}
		ast.Inspect(function, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.SelectorExpr:
				if slices.Contains(loggerNames, typed.Sel.Name) {
					report(typed, "selects %s on the event path", typed.Sel.Name)
					return true
				}
				if qualifier, ok := typed.X.(*ast.Ident); ok && writesOutput(packageOf[qualifier.Name], typed.Sel.Name) {
					report(typed, "names %s.%s on the event path", qualifier.Name, typed.Sel.Name)
				}
			case *ast.Ident:
				if slices.Contains(outputBuiltins, typed.Name) {
					report(typed, "names %s on the event path", typed.Name)
				}
			}
			return true
		})
	}
	return findings
}

// writesOutput reports whether a package member logs or writes to the
// process's standard output or error. Formatting alone, such as fmt.Errorf,
// writes nothing.
func writesOutput(importPath, name string) bool {
	switch importPath {
	case "log", "log/slog":
		return true
	case "fmt":
		return strings.HasPrefix(name, "Print") || strings.HasPrefix(name, "Fprint")
	case "os":
		return name == "Stdout" || name == "Stderr"
	}
	return false
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

func parseRepoFile(t *testing.T, fset *token.FileSet, root, rel string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(fset, filepath.Join(root, filepath.FromSlash(rel)), nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", rel, err)
	}
	return file
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
