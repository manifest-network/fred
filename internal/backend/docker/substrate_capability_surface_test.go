package docker

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestBackendRetainsOnlyReadViewsAndExplicitSettlementCapabilities(t *testing.T) {
	dockerView := projectDockerRead(&mockDockerClient{})
	if _, ok := dockerView.(dockerMutationSink); ok {
		t.Fatal("Docker read projection retains mutation methods")
	}
	composeView := projectComposeRead(&mockComposeExecutor{})
	if _, ok := composeView.(composeMutationSink); ok {
		t.Fatal("Compose read projection retains mutation methods")
	}
	volumeView := projectVolumeRead(&mockVolumeManager{})
	if _, ok := volumeView.(volumeMutationSink); ok {
		t.Fatal("volume read projection retains mutation methods")
	}

	backendType := reflect.TypeFor[Backend]()
	assertFieldType := func(name string, want reflect.Type) {
		t.Helper()
		field, ok := backendType.FieldByName(name)
		if !ok {
			t.Fatalf("Backend.%s is missing", name)
		}
		if field.Type != want {
			t.Fatalf("Backend.%s type = %v, want %v", name, field.Type, want)
		}
	}
	assertFieldType("docker", reflect.TypeFor[dockerReadClient]())
	assertFieldType("compose", reflect.TypeFor[composeReader]())
	assertFieldType("volumes", reflect.TypeFor[volumeReader]())
	assertFieldType("backgroundMaintenance", reflect.TypeFor[*backgroundMaintenanceCoordinator]())

	for index := range backendType.NumField() {
		field := backendType.Field(index)
		fieldType := field.Type.String()
		if strings.Contains(strings.ToLower(field.Name), "residual") {
			t.Fatalf("Backend.%s retains a residual mutation escape hatch", field.Name)
		}
		if field.Type == reflect.TypeFor[dockerMutationSink]() ||
			field.Type == reflect.TypeFor[composeMutationSink]() ||
			field.Type == reflect.TypeFor[volumeMutationSink]() ||
			strings.Contains(fieldType, "substratemutation.Guard[") {
			t.Fatalf("Backend.%s retains raw/generic mutation authority %v", field.Name, field.Type)
		}
	}

	// The retained background service may select only a complete convergence
	// workflow. In particular, no function may accept a string, record DTO, or
	// other caller-supplied substrate target.
	coordinatorType := reflect.TypeFor[backgroundMaintenanceCoordinator]()
	contextType := reflect.TypeFor[context.Context]()
	for index := range coordinatorType.NumField() {
		field := coordinatorType.Field(index)
		if field.Type.Kind() != reflect.Func || field.Type.NumIn() != 1 || field.Type.In(0) != contextType {
			t.Fatalf("background coordinator field %s exposes targetable surface %v", field.Name, field.Type)
		}
	}

	// Live execution receives only a purpose-specific invocation closure. The
	// exact opaque subject, mutation facade, target names and action selection
	// are captured by its builder and cannot be changed by run*Substrate.
	errorType := reflect.TypeFor[error]()
	assertInvocationOnly := func(name string, capability reflect.Type) {
		t.Helper()
		if capability.Kind() != reflect.Func || capability.IsVariadic() ||
			capability.NumIn() != 1 || capability.In(0) != contextType ||
			capability.NumOut() != 1 || capability.Out(0) != errorType {
			t.Fatalf("%s exposes a targetable execution surface %v", name, capability)
		}
	}
	operationCapability := reflect.TypeFor[operationSubstrate]()
	maintenanceCapability := reflect.TypeFor[maintenanceSubstrate]()
	closeCapability := reflect.TypeFor[closeSubstrate]()
	assertInvocationOnly("operationSubstrate", operationCapability)
	assertInvocationOnly("maintenanceSubstrate", maintenanceCapability)
	assertInvocationOnly("closeSubstrate", closeCapability)
	if operationCapability.AssignableTo(maintenanceCapability) ||
		operationCapability.AssignableTo(closeCapability) ||
		maintenanceCapability.AssignableTo(closeCapability) {
		t.Fatal("purpose-specific substrate invocation capabilities are interchangeable")
	}
}

func TestBackgroundMaintenanceConstructionRejectsPartialRawSubstrate(t *testing.T) {
	backend := &Backend{}
	coordinator, err := newBackgroundMaintenanceCoordinator(
		backend,
		storageMutationOperations{backend: backend},
	)
	if err == nil || coordinator != nil {
		t.Fatal("partial raw substrate constructed a background maintenance coordinator")
	}

	docker := &mockDockerClient{}
	compose := &mockComposeExecutor{}
	volumes := &mockVolumeManager{}
	for _, test := range []struct {
		name   string
		mutate func(*storageMutationOperations)
	}{
		{name: "typed-nil docker", mutate: func(ops *storageMutationOperations) {
			ops.docker = (*mockDockerClient)(nil)
		}},
		{name: "typed-nil compose", mutate: func(ops *storageMutationOperations) {
			ops.compose = (*mockComposeExecutor)(nil)
		}},
		{name: "typed-nil volumes", mutate: func(ops *storageMutationOperations) {
			ops.volumes = (*mockVolumeManager)(nil)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ops := newStorageMutationOperations(backend, docker, compose, volumes)
			test.mutate(&ops)
			coordinator, err := newBackgroundMaintenanceCoordinator(backend, ops)
			if err == nil || coordinator != nil {
				t.Fatal("typed-nil raw substrate constructed a background maintenance coordinator")
			}
		})
	}
}

// TestProductionSubstrateMutationSurface is a mechanical regression guard for
// the construction boundary. Raw Docker/Compose/volume writers may be invoked
// only by their low-level adapter implementation or by the one facade which
// brackets them. Orchestration files may hold only a subject-bound facade.
func TestProductionSubstrateMutationSurface(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, name, nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", name, parseErr)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			position := fset.Position(selector.Pos())
			if selector.Sel.Name == "mutationAdapter" {
				t.Errorf("%s: production code recovers the removed unscoped mutation adapter", position)
				return true
			}
			if !rawSubstrateMethod(selector.Sel.Name) && !rawVolumeMutation(selector) {
				return true
			}
			base := filepath.Base(position.Filename)
			if base != "storage_mutation_guard.go" && base != "compose.go" {
				t.Errorf("%s: raw substrate method %s is outside the bound facade", position, selector.Sel.Name)
			}
			return true
		})
	}
}

// Create and Destroy are intentionally excluded from rawSubstrateMethod: both
// names are common on unrelated stores. They are raw substrate writes only
// when selected from the volume sink retained in the construction-only ops
// bundle (or from another value explicitly named volumes).
func rawVolumeMutation(selector *ast.SelectorExpr) bool {
	if selector == nil || selector.Sel == nil ||
		(selector.Sel.Name != "Create" && selector.Sel.Name != "Destroy") {
		return false
	}
	receiver, ok := selector.X.(*ast.SelectorExpr)
	return ok && receiver.Sel != nil && receiver.Sel.Name == "volumes"
}

func rawSubstrateMethod(name string) bool {
	switch name {
	case "PullImage", "ResolveImageUser", "CreateContainer", "StartContainer",
		"StopContainer", "RenameContainer", "RemoveContainer", "EnsureTenantNetwork",
		"RemoveTenantNetworkIfEmpty", "DetectVolumeOwner", "DetectWritablePaths",
		"ExtractImageContent", "EnsureQuota", "RenameVolume", "Up", "Down":
		return true
	default:
		return false
	}
}

// PullImage is the only operation allowed to use Runner.Prepare. Image-user,
// volume-owner, and writable-path probes create temporary containers and may
// leave anonymous volumes when Docker returns ambiguously; they are tenant
// Steps even though their intended result is observational.
func TestRunnerPrepareAllowlistContainsOnlyPullImage(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "storage_mutation_guard.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var prepares []string
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "Prepare" {
				prepares = append(prepares, function.Name.Name)
			}
			return true
		})
	}
	if !reflect.DeepEqual(prepares, []string{"pullImage"}) {
		t.Fatalf("Runner.Prepare call sites = %v, want only storageMutations.pullImage", prepares)
	}
}
