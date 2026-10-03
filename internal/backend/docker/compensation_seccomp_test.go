package docker

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

// testdata/compensation-plan-315ed5a.json was written by the unchanged
// 315ed5a encoder: instance 0 has the pre-profile security options, instance
// 1 also names a stale profile. Plans persisted by deployed binaries must
// keep decoding, and must never replay a profile of their own.
func TestCompensationPlanFrom315ed5aCreatesWithTheCurrentProfile(t *testing.T) {
	encoded, err := os.ReadFile("testdata/compensation-plan-315ed5a.json")
	require.NoError(t, err)
	plan, err := decodeCompensationSourceSnapshot(encoded)
	require.NoError(t, err)
	require.Len(t, plan.Containers, 2)
	for _, snapshot := range plan.Containers {
		require.Equal(t, []string{"no-new-privileges:true"}, snapshot.Host.SecurityOpt)
		require.True(t, snapshot.Host.ReadonlyRootfs)
		require.Equal(t, []string{"ALL"}, []string(snapshot.Host.CapDrop))
	}

	var created [][]string
	docker := newImageSecurityDockerClient(t, func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/images/") {
			return imageSecurityResponse(http.StatusOK, platformSecurityJSON(testImageID, ocispec.MediaTypeImageManifest)), nil
		}
		require.True(t, strings.HasSuffix(req.URL.Path, "/containers/create"))
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		var request struct{ HostConfig container.HostConfig }
		require.NoError(t, json.Unmarshal(body, &request))
		created = append(created, request.HostConfig.SecurityOpt)
		return imageSecurityResponse(http.StatusCreated, fmt.Sprintf(`{"Id":"source-%d"}`, len(created))), nil
	})
	image, err := docker.AdmitImage(t.Context(), testImageID)
	require.NoError(t, err)
	project, err := docker.images.Compile(&composetypes.Project{Name: "fred-source-lease", Services: composetypes.Services{"web-1": {Image: image.Reference()}}}, map[string]imageexec.Image{"web-1": image})
	require.NoError(t, err)
	binding, err := project.Container("web-1")
	require.NoError(t, err)
	want := tenantSeccompSecurityOpt(t, "no-new-privileges:true")
	for _, snapshot := range plan.Containers {
		for range 2 { // a retry must not accumulate entries
			_, outcome := docker.createCompensationContainer(t.Context(), compensationContainer{
				Name: snapshot.Name, Binding: binding, Config: snapshot.Config, Host: snapshot.Host, Networks: snapshot.Networks,
			})
			require.True(t, outcome.settled)
			require.NoError(t, outcome.err)
		}
		require.Equal(t, []string{"no-new-privileges:true"}, snapshot.Host.SecurityOpt, "creation must not change the decoded plan")
	}
	require.Len(t, created, 4)
	for _, securityOpt := range created {
		require.Equal(t, want, securityOpt)
	}
}

func inspectedTenantContainer(t *testing.T, index int, securityOpt []string) container.InspectResponse {
	t.Helper()
	image := fixtureImageID("compensation-size")
	labels := map[string]string{
		LabelManaged: "true", LabelLeaseUUID: "0192f1a0-1111-4abc-8def-00000000e11a", LabelTenant: "manifest1tenant",
		LabelProviderUUID: nominalDockerProviderUUID, LabelSKU: "docker-small", LabelServiceName: "web",
		LabelInstanceIndex: fmt.Sprint(index), LabelFailCount: "0", LabelCreatedAt: time.Unix(int64(index), 0).UTC().Format(time.RFC3339),
		LabelBackendName: "docker", LabelImageReference: "registry.example/web:1",
		LabelCallbackURL: "https://fred.example/callbacks/provision?operation_id=x", LabelLifecycleCallbackURL: "https://fred.example/callbacks/lifecycle",
	}
	return container.InspectResponse{
		ContainerJSONBase: &container.ContainerJSONBase{
			Name: fmt.Sprintf("/fred-lease-web-%d", index), Image: image,
			HostConfig: &container.HostConfig{
				CapDrop: []string{"ALL"}, SecurityOpt: securityOpt, ReadonlyRootfs: true,
				Tmpfs:         map[string]string{"/tmp": "size=64M", "/run": "size=64M"},
				RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyDisabled},
				Resources:     container.Resources{NanoCPUs: 500_000_000, Memory: 512 << 20, MemorySwap: 512 << 20},
				Mounts:        []mount.Mount{{Type: mount.TypeBind, Source: fmt.Sprintf("/data/volumes/fred-lease-web-%d/data", index), Target: "/data"}},
			},
		},
		Config:          &container.Config{Image: image, Env: []string{"PORT=8080"}, Hostname: fmt.Sprintf("web-%d", index), Labels: labels},
		NetworkSettings: &container.NetworkSettings{Networks: map[string]*network.EndpointSettings{"fred-tenant": {Aliases: []string{"web"}}}},
	}
}

// Captured configuration keeps every other security option and drops a
// profile; the caller's inspected value is not modified.
func TestCompensationSnapshotDropsSeccompOptions(t *testing.T) {
	inspected := inspectedTenantContainer(t, 0, tenantSeccompSecurityOpt(t, "no-new-privileges:true", "label=disable", "seccomp:unconfined"))
	original := slices.Clone(inspected.HostConfig.SecurityOpt)
	record := snapshotCompensationContainer(inspected, nil)
	require.NotNil(t, record)
	require.Equal(t, []string{"no-new-privileges:true", "label=disable"}, record.Host.SecurityOpt)
	require.NotSame(t, inspected.HostConfig, record.Host)
	require.Equal(t, original, inspected.HostConfig.SecurityOpt)
	require.Nil(t, compensationHostConfig(nil))
	withoutOptions := compensationHostConfig(&container.HostConfig{ReadonlyRootfs: true})
	require.Nil(t, withoutOptions.SecurityOpt, "a host without options keeps its encoded form")
}

// Every running container carries an inline profile of about 13 KB. A plan
// that kept it per instance would exceed the 4 MiB maintenance journal entry
// long before the largest admitted lease; captured plans drop it.
func TestCompensationPlanForTheLargestLeaseFitsTheJournal(t *testing.T) {
	inline := tenantSeccompSecurityOpt(t, "no-new-privileges:true")
	plan := compensationSourcePlan{Version: 1}
	for index := range backend.MaxOperationQuantity {
		record := snapshotCompensationContainer(inspectedTenantContainer(t, index, inline), nil)
		require.NotNil(t, record)
		record.Platform = ocispec.Platform{OS: "linux", Architecture: "amd64"}
		plan.Containers = append(plan.Containers, *record)
	}
	encoded, err := encodeCompensationSourcePlan(plan)
	require.NoError(t, err)
	const journalEntryLimit = 4 << 20
	t.Logf("plan for %d instances: %d bytes", backend.MaxOperationQuantity, len(encoded))
	require.Less(t, len(encoded), journalEntryLimit-64<<10, "the plan must leave room for the journal record around it")
	decoded, err := decodeCompensationSourceSnapshot(encoded)
	require.NoError(t, err)
	require.Len(t, decoded.Containers, backend.MaxOperationQuantity)
	require.Greater(t, backend.MaxOperationQuantity*len(inline[len(inline)-1]), journalEntryLimit,
		"kept per instance, the profile alone would not fit")

	// Records whose host configuration names a profile anyway still encode
	// to the same plan: the encoder strips before persisting.
	for index := range plan.Containers {
		plan.Containers[index].Host.SecurityOpt = inline
	}
	restripped, err := encodeCompensationSourcePlan(plan)
	require.NoError(t, err)
	require.Equal(t, encoded, restripped)
}

// The plan encoder is the one persistence sink: whatever a record holds,
// including a record built without the constructor, no persisted instance
// names a seccomp profile, and every other option survives.
func TestCompensationPlanEncoderPersistsNoSeccompOption(t *testing.T) {
	inline := tenantSeccompSecurityOpt(t, "no-new-privileges:true", "label=disable")
	record := snapshotCompensationContainer(inspectedTenantContainer(t, 0, nil), nil)
	require.NotNil(t, record)
	record.Platform = ocispec.Platform{OS: "linux", Architecture: "amd64"}
	for name, securityOpt := range map[string][]string{
		"inline profile":      inline,
		"unconfined appended": append(slices.Clone(inline), "seccomp:unconfined"),
		"builtin":             {"no-new-privileges:true", "label=disable", "seccomp=builtin"},
	} {
		t.Run(name, func(t *testing.T) {
			bypassed := *record
			host := *record.Host
			host.SecurityOpt = securityOpt
			bypassed.Host = &host
			encoded, err := encodeCompensationSourcePlan(compensationSourcePlan{Version: 1, Containers: []compensationContainerRecord{bypassed}})
			require.NoError(t, err)
			var stored storedCompensationPlan
			require.NoError(t, json.Unmarshal(encoded, &stored))
			require.Len(t, stored.Containers, 1)
			require.Equal(t, []string{"no-new-privileges:true", "label=disable"}, stored.Containers[0].Host.SecurityOpt)
			require.NotContains(t, string(encoded), "SCMP_ACT", "no profile body is persisted")
			require.Equal(t, securityOpt, host.SecurityOpt, "encoding must not change the record")
			decoded, err := decodeCompensationSourceSnapshot(encoded)
			require.NoError(t, err)
			require.Equal(t, []string{"no-new-privileges:true", "label=disable"}, decoded.Containers[0].Host.SecurityOpt)
		})
	}
}

// compensationRecordLiterals returns every composite literal of
// compensationContainerRecord in file outside newCompensationContainerRecord,
// including a slice or array literal whose elements are records built in
// place with their type elided.
func compensationRecordLiterals(fset *token.FileSet, file *ast.File) []string {
	isRecord := func(expr ast.Expr) bool {
		if star, ok := expr.(*ast.StarExpr); ok {
			expr = star.X
		}
		ident, ok := expr.(*ast.Ident)
		return ok && ident.Name == "compensationContainerRecord"
	}
	var findings []string
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == "newCompensationContainerRecord" {
			continue
		}
		ast.Inspect(decl, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			switch typ := literal.Type.(type) {
			case *ast.Ident:
				if isRecord(typ) {
					findings = append(findings, fset.Position(literal.Pos()).String())
				}
			case *ast.ArrayType:
				if isRecord(typ.Elt) && len(literal.Elts) > 0 {
					findings = append(findings, fset.Position(literal.Pos()).String())
				}
			}
			return true
		})
	}
	return findings
}

// Every production record comes from newCompensationContainerRecord, so no
// writer can keep a captured or decoded profile.
func TestCompensationRecordsComeOnlyFromTheConstructor(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	require.NoError(t, err)
	scanned, constructors := 0, 0
	var findings []string
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		scanned++
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == "newCompensationContainerRecord" {
				constructors++
			}
		}
		findings = append(findings, compensationRecordLiterals(fset, file)...)
	}
	require.Greater(t, scanned, 50)
	require.Equal(t, 1, constructors)
	require.Empty(t, findings, "build compensation records with newCompensationContainerRecord")

	// Positive control: a literal outside the constructor is found, whether a
	// value, a pointer or the elided elements of a slice; the constructor's
	// own is not, and neither is an empty slice.
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "positive.go", `package docker
func newCompensationContainerRecord() compensationContainerRecord { return compensationContainerRecord{} }
func capture() *compensationContainerRecord { return &compensationContainerRecord{Name: "x"} }
func decode() []compensationContainerRecord { return []compensationContainerRecord{{Name: "y"}, compensationContainerRecord{}} }
func empty() []*compensationContainerRecord { return []*compensationContainerRecord{} }
`, parser.SkipObjectResolution)
	require.NoError(t, err)
	require.Len(t, compensationRecordLiterals(fset, file), 3)
}
