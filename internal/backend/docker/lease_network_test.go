package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	composeapi "github.com/docker/compose/v5/pkg/api"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/tenantseccomp"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

const (
	ownershipLease   = "00000000-0000-4000-8000-000000000271"
	ownershipBackend = "docker"
)

// networkDaemon is a synthetic Docker daemon at the HTTP boundary. It answers
// the requests Compose and DockerClient make for networks and containers, and
// records every request so a test can prove what reached the daemon.
type networkDaemon struct {
	t        *testing.T
	mu       sync.Mutex
	byName   map[string]string // network name -> ID
	networks map[string]string // network ID -> inspect JSON
	requests []string
	creates  [][]byte
	// containers is the JSON array /containers/json returns.
	containers string
}

func newNetworkDaemon(t *testing.T) *networkDaemon {
	return &networkDaemon{t: t, byName: map[string]string{}, networks: map[string]string{}, containers: "[]"}
}

// put installs a network under its name and ID.
func (d *networkDaemon) put(name, id string, labels map[string]string, containers map[string]string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	attached := map[string]map[string]string{}
	for cid, cname := range containers {
		attached[cid] = map[string]string{"Name": cname}
	}
	body, err := json.Marshal(map[string]any{"Name": name, "Id": id, "Driver": "bridge", "Labels": labels, "Containers": attached})
	require.NoError(d.t, err)
	d.byName[name] = id
	d.networks[id] = string(body)
}

func (d *networkDaemon) seen() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.requests...)
}

func (d *networkDaemon) createBodies() [][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([][]byte(nil), d.creates...)
}

func (d *networkDaemon) RoundTrip(r *http.Request) (*http.Response, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	path := r.URL.Path
	if strings.HasPrefix(path, "/v") {
		_, rest, _ := strings.Cut(path[1:], "/")
		path = "/" + rest
	}
	d.requests = append(d.requests, r.Method+" "+path)
	status, body := http.StatusOK, ""
	switch {
	case path == "/_ping":
		body = "OK"
	case path == "/version":
		body = `{"ApiVersion":"1.51","Os":"linux","Arch":"amd64"}`
	case path == "/info":
		body = `{"OSType":"linux","Architecture":"amd64","NCPU":8,"MemTotal":16000000000}`
	case strings.HasPrefix(path, "/images/") && strings.HasSuffix(path, "/json"):
		body = fmt.Sprintf(`{"Id":%q,"Os":"linux","Architecture":"amd64","Config":{"Cmd":["sleep","3600"]},"RootFS":{"Type":"layers","Layers":[]}}`, testImageID)
	case path == "/containers/json":
		body = d.containers
	case path == "/volumes":
		body = `{"Volumes":[],"Warnings":[]}`
	case path == "/networks" && r.Method == http.MethodGet:
		body = d.listNetworks(r.URL.Query().Get("filters"))
	case path == "/networks/create" && r.Method == http.MethodPost:
		var req struct {
			Name   string
			Labels map[string]string
		}
		require.NoError(d.t, json.NewDecoder(r.Body).Decode(&req))
		if _, exists := d.byName[req.Name]; exists {
			status, body = http.StatusConflict, fmt.Sprintf(`{"message":"network with name %s already exists"}`, req.Name)
			break
		}
		id := testLeaseNetworkID("created/" + req.Name)
		d.mu.Unlock()
		d.put(req.Name, id, req.Labels, nil)
		d.mu.Lock()
		body = fmt.Sprintf(`{"Id":%q}`, id)
	case strings.HasPrefix(path, "/networks/") && r.Method == http.MethodGet:
		ref := strings.TrimPrefix(path, "/networks/")
		if id, ok := d.byName[ref]; ok {
			ref = id
		}
		if network, ok := d.networks[ref]; ok {
			body = network
		} else {
			status, body = http.StatusNotFound, `{"message":"network not found"}`
		}
	case strings.HasPrefix(path, "/networks/") && r.Method == http.MethodDelete:
		id := strings.TrimPrefix(path, "/networks/")
		delete(d.networks, id)
		status, body = http.StatusNoContent, ""
	case path == "/containers/create":
		data, err := io.ReadAll(r.Body)
		require.NoError(d.t, err)
		d.creates = append(d.creates, data)
		status, body = http.StatusInternalServerError, `{"message":"synthetic daemon stops after observing the create request"}`
	case strings.HasPrefix(path, "/containers/") && (strings.HasSuffix(path, "/stop") || r.Method == http.MethodDelete):
		status, body = http.StatusNoContent, ""
	default:
		return nil, fmt.Errorf("unexpected daemon request: %s %s", r.Method, path)
	}
	return &http.Response{StatusCode: status, Header: http.Header{
		"Api-Version": {"1.51"}, "Ostype": {"linux"}, "Content-Type": {"application/json"},
	}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
}

// listNetworks answers a filtered network list with every network whose name
// equals a requested name, or whose labels contain every requested label.
func (d *networkDaemon) listNetworks(raw string) string {
	var filter map[string]map[string]bool
	if raw != "" {
		require.NoError(d.t, json.Unmarshal([]byte(raw), &filter))
	}
	var out []json.RawMessage
	for _, body := range d.networks {
		var network struct {
			Name   string
			Labels map[string]string
		}
		require.NoError(d.t, json.Unmarshal([]byte(body), &network))
		match := true
		for name := range filter["name"] {
			match = match && network.Name == name
		}
		for label := range filter["label"] {
			key, value, _ := strings.Cut(label, "=")
			match = match && network.Labels[key] == value
		}
		if match {
			out = append(out, json.RawMessage(body))
		}
	}
	data, err := json.Marshal(out)
	require.NoError(d.t, err)
	if out == nil {
		return "[]"
	}
	return string(data)
}

func (d *networkDaemon) dockerClient(t *testing.T, backendName string) *DockerClient {
	t.Helper()
	sdk, err := client.NewClientWithOpts(client.WithHost("http://docker.invalid"), client.WithVersion("1.51"),
		client.WithHTTPClient(&http.Client{Transport: d}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sdk.Close()) })
	return &DockerClient{client: newDockerSDKView(sdk), backendName: backendName}
}

func requireNoNetworkMutation(t *testing.T, requests []string) {
	t.Helper()
	for _, request := range requests {
		require.False(t, strings.HasPrefix(request, "POST /networks/create") || strings.HasPrefix(request, "DELETE /networks/") ||
			strings.HasSuffix(request, "/connect") || strings.HasSuffix(request, "/disconnect"),
			"no network mutation may reach the daemon: %s", request)
	}
}

func foreignLeaseNetworkLabels() map[string]string {
	return map[string]string{
		LabelManaged: "true", LabelNetworkScope: networkScopeLease,
		LabelLeaseUUID: "00000000-0000-4000-8000-000000000999", LabelBackendName: "other-backend",
		composeapi.ProjectLabel: "other-project", composeapi.NetworkLabel: "lease",
	}
}

// 1. A same-named network that is not this lease's on this backend is refused
// before anything can attach to it, and is left untouched.
func TestInspectLeaseNetworkRefusesForeignAndUnlabeledSameNameNetworks(t *testing.T) {
	name := leaseNetworkName(ownershipLease)
	for label, labels := range map[string]map[string]string{
		"another lease and backend": foreignLeaseNetworkLabels(),
		"unlabeled":                 nil,
		"another backend":           leaseNetworkLabels(ownershipLease, "other-backend"),
		"no backend label":          leaseNetworkLabels(ownershipLease, ""),
		"missing scope":             {LabelManaged: "true", LabelLeaseUUID: ownershipLease, LabelBackendName: ownershipBackend},
	} {
		t.Run(label, func(t *testing.T) {
			daemon := newNetworkDaemon(t)
			daemon.put(name, testLeaseNetworkID("foreign"), labels, map[string]string{"unrelated": "other-tenant-db"})
			docker := daemon.dockerClient(t, ownershipBackend)

			authority, present, err := docker.InspectLeaseNetwork(t.Context(), ownershipLease)
			require.ErrorIs(t, err, errLeaseNetworkOwnership)
			require.False(t, present)
			require.False(t, authority.established(), "a refused network yields no authority")
			requireNoNetworkMutation(t, daemon.seen())
			require.Equal(t, []string{"GET /networks/" + name}, daemon.seen())
		})
	}
}

// 3. A missing network is created with ownership labels and verified by its
// returned ID; an owned one is reused without a create.
func TestLeaseNetworkCreatedVerifiedAndReusedWhenOwned(t *testing.T) {
	daemon := newNetworkDaemon(t)
	docker := daemon.dockerClient(t, ownershipBackend)

	_, present, err := docker.InspectLeaseNetwork(t.Context(), ownershipLease)
	require.NoError(t, err)
	require.False(t, present)
	created, err := docker.CreateLeaseNetwork(t.Context(), ownershipLease)
	require.NoError(t, err)
	id := testLeaseNetworkID("created/" + leaseNetworkName(ownershipLease))
	require.Equal(t, leaseNetworkAuthority{leaseUUID: ownershipLease, backendName: ownershipBackend, networkID: id}, created)
	require.Equal(t, []string{"GET /networks/" + leaseNetworkName(ownershipLease), "POST /networks/create", "GET /networks/" + id}, daemon.seen(),
		"the created network is verified by the ID the daemon returned")

	reused, present, err := docker.InspectLeaseNetwork(t.Context(), ownershipLease)
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, created, reused)
	require.Len(t, daemon.seen(), 4, "an owned network is reused without another create")

	// A create that loses the name to a foreign network uses nothing.
	foreign := newNetworkDaemon(t)
	foreign.put(leaseNetworkName(ownershipLease), testLeaseNetworkID("foreign"), foreignLeaseNetworkLabels(), nil)
	_, err = foreign.dockerClient(t, ownershipBackend).CreateLeaseNetwork(t.Context(), ownershipLease)
	require.ErrorIs(t, err, errLeaseNetworkOwnership)
}

// 4. The zero value, and authority for another lease or backend, authorize
// nothing: not a project build, not a removal, not a reclamation.
func TestLeaseNetworkAuthorityZeroWrongLeaseAndWrongBackendAuthorizeNothing(t *testing.T) {
	owned := testLeaseNetworkAuthority(ownershipLease, ownershipBackend)
	require.NoError(t, owned.authorizes(ownershipLease, ownershipBackend))
	for name, authority := range map[string]leaseNetworkAuthority{
		"zero":          {},
		"wrong lease":   testLeaseNetworkAuthority("00000000-0000-4000-8000-000000000999", ownershipBackend),
		"wrong backend": testLeaseNetworkAuthority(ownershipLease, "other-backend"),
		"partial ID":    {leaseUUID: ownershipLease, backendName: ownershipBackend, networkID: "abc123"},
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, authority.authorizes(ownershipLease, ownershipBackend), errLeaseNetworkOwnership)

			params := baseProjectParams()
			params.LeaseUUID, params.BackendName, params.Network = ownershipLease, ownershipBackend, authority
			plan, err := newEffectiveIngressPlan(params.Ingress, params.Stack, params.Items)
			require.NoError(t, err)
			_, err = buildPlannedComposeProject(params, plan)
			require.ErrorIs(t, err, errLeaseNetworkOwnership, "no project can be built without this lease's authority")

			daemon := newNetworkDaemon(t)
			daemon.put(leaseNetworkName(ownershipLease), testLeaseNetworkID(ownershipLease), leaseNetworkLabels(ownershipLease, ownershipBackend), nil)
			if authority.established() {
				// A minted authority for another backend is refused by this backend's client.
				_, err = daemon.dockerClient(t, ownershipBackend).RemoveLeaseNetworkIfEmpty(t.Context(), leaseNetworkAuthority{
					leaseUUID: authority.leaseUUID, backendName: authority.backendName, networkID: authority.networkID,
				})
				if authority.backendName != ownershipBackend {
					require.ErrorIs(t, err, errLeaseNetworkOwnership)
				}
			} else {
				_, err = daemon.dockerClient(t, ownershipBackend).RemoveLeaseNetworkIfEmpty(t.Context(), authority)
				require.ErrorIs(t, err, errLeaseNetworkOwnership)
			}
			if authority.backendName != ownershipBackend || !authority.established() {
				require.Empty(t, daemon.seen(), "a refused authority reaches no daemon")
			}

			// The reclamation sweep takes the lease from the authority itself, so
			// only an unestablished authority or another backend's is refused there.
			if !authority.established() || authority.backendName != "docker" {
				b := newBackendForTest(&mockDockerClient{}, nil)
				defer b.stopCancel()
				_, err = b.removeOrphanedLeaseNetworkUsing(t.Context(), authority, func(context.Context, leaseNetworkAuthority) (networkRemoval, error) {
					t.Fatal("a refused authority must never reach the removal sink")
					return networkRemovalUnknown, nil
				})
				require.ErrorIs(t, err, errLeaseNetworkOwnership)
			}
		})
	}
}

// 3 and 5. Removal acts only on the verified ID: it removes the owned network,
// and a network now holding the name cannot be removed in its place.
func TestRemoveLeaseNetworkActsOnlyOnTheVerifiedID(t *testing.T) {
	authority := testLeaseNetworkAuthority(ownershipLease, ownershipBackend)
	name := leaseNetworkName(ownershipLease)

	t.Run("owned and empty", func(t *testing.T) {
		daemon := newNetworkDaemon(t)
		daemon.put(name, authority.networkID, leaseNetworkLabels(ownershipLease, ownershipBackend), nil)
		outcome, err := daemon.dockerClient(t, ownershipBackend).RemoveLeaseNetworkIfEmpty(t.Context(), authority)
		require.NoError(t, err)
		require.Equal(t, networkRemoved, outcome)
		require.Equal(t, []string{"GET /networks/" + authority.networkID, "DELETE /networks/" + authority.networkID}, daemon.seen())
	})
	t.Run("connected", func(t *testing.T) {
		daemon := newNetworkDaemon(t)
		daemon.put(name, authority.networkID, leaseNetworkLabels(ownershipLease, ownershipBackend), map[string]string{"c": "app"})
		outcome, err := daemon.dockerClient(t, ownershipBackend).RemoveLeaseNetworkIfEmpty(t.Context(), authority)
		require.NoError(t, err)
		require.Equal(t, networkInUse, outcome)
		requireNoNetworkMutation(t, daemon.seen())
	})
	t.Run("replaced under the same name", func(t *testing.T) {
		// The verified network is gone and another one now holds the name.
		daemon := newNetworkDaemon(t)
		daemon.put(name, testLeaseNetworkID("replacement"), leaseNetworkLabels(ownershipLease, ownershipBackend), nil)
		outcome, err := daemon.dockerClient(t, ownershipBackend).RemoveLeaseNetworkIfEmpty(t.Context(), authority)
		require.NoError(t, err)
		require.Equal(t, networkAbsent, outcome)
		requireNoNetworkMutation(t, daemon.seen())
		require.Equal(t, []string{"GET /networks/" + authority.networkID}, daemon.seen(), "the name is never resolved")
	})
}

// admittedCreates records each container create the network policy admitted
// on its way to the next transport check.
type admittedCreates struct {
	mu     sync.Mutex
	bodies [][]byte
	next   http.RoundTripper
}

func (a *admittedCreates) RoundTrip(req *http.Request) (*http.Response, error) {
	if daemonContainerCreateRequest(req) && req.GetBody != nil {
		body, err := req.GetBody()
		if err == nil {
			data, _ := io.ReadAll(body)
			_ = body.Close()
			a.mu.Lock()
			a.bodies = append(a.bodies, data)
			a.mu.Unlock()
		}
	}
	return a.next.RoundTrip(req)
}

// newOwnershipLaunchEngine is the real pinned Compose engine behind Fred's
// per-invocation launch transport, bound to network. Requests the transport
// admits continue to daemon; recorder sees every create that reached the
// transport.
func newOwnershipLaunchEngine(t *testing.T, daemon *networkDaemon, network leaseNetworkAuthority) (composeapi.Compose, *admittedCreates) {
	t.Helper()
	scope := newDaemonLaunchScope(t.Context(), nil)
	t.Cleanup(scope.close)
	recorder := &admittedCreates{}
	recorder.next = daemonLaunchTransport{
		next: daemon, scope: scope, profiles: tenantSeccompTestProfiles(), network: launchNetworkPolicy{authority: network},
	}
	engine, err := newComposeEngine("", &http.Client{Transport: recorder})
	require.NoError(t, err)
	return engine, recorder
}

// ownershipProject is the lease's real planned project with an immutable image.
func ownershipProject(t *testing.T, network leaseNetworkAuthority) *composetypes.Project {
	t.Helper()
	params := baseProjectParams()
	params.LeaseUUID, params.BackendName, params.Network = ownershipLease, ownershipBackend, network
	project := buildTestComposeProject(t, params)
	for name, service := range project.Services {
		service.Image = testImageID
		project.Services[name] = service
	}
	return project
}

// 1 and 5, through the pinned Compose execution path: the lease attaches to
// exactly the verified network ID. A foreign network that holds the lease's
// name is never named, created, or attached to, so replacing the network under
// that name cannot redirect the launch.
func TestComposeLaunchAttachesOnlyToTheVerifiedNetworkID(t *testing.T) {
	authority := testLeaseNetworkAuthority(ownershipLease, ownershipBackend)
	daemon := newNetworkDaemon(t)
	daemon.put("verified", authority.networkID, leaseNetworkLabels(ownershipLease, ownershipBackend), nil)
	// The name now resolves to a foreign network with an unrelated container.
	daemon.put(leaseNetworkName(ownershipLease), testLeaseNetworkID("foreign"), foreignLeaseNetworkLabels(),
		map[string]string{"unrelated": "other-tenant-db"})

	engine, recorder := newOwnershipLaunchEngine(t, daemon, authority)
	err := engine.Create(t.Context(), ownershipProject(t, authority), composeapi.CreateOptions{})
	// The network policy admits the create; the tenant seccomp wire check,
	// which this synthetic project does not satisfy, refuses it next.
	require.ErrorIs(t, err, tenantseccomp.ErrRefused)
	require.NotErrorIs(t, err, errLeaseNetworkOwnership)

	recorder.mu.Lock()
	creates := append([][]byte(nil), recorder.bodies...)
	recorder.mu.Unlock()
	require.NotEmpty(t, creates, "Compose emitted the create")
	for _, body := range creates {
		var request struct {
			HostConfig       struct{ NetworkMode string }
			NetworkingConfig struct {
				EndpointsConfig map[string]struct{ Aliases []string }
			}
		}
		require.NoError(t, json.Unmarshal(body, &request))
		require.Equal(t, authority.networkID, request.HostConfig.NetworkMode)
		require.Len(t, request.NetworkingConfig.EndpointsConfig, 1)
		require.Contains(t, request.NetworkingConfig.EndpointsConfig, authority.networkID)
		require.Contains(t, request.NetworkingConfig.EndpointsConfig[authority.networkID].Aliases, "web")
		require.NotContains(t, string(body), testLeaseNetworkID("foreign"))
	}
	requireNoNetworkMutation(t, daemon.seen())
	for _, request := range daemon.seen() {
		require.NotEqual(t, "GET /networks/"+leaseNetworkName(ownershipLease), request, "the lease network is never resolved by name")
	}
}

// 1, through the pinned Compose execution path: even a project that names the
// foreign network directly, bypassing the builder, cannot attach to it. The
// launch transport refuses the create before it reaches the daemon.
func TestComposeLaunchTransportRefusesAttachmentOutsideTheAuthority(t *testing.T) {
	authority := testLeaseNetworkAuthority(ownershipLease, ownershipBackend)
	name := leaseNetworkName(ownershipLease)
	for label, network := range map[string]composetypes.NetworkConfig{
		"by name, external":     {Name: name, External: true},
		"by name, managed":      {Name: name},
		"another network by ID": {Name: testLeaseNetworkID("foreign"), External: true},
	} {
		t.Run(label, func(t *testing.T) {
			daemon := newNetworkDaemon(t)
			daemon.put(name, testLeaseNetworkID("foreign"), foreignLeaseNetworkLabels(), map[string]string{"unrelated": "other-tenant-db"})
			project := ownershipProject(t, authority)
			project.Networks = composetypes.Networks{leaseNetworkKey: network}

			engine, recorder := newOwnershipLaunchEngine(t, daemon, authority)
			err := engine.Create(t.Context(), project, composeapi.CreateOptions{})
			require.ErrorIs(t, err, errLeaseNetworkOwnership, "the network policy refuses the create")
			recorder.mu.Lock()
			require.NotEmpty(t, recorder.bodies, "Compose did emit a create")
			recorder.mu.Unlock()
			require.Empty(t, daemon.createBodies(), "no create naming another network reaches the daemon")
			requireNoNetworkMutation(t, daemon.seen())
		})
	}
}

// 4, at the wire: without authority, a create may name no user network, and
// every network mutation is refused whatever the authority.
func TestLaunchNetworkPolicyRefusesUnauthorizedRequests(t *testing.T) {
	authority := testLeaseNetworkAuthority(ownershipLease, ownershipBackend)
	createWith := func(t *testing.T, mode string, endpoints ...string) *http.Request {
		t.Helper()
		config := map[string]any{}
		for _, endpoint := range endpoints {
			config[endpoint] = map[string]any{}
		}
		body, err := json.Marshal(map[string]any{"HostConfig": map[string]any{"NetworkMode": mode}, "NetworkingConfig": map[string]any{"EndpointsConfig": config}})
		require.NoError(t, err)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://docker.invalid/v1.51/containers/create", bytes.NewReader(body))
		require.NoError(t, err)
		return req
	}
	policy := launchNetworkPolicy{authority: authority}
	require.NoError(t, policy.refuse(createWith(t, authority.networkID, authority.networkID)))
	for name, req := range map[string]*http.Request{
		"by name":         createWith(t, leaseNetworkName(ownershipLease), leaseNetworkName(ownershipLease)),
		"another ID":      createWith(t, testLeaseNetworkID("foreign"), testLeaseNetworkID("foreign")),
		"an extra":        createWith(t, authority.networkID, authority.networkID, "bridge"),
		"mode mismatch":   createWith(t, "none", authority.networkID),
		"default network": createWith(t, ""),
	} {
		require.ErrorIs(t, policy.refuse(req), errLeaseNetworkOwnership, name)
	}

	var zero launchNetworkPolicy
	require.NoError(t, zero.refuse(createWith(t, "none")))
	require.ErrorIs(t, zero.refuse(createWith(t, authority.networkID, authority.networkID)), errLeaseNetworkOwnership,
		"no authority admits no user network")

	for _, request := range []struct{ method, path string }{
		{http.MethodPost, "/v1.51/networks/create"},
		{http.MethodDelete, "/v1.51/networks/" + authority.networkID},
		{http.MethodPost, "/v1.51/networks/" + authority.networkID + "/connect"},
		{http.MethodPost, "/v1.51/networks/" + authority.networkID + "/disconnect"},
		{http.MethodPost, "/v1.51/networks/prune"},
	} {
		req, err := http.NewRequestWithContext(t.Context(), request.method, "http://docker.invalid"+request.path, nil)
		require.NoError(t, err)
		require.ErrorIs(t, policy.refuse(req), errComposeNetworkMutationRefused, request.path)
		require.ErrorIs(t, zero.refuse(req), errComposeNetworkMutationRefused, request.path)
	}
}

// 2, through the pinned Compose teardown path: Compose rebuilds a project's
// networks from Compose labels alone. A network carrying the lease project's
// Compose labels but another owner's Fred labels, or no Fred labels, survives
// Fred's teardown, which still completes.
func TestComposeTeardownLeavesConflictingNetworks(t *testing.T) {
	project := composeProjectName(ownershipLease)
	for label, labels := range map[string]map[string]string{
		"matching Compose labels, foreign Fred labels": {
			composeapi.ProjectLabel: project, composeapi.NetworkLabel: leaseNetworkKey,
			LabelManaged: "true", LabelNetworkScope: networkScopeLease,
			LabelLeaseUUID: "00000000-0000-4000-8000-000000000999", LabelBackendName: "other-backend",
		},
		"matching Compose labels, no Fred labels": {composeapi.ProjectLabel: project, composeapi.NetworkLabel: leaseNetworkKey},
	} {
		t.Run(label, func(t *testing.T) {
			daemon := newNetworkDaemon(t)
			daemon.put(leaseNetworkName(ownershipLease), testLeaseNetworkID("conflict"), labels, nil)
			daemon.containers = fmt.Sprintf(`[{"Id":"c1","Names":["/%s-web-0"],"State":"running","Labels":{%q:%q,%q:"web",%q:"False"}}]`,
				project, composeapi.ProjectLabel, project, composeapi.ServiceLabel, composeapi.OneoffLabel)
			engine, err := newComposeEngine("", newComposeReadHTTPClient(daemon))
			require.NoError(t, err)
			service := &composeService{down: engine.Down}

			require.NoError(t, service.Down(t.Context(), project, time.Second), "the container teardown completed")
			require.Contains(t, daemon.seen(), "DELETE /containers/c1", "Compose removed the container")
			requireNoNetworkMutation(t, daemon.seen())
			daemon.mu.Lock()
			_, survives := daemon.networks[testLeaseNetworkID("conflict")]
			daemon.mu.Unlock()
			require.True(t, survives)
		})
	}
}

// Compensation recreates a source container on networks re-verified now and
// named by ID; a network no longer owned refuses the compensation.
func TestBindCompensationNetworksAttachesOnlyToVerifiedIDs(t *testing.T) {
	const tenant = "manifest1tenant"
	lease, legacy := leaseNetworkName(ownershipLease), TenantNetworkName(tenant)
	owned := func(daemon *networkDaemon) {
		daemon.put(lease, testLeaseNetworkID(ownershipLease), leaseNetworkLabels(ownershipLease, ownershipBackend), nil)
		daemon.put(legacy, testLeaseNetworkID(tenant), map[string]string{LabelManaged: "true", LabelTenant: tenant, LabelBackendName: ownershipBackend}, nil)
	}
	for name, source := range map[string]compensationNetworks{
		"lease network":  {mode: lease, endpoints: map[string]*networktypes.EndpointSettings{lease: {Aliases: []string{"web"}}}},
		"tenant network": {mode: legacy, endpoints: map[string]*networktypes.EndpointSettings{legacy: {Aliases: []string{"web"}}}},
	} {
		t.Run(name, func(t *testing.T) {
			daemon := newNetworkDaemon(t)
			owned(daemon)
			bound, err := daemon.dockerClient(t, ownershipBackend).bindCompensationNetworks(t.Context(), ownershipLease, tenant, source)
			require.NoError(t, err)
			id := daemon.byName[source.mode]
			require.Equal(t, id, bound.mode)
			require.Len(t, bound.endpoints, 1)
			require.Equal(t, []string{"web"}, bound.endpoints[id].Aliases)
		})
	}
	t.Run("a container attached by the verified ID", func(t *testing.T) {
		// Docker keeps the ID as NetworkMode and keys the endpoint by name.
		daemon := newNetworkDaemon(t)
		owned(daemon)
		id := testLeaseNetworkID(ownershipLease)
		bound, err := daemon.dockerClient(t, ownershipBackend).bindCompensationNetworks(t.Context(), ownershipLease, tenant,
			compensationNetworks{mode: id, endpoints: map[string]*networktypes.EndpointSettings{lease: {Aliases: []string{"web"}}}})
		require.NoError(t, err)
		require.Equal(t, compensationNetworks{mode: id, endpoints: map[string]*networktypes.EndpointSettings{id: {Aliases: []string{"web"}}}}, bound)
	})
	t.Run("network mode none", func(t *testing.T) {
		// Without isolation a container runs in network mode none, which
		// Docker also lists as its one endpoint.
		daemon := newNetworkDaemon(t)
		source := compensationNetworks{mode: "none", endpoints: map[string]*networktypes.EndpointSettings{"none": {}}}
		bound, err := daemon.dockerClient(t, ownershipBackend).bindCompensationNetworks(t.Context(), ownershipLease, tenant, source)
		require.NoError(t, err)
		require.Equal(t, source, bound)
		require.Empty(t, daemon.seen(), "a predefined network needs no ownership check")
	})
	t.Run("an ID that resolves to another lease's network", func(t *testing.T) {
		daemon := newNetworkDaemon(t)
		other := "00000000-0000-4000-8000-000000000999"
		daemon.put(leaseNetworkName(other), testLeaseNetworkID(other), leaseNetworkLabels(other, ownershipBackend), nil)
		_, err := daemon.dockerClient(t, ownershipBackend).bindCompensationNetworks(t.Context(), ownershipLease, tenant,
			compensationNetworks{mode: testLeaseNetworkID(other), endpoints: map[string]*networktypes.EndpointSettings{}})
		require.ErrorIs(t, err, errLeaseNetworkOwnership)
	})
	t.Run("foreign under the lease network's name", func(t *testing.T) {
		daemon := newNetworkDaemon(t)
		daemon.put(lease, testLeaseNetworkID("foreign"), foreignLeaseNetworkLabels(), nil)
		_, err := daemon.dockerClient(t, ownershipBackend).bindCompensationNetworks(t.Context(), ownershipLease, tenant,
			compensationNetworks{mode: lease, endpoints: map[string]*networktypes.EndpointSettings{lease: {}}})
		require.ErrorIs(t, err, errLeaseNetworkOwnership)
	})
	t.Run("a network of neither", func(t *testing.T) {
		daemon := newNetworkDaemon(t)
		daemon.put("elsewhere", testLeaseNetworkID("elsewhere"), map[string]string{LabelManaged: "true"}, nil)
		_, err := daemon.dockerClient(t, ownershipBackend).bindCompensationNetworks(t.Context(), ownershipLease, tenant,
			compensationNetworks{mode: "elsewhere", endpoints: map[string]*networktypes.EndpointSettings{"elsewhere": {}}})
		require.ErrorIs(t, err, errLeaseNetworkOwnership)
	})
}

// An ownership conflict fails the provision as a backend failure, never as an
// invalid tenant manifest, and attaches nothing.
func TestProvisionOwnershipConflictIsABackendFailure(t *testing.T) {
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
		InspectLeaseNetworkFn: func(context.Context, string) (leaseNetworkAuthority, bool, error) {
			return leaseNetworkAuthority{}, false, fmt.Errorf("%w: network %q carries another owner's labels", errLeaseNetworkOwnership, "fred-x")
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: durableCallbackTestLeaseUUID,
			Status: backend.ProvisionStatusProvisioning, Quantity: 1}},
	})
	b.cfg.NetworkIsolation = ptrBool(true)
	b.provisions[durableCallbackTestLeaseUUID].CallbackURL = callbackServer.URL
	launched := false
	b.compose.(*mockComposeExecutor).UpFn = func(context.Context, *composetypes.Project, composeUpOpts) error {
		launched = true
		return nil
	}
	req := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	stack, err := manifest.ParseManifest(validManifestJSON("nginx:latest"))
	require.NoError(t, err)
	b.doProvisionAndFire(t, t.Context(), req, stack, map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}, b.logger)

	require.False(t, launched, "nothing launches without network authority")
	b.provisionsMu.RLock()
	prov := b.provisions[durableCallbackTestLeaseUUID]
	b.provisionsMu.RUnlock()
	require.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Equal(t, backend.ReasonInternal, prov.Reason, "an ownership conflict is a backend failure")
}
