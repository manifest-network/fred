package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strings"

	networktypes "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
)

// errLeaseNetworkOwnership reports a Docker network a lease's containers must
// not use: one that carries another owner's labels, or none. It is a backend
// failure, never a tenant validation refusal: nothing in the manifest caused it,
// and nothing the tenant changes can fix it.
var errLeaseNetworkOwnership = errors.New("lease network ownership is not established")

// errComposeNetworkMutationRefused is the transport refusal of a network
// create, removal, connect or disconnect issued by Compose. Fred alone creates
// and removes lease networks, through leaseNetworkAuthority; Compose only
// attaches containers to the exact network that authority names.
var errComposeNetworkMutationRefused = errors.New("compose never creates, removes or reconnects networks")

// leaseNetworkAuthority is proof that one Docker network, identified by its
// full immutable ID, is the network this backend created for one lease. Only
// DockerClient mints one, after inspecting the network and verifying every
// ownership label, or after creating it and verifying the result. The zero
// value authorizes nothing.
//
// Attachment and removal act on the ID, never on the name, so a network
// replaced under the same name after the check cannot receive the lease's
// containers or be removed in its place.
type leaseNetworkAuthority struct {
	leaseUUID   string
	backendName string
	networkID   string
}

// fullNetworkID matches a complete Docker network ID. Docker resolves a
// reference that equals a full ID before any name or ID prefix, so a full ID
// can never be redirected to another network.
var fullNetworkID = regexp.MustCompile(`^[0-9a-f]{64}$`)

// established reports whether the authority was minted at all.
func (a leaseNetworkAuthority) established() bool {
	return a.leaseUUID != "" && fullNetworkID.MatchString(a.networkID)
}

// authorizes checks the authority against the exact lease and backend an
// operation acts for. The zero value and an authority for any other lease or
// backend authorize nothing.
func (a leaseNetworkAuthority) authorizes(leaseUUID, backendName string) error {
	if !a.established() {
		return fmt.Errorf("%w: no verified network", errLeaseNetworkOwnership)
	}
	if leaseUUID == "" || a.leaseUUID != leaseUUID || a.backendName != backendName {
		return fmt.Errorf("%w: the verified network belongs to another lease or backend", errLeaseNetworkOwnership)
	}
	return nil
}

// leaseNetworkName returns the name of the lease's own bridge network, the
// Compose project name. The name only labels the network for operators and
// Traefik; every attachment and removal uses the verified network ID.
func leaseNetworkName(leaseUUID string) string {
	return composeProjectName(leaseUUID)
}

// networkScopeLease is the LabelNetworkScope value of a per-lease network.
const networkScopeLease = "lease"

// leaseNetworkLabels are the ownership labels Fred creates a lease network
// with and requires of an existing one.
func leaseNetworkLabels(leaseUUID, backendName string) map[string]string {
	labels := map[string]string{
		LabelManaged:      "true",
		LabelLeaseUUID:    leaseUUID,
		LabelNetworkScope: networkScopeLease,
	}
	if backendName != "" {
		labels[LabelBackendName] = backendName
	}
	return labels
}

// verifyLeaseNetwork checks an inspected network against the lease and backend
// it must belong to: the exact name, a full ID, and every ownership label.
// A missing label is as disqualifying as a conflicting one.
func verifyLeaseNetwork(resp networktypes.Inspect, leaseUUID, backendName string) error {
	name := leaseNetworkName(leaseUUID)
	if resp.Name != name || !fullNetworkID.MatchString(resp.ID) {
		return fmt.Errorf("%w: network %q is not the expected lease network", errLeaseNetworkOwnership, resp.Name)
	}
	for key, want := range leaseNetworkLabels(leaseUUID, backendName) {
		if resp.Labels[key] != want {
			return fmt.Errorf("%w: network %q does not carry this lease's and backend's ownership labels", errLeaseNetworkOwnership, name)
		}
	}
	if backendName == "" && resp.Labels[LabelBackendName] != "" {
		return fmt.Errorf("%w: network %q belongs to another backend", errLeaseNetworkOwnership, name)
	}
	return nil
}

// mintLeaseNetworkAuthority is the only constructor of a usable
// leaseNetworkAuthority. It verifies an inspected network against the lease and
// backend first.
func mintLeaseNetworkAuthority(resp networktypes.Inspect, leaseUUID, backendName string) (leaseNetworkAuthority, error) {
	if leaseUUID == "" {
		return leaseNetworkAuthority{}, fmt.Errorf("%w: no lease", errLeaseNetworkOwnership)
	}
	if err := verifyLeaseNetwork(resp, leaseUUID, backendName); err != nil {
		return leaseNetworkAuthority{}, err
	}
	return leaseNetworkAuthority{leaseUUID: leaseUUID, backendName: backendName, networkID: resp.ID}, nil
}

// InspectLeaseNetwork returns authority over the lease's existing network, or
// present=false when no network of that name exists. It changes nothing. An
// existing network is accepted only when it carries this lease's and backend's
// ownership labels; any other network of that name is refused and left
// untouched: never attached to, adopted, relabeled or removed.
func (d *DockerClient) InspectLeaseNetwork(ctx context.Context, leaseUUID string) (authority leaseNetworkAuthority, present bool, err error) {
	if leaseUUID == "" {
		return leaseNetworkAuthority{}, false, fmt.Errorf("%w: no lease", errLeaseNetworkOwnership)
	}
	existing, err := d.client.NetworkInspect(ctx, leaseNetworkName(leaseUUID), networktypes.InspectOptions{})
	if err != nil {
		if client.IsErrNotFound(err) {
			return leaseNetworkAuthority{}, false, nil
		}
		return leaseNetworkAuthority{}, false, fmt.Errorf("inspect lease network: %w", err)
	}
	authority, err = mintLeaseNetworkAuthority(existing, leaseUUID, d.backendName)
	return authority, err == nil, err
}

// CreateLeaseNetwork creates the lease's network with its ownership labels and
// returns authority over it only after inspecting the ID the daemon returned.
// When another creator wins the name first, its network is used only if it
// proves to be this lease's on this backend.
func (d *DockerClient) CreateLeaseNetwork(ctx context.Context, leaseUUID string) (leaseNetworkAuthority, error) {
	if leaseUUID == "" {
		return leaseNetworkAuthority{}, fmt.Errorf("%w: no lease", errLeaseNetworkOwnership)
	}
	name := leaseNetworkName(leaseUUID)
	created, err := d.client.NetworkCreate(ctx, name, networktypes.CreateOptions{
		Driver:   "bridge",
		Internal: false, // Must be false: Internal:true prevents port publishing (moby#36174)
		Labels:   leaseNetworkLabels(leaseUUID, d.backendName),
	})
	if err != nil {
		if !errdefs.IsConflict(err) {
			return leaseNetworkAuthority{}, fmt.Errorf("create lease network: %w", err)
		}
		raced, inspectErr := d.client.NetworkInspect(ctx, name, networktypes.InspectOptions{})
		if inspectErr != nil {
			return leaseNetworkAuthority{}, fmt.Errorf("inspect lease network after a create conflict: %w", inspectErr)
		}
		return mintLeaseNetworkAuthority(raced, leaseUUID, d.backendName)
	}
	if !fullNetworkID.MatchString(created.ID) {
		return leaseNetworkAuthority{}, fmt.Errorf("%w: the daemon returned no full network ID", errLeaseNetworkOwnership)
	}
	verified, err := d.client.NetworkInspect(ctx, created.ID, networktypes.InspectOptions{})
	if err != nil {
		return leaseNetworkAuthority{}, fmt.Errorf("inspect created lease network: %w", err)
	}
	if verified.ID != created.ID {
		return leaseNetworkAuthority{}, fmt.Errorf("%w: the created network ID did not resolve to itself", errLeaseNetworkOwnership)
	}
	return mintLeaseNetworkAuthority(verified, leaseUUID, d.backendName)
}

// leaseNetworkCandidate mints authority from a network the fleet inventory
// listed, for the reclamation sweep. The listing only nominates a candidate:
// removal re-inspects the exact ID and verifies ownership again.
func leaseNetworkCandidate(listed networktypes.Inspect, backendName string) (leaseNetworkAuthority, error) {
	return mintLeaseNetworkAuthority(listed, listed.Labels[LabelLeaseUUID], backendName)
}

// RemoveLeaseNetworkIfEmpty removes the exact network the authority names, if
// it still carries the lease's and this backend's ownership labels and no
// container is connected. It never resolves the network by name.
func (d *DockerClient) RemoveLeaseNetworkIfEmpty(ctx context.Context, authority leaseNetworkAuthority) (networkRemoval, error) {
	if err := authority.authorizes(authority.leaseUUID, d.backendName); err != nil {
		return networkRemovalUnknown, err
	}
	resp, err := d.client.NetworkInspect(ctx, authority.networkID, networktypes.InspectOptions{})
	if err != nil {
		if client.IsErrNotFound(err) {
			return networkAbsent, nil
		}
		return networkRemovalUnknown, fmt.Errorf("inspect lease network: %w", err)
	}
	if resp.ID != authority.networkID {
		return networkRemovalUnknown, fmt.Errorf("%w: the network ID did not resolve to itself", errLeaseNetworkOwnership)
	}
	if err := verifyLeaseNetwork(resp, authority.leaseUUID, d.backendName); err != nil {
		return networkRemovalUnknown, err
	}
	if len(resp.Containers) > 0 {
		return networkInUse, nil
	}
	if err := d.client.NetworkRemove(ctx, authority.networkID); err != nil {
		if client.IsErrNotFound(err) {
			return networkAbsent, nil
		}
		return networkRemovalUnknown, fmt.Errorf("remove lease network: %w", err)
	}
	return networkRemoved, nil
}

// verifyTenantNetwork checks an inspected legacy per-tenant network against the
// tenant and backend it must belong to.
func verifyTenantNetwork(resp networktypes.Inspect, tenant, backendName string) error {
	if tenant == "" || resp.Name != TenantNetworkName(tenant) || !fullNetworkID.MatchString(resp.ID) ||
		resp.Labels[LabelManaged] != "true" || resp.Labels[LabelTenant] != tenant ||
		resp.Labels[LabelBackendName] != backendName {
		return fmt.Errorf("%w: network %q is not this tenant's network on this backend", errLeaseNetworkOwnership, resp.Name)
	}
	return nil
}

// RemoveTenantNetworkIfEmpty removes the tenant's legacy network if it still
// carries this tenant's and backend's ownership labels and no container is
// connected. The verified ID, not the name, is removed.
func (d *DockerClient) RemoveTenantNetworkIfEmpty(ctx context.Context, tenant string) (networkRemoval, error) {
	resp, err := d.client.NetworkInspect(ctx, TenantNetworkName(tenant), networktypes.InspectOptions{})
	if err != nil {
		if client.IsErrNotFound(err) {
			return networkAbsent, nil
		}
		return networkRemovalUnknown, fmt.Errorf("failed to inspect network: %w", err)
	}
	if err := verifyTenantNetwork(resp, tenant, d.backendName); err != nil {
		return networkRemovalUnknown, err
	}
	if len(resp.Containers) > 0 {
		return networkInUse, nil
	}
	if err := d.client.NetworkRemove(ctx, resp.ID); err != nil {
		if client.IsErrNotFound(err) {
			return networkAbsent, nil
		}
		return networkRemovalUnknown, fmt.Errorf("failed to remove network: %w", err)
	}
	return networkRemoved, nil
}

// compensationNetworks is the network part of one compensation container: its
// primary network mode and its endpoints, keyed by network reference.
type compensationNetworks struct {
	mode      string
	endpoints map[string]*networktypes.EndpointSettings
}

// bindCompensationNetworks rewrites a compensation snapshot's network targets
// from names to verified network IDs. A source container was attached either to
// its lease network or, before per-lease networks, to its tenant network; each
// is re-verified against this lease, tenant and backend before the recreated
// container may attach to it. Any other network, or one whose ownership no
// longer holds, refuses the compensation, which then fails like any other
// unavailable source.
func (d *DockerClient) bindCompensationNetworks(ctx context.Context, leaseUUID, tenant string, target compensationNetworks) (compensationNetworks, error) {
	resolved := make(map[string]string, len(target.endpoints)+1)
	// A source container names its network by ID when Fred attached it by its
	// verified ID, and by name in its endpoint map. Either reference is checked
	// by the network it resolves to now, never by the reference itself.
	resolve := func(ref string) (string, error) {
		if id, ok := resolved[ref]; ok {
			return id, nil
		}
		resp, err := d.client.NetworkInspect(ctx, ref, networktypes.InspectOptions{})
		if err != nil {
			return "", fmt.Errorf("inspect compensation network %q: %w", ref, err)
		}
		switch resp.Name {
		case leaseNetworkName(leaseUUID):
			err = verifyLeaseNetwork(resp, leaseUUID, d.backendName)
		case TenantNetworkName(tenant):
			err = verifyTenantNetwork(resp, tenant, d.backendName)
		default:
			err = fmt.Errorf("%w: network %q belongs to neither this lease nor its tenant", errLeaseNetworkOwnership, resp.Name)
		}
		if err != nil {
			return "", err
		}
		resolved[ref] = resp.ID
		return resp.ID, nil
	}
	bound := compensationNetworks{mode: target.mode, endpoints: make(map[string]*networktypes.EndpointSettings, len(target.endpoints))}
	if target.mode != "" && !nonAttachingNetworkMode(target.mode) {
		id, err := resolve(target.mode)
		if err != nil {
			return compensationNetworks{}, err
		}
		bound.mode = id
	}
	for _, name := range slices.Sorted(maps.Keys(target.endpoints)) {
		if nonAttachingNetworkMode(name) {
			// Docker's predefined networks: a container in network mode none
			// lists its "none" endpoint. No other network can hold these
			// reserved names, and they reach no lease's or tenant's network.
			bound.endpoints[name] = target.endpoints[name]
			continue
		}
		id, err := resolve(name)
		if err != nil {
			return compensationNetworks{}, err
		}
		bound.endpoints[id] = target.endpoints[name]
	}
	return bound, nil
}

// nonAttachingNetworkMode reports a NetworkMode that names no user network.
func nonAttachingNetworkMode(mode string) bool {
	return mode == "none" || mode == "default" || mode == "bridge"
}

// daemonNetworkMutationRequest reports a request that creates, removes, prunes,
// connects or disconnects a Docker network.
func daemonNetworkMutationRequest(req *http.Request) bool {
	path := req.URL.Path
	if _, rest, found := strings.Cut(path, "/networks"); found {
		switch req.Method {
		case http.MethodDelete:
			return true
		case http.MethodPost:
			return rest == "/create" || rest == "/prune" ||
				strings.HasSuffix(rest, "/connect") || strings.HasSuffix(rest, "/disconnect")
		}
	}
	return false
}

// launchNetworkPolicy is the network rule one Compose launch enforces on the
// wire: every container create must attach to exactly the verified lease
// network, or, with isolation disabled, to no user network at all.
type launchNetworkPolicy struct {
	authority leaseNetworkAuthority
}

// refuse checks one request. Network mutations are always refused. A container
// create is read from a copy of its body and must name only the verified
// network ID; the request itself is left untouched.
func (p launchNetworkPolicy) refuse(req *http.Request) error {
	if daemonNetworkMutationRequest(req) {
		return errComposeNetworkMutationRefused
	}
	if !daemonContainerCreateRequest(req) {
		return nil
	}
	if req.GetBody == nil {
		return fmt.Errorf("%w: the create request body cannot be read again", errLeaseNetworkOwnership)
	}
	body, err := req.GetBody()
	if err != nil {
		return fmt.Errorf("%w: copy the create request body: %w", errLeaseNetworkOwnership, err)
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(io.LimitReader(body, maxLaunchCreateBody+1))
	if err != nil {
		return fmt.Errorf("%w: read the create request body: %w", errLeaseNetworkOwnership, err)
	}
	if len(data) > maxLaunchCreateBody {
		return fmt.Errorf("%w: the create request body exceeds %d bytes", errLeaseNetworkOwnership, maxLaunchCreateBody)
	}
	return p.verifyCreateBody(data)
}

func (p launchNetworkPolicy) verifyCreateBody(data []byte) error {
	var request struct {
		HostConfig *struct {
			NetworkMode string `json:"NetworkMode"`
		} `json:"HostConfig"`
		NetworkingConfig *struct {
			EndpointsConfig map[string]json.RawMessage `json:"EndpointsConfig"`
		} `json:"NetworkingConfig"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&request); err != nil {
		return fmt.Errorf("%w: decode the create request: %w", errLeaseNetworkOwnership, err)
	}
	mode := ""
	if request.HostConfig != nil {
		mode = request.HostConfig.NetworkMode
	}
	var endpoints []string
	if request.NetworkingConfig != nil {
		endpoints = slices.Sorted(maps.Keys(request.NetworkingConfig.EndpointsConfig))
	}
	if !p.authority.established() {
		if (mode == "" || mode == "none") && len(endpoints) == 0 {
			return nil
		}
		return fmt.Errorf("%w: the launch holds no network authority but the create names network %q", errLeaseNetworkOwnership, mode)
	}
	id := p.authority.networkID
	if mode != id || !slices.Equal(endpoints, []string{id}) {
		return fmt.Errorf("%w: the create does not attach exactly to the verified lease network", errLeaseNetworkOwnership)
	}
	return nil
}

// maxLaunchNetworkInspectBody bounds the network inspection read before a
// launch request. Its Containers map lists only this lease's containers.
const maxLaunchNetworkInspectBody = 4 << 20

// confirm runs before a container create or start under an established
// authority. Docker resolves a network reference by exact ID and then by exact
// name, at create and again at start, so the full ID protects the attachment
// only while the verified network exists: once it is gone, a network named
// with that ID would receive the container. The daemon is asked what the ID
// resolves to now, through the launch's own transport, and the request goes
// ahead only when that is the authority's network itself.
func (p launchNetworkPolicy) confirm(req *http.Request, daemon http.RoundTripper) error {
	if !p.authority.established() || !daemonContainerLaunchRequest(req) {
		return nil
	}
	prefix, _, _ := strings.Cut(req.URL.Path, "/containers/")
	// The parsed URL is reused rather than re-parsed: a Docker client on a
	// unix socket escapes the socket path into the URL host.
	target := *req.URL
	target.Path, target.RawPath, target.RawQuery = prefix+"/networks/"+p.authority.networkID, "", ""
	inspect, err := http.NewRequestWithContext(req.Context(), http.MethodGet, "/", nil)
	if err != nil {
		return fmt.Errorf("%w: build the lease network inspection: %w", errLeaseNetworkOwnership, err)
	}
	inspect.URL, inspect.Host = &target, req.Host
	if agent := req.Header.Get("User-Agent"); agent != "" {
		inspect.Header.Set("User-Agent", agent)
	}
	response, err := daemon.RoundTrip(inspect)
	if err != nil {
		return fmt.Errorf("%w: inspect the verified lease network: %w", errLeaseNetworkOwnership, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: the verified lease network does not resolve (status %d)", errLeaseNetworkOwnership, response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxLaunchNetworkInspectBody+1))
	if err != nil {
		return fmt.Errorf("%w: read the lease network inspection: %w", errLeaseNetworkOwnership, err)
	}
	if len(data) > maxLaunchNetworkInspectBody {
		return fmt.Errorf("%w: the lease network inspection exceeds %d bytes", errLeaseNetworkOwnership, maxLaunchNetworkInspectBody)
	}
	var resolved networktypes.Inspect
	if err := json.Unmarshal(data, &resolved); err != nil {
		return fmt.Errorf("%w: decode the lease network inspection: %w", errLeaseNetworkOwnership, err)
	}
	if resolved.ID != p.authority.networkID {
		return fmt.Errorf("%w: the verified lease network ID now resolves to another network", errLeaseNetworkOwnership)
	}
	return verifyLeaseNetwork(resolved, p.authority.leaseUUID, p.authority.backendName)
}
