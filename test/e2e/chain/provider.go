//go:build integration

package chain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/manifest-network/fred/test/e2e"
)

// ProviderBinDirEnv names the directory holding the providerd and
// placement-preflight binaries under test. `make test-e2e` builds them and
// sets this. It is separate from the backend's directory so that the two
// sides can come from different releases.
const ProviderBinDirEnv = "FRED_E2E_PROVIDER_BIN_DIR"

const (
	providerKey = "provider"
	// billingDenom prices SKUs and funds tenants. `testnet init-files` gives
	// it to the validator.
	billingDenom = "testtoken"
	// insecureChainConfirmation is what placement-preflight requires before
	// it accepts lease evidence from a chain endpoint without TLS.
	insecureChainConfirmation = "I ACCEPT UNAUTHENTICATED CHAIN EVIDENCE FOR LOCAL DEVELOPMENT"
)

// Provider is a whole provider: a chain, one or more docker-backends, and the
// providerd that joins them. Each test starts its own, so tests share only
// the Docker daemon.
type Provider struct {
	Chain *Chain
	// Backends are the provider's backends, each on its own loopback with its
	// own storage identity.
	Backends []*e2e.Backend
	// URL is the base URL of providerd's API.
	URL string
	// UUID is the provider's UUID on the chain.
	UUID string
	// SKUs are served by every backend, so providerd chooses where a lease of
	// one goes: the least loaded backend. Each backend's own SKUs.Pinned is
	// served by that backend alone.
	SKUs e2e.SKUs

	t       *testing.T
	binDir  string
	process *e2e.Process
	command func(ctx context.Context) *exec.Cmd
	tenants int
}

// StartProvider starts a chain, registers a provider and its SKUs on it,
// starts the given number of backends, and bootstraps providerd as an
// operator does: create the placement database against the empty backends,
// then start the daemon. It skips the test when the host cannot run the
// suite. Cleanup stops providerd first, then the backends, then the chain.
func StartProvider(t *testing.T, backends int) *Provider {
	t.Helper()
	if os.Getuid() != 0 {
		t.Skip("e2e needs root: it mounts an XFS loopback and runs the binaries as a service account")
	}
	binDir := os.Getenv(ProviderBinDirEnv)
	if binDir == "" {
		t.Skipf("%s is not set; run `make test-e2e`", ProviderBinDirEnv)
	}
	for _, name := range []string{"providerd", "placement-preflight", "lease-token"} {
		_, err := os.Stat(filepath.Join(binDir, name))
		require.NoError(t, err, "%s must hold a %s binary", ProviderBinDirEnv, name)
	}

	// A docker-backend needs a Docker daemon to itself.
	sockets := e2e.DockerSockets()
	if len(sockets) < backends {
		t.Skipf("%d backends need %d Docker daemons, and %s names %d; start more with scripts/e2e-second-dockerd.sh",
			backends, backends, e2e.DockerSocketsEnv, len(sockets))
	}

	p := &Provider{t: t, binDir: binDir}
	var key Key
	p.Chain = Start(t, func(genesis *Genesis) {
		// A dedicated provider key, funded for fees and allowed to register a
		// provider and SKUs without a governance proposal.
		key = genesis.AddKey(t, providerKey)
		genesis.AddAccount(t, key.Address, "1000000000umfx")
		genesis.Edit(t, func(document map[string]any) {
			sku := document["app_state"].(map[string]any)["sku"].(map[string]any)
			sku["params"].(map[string]any)["allowed_list"] = []string{key.Address}
		})
	})
	created := p.Chain.Tx(t, providerKey, "sku", "create-provider", key.Address, key.Address).Event(t, "provider_created")
	require.Equal(t, key.Address, created["address"])
	p.UUID = created["provider_uuid"]
	p.SKUs = e2e.SKUs{Stateless: p.createSKU("e2e-stateless"), Stateful: p.createSKU("e2e-stateful")}
	for i := range backends {
		served := p.SKUs
		served.Pinned = p.createSKU(fmt.Sprintf("e2e-pinned-%d", i))
		// The backend's own client creates leases for this provider, as
		// providerd does.
		p.Backends = append(p.Backends, e2e.StartBackendFor(t, sockets[i], p.UUID, served))
	}

	// providerd gets its own directory and its own copy of the keyring, both
	// owned by the service account it runs as. Unlike docker-backend it holds
	// no capability, so it must be able to reach its files: the test's
	// temporary root is created for root alone.
	state := t.TempDir()
	require.NoError(t, os.Chmod(filepath.Dir(state), 0o755)) //nolint:gosec // G302: lets the service account traverse to its own files
	keyring := filepath.Join(state, "keyring-test")
	require.NoError(t, os.Mkdir(keyring, 0o700))
	owned := []string{state, keyring}
	entries, err := os.ReadDir(filepath.Join(p.Chain.Home(), "keyring-test"))
	require.NoError(t, err)
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(p.Chain.Home(), "keyring-test", entry.Name())) //nolint:gosec // G304: the chain's own keyring
		require.NoError(t, err)
		owned = append(owned, filepath.Join(keyring, entry.Name()))
		require.NoError(t, os.WriteFile(owned[len(owned)-1], data, 0o600))
	}
	// Without a capability the service account cannot reach the build
	// directory either, which sits under the invoking user's home: it runs
	// copies of the binaries.
	for _, name := range []string{"providerd", "placement-preflight"} {
		owned = append(owned, filepath.Join(state, name))
		copyExecutable(t, filepath.Join(binDir, name), owned[len(owned)-1])
	}
	listenAddr := e2e.FreeLoopbackAddr(t)
	p.URL = "http://" + listenAddr
	encoded, err := yaml.Marshal(p.config(key, state, listenAddr))
	require.NoError(t, err)
	configPath := filepath.Join(state, "providerd.yaml")
	require.NoError(t, os.WriteFile(configPath, encoded, 0o600))
	e2e.GiveToServiceAccount(t, append(owned, configPath)...)

	p.process = e2e.NewProcess(t, "providerd-"+p.Backends[0].Name)
	command := func(ctx context.Context, binary string, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, filepath.Join(state, binary), args...) //nolint:gosec // G204: a binary under test and fixture-built arguments
		cmd.Env = p.process.Env()
		e2e.AsServiceAccount(cmd)
		return cmd
	}
	p.command = func(ctx context.Context) *exec.Cmd { return command(ctx, "providerd", "--config", configPath) }

	// providerd never creates its placement database. A new provider's
	// operator does, once, with every backend running and still empty: the
	// tool prints an acknowledgement bound to this provider and its backends,
	// and creates the database only when given it back.
	names := make([]string, len(p.Backends))
	for i, backend := range p.Backends {
		names[i] = backend.Name
	}
	roster, err := json.Marshal(names)
	require.NoError(t, err)
	confirmation, err := command(t.Context(), "placement-preflight", "-config", configPath,
		"-print-fresh-confirmation", "-expected-backends", string(roster)).Output()
	require.NoError(t, err, "print the fresh-placement confirmation: %s", stderrOf(err))
	out, err := command(t.Context(), "placement-preflight", "-config", configPath,
		"-initialize-fresh", "-expected-backends", string(roster),
		"-confirm-insecure-chain", insecureChainConfirmation,
		"-confirm-quiesced", strings.TrimSpace(string(confirmation))).CombinedOutput()
	require.NoError(t, err, "create the placement database: %s", out)

	t.Cleanup(func() {
		p.Stop()
		p.process.LogOnFailure()
	})
	p.Start()
	return p
}

// Start runs providerd and waits until its API answers. StartProvider calls
// it; a test calls it again after Stop.
func (p *Provider) Start() {
	p.t.Helper()
	p.process.Start(p.command, p.healthy)
}

// Stop sends providerd SIGTERM and waits for it to exit. Backends and their
// workloads keep running. A no-op when providerd is not running.
func (p *Provider) Stop() {
	p.t.Helper()
	p.process.Stop()
}

// Host returns the backend that reports the lease, and fails the test unless
// exactly one does.
func (p *Provider) Host(t *testing.T, leaseUUID string) *e2e.Backend {
	t.Helper()
	var hosts []*e2e.Backend
	for _, backend := range p.Backends {
		_, err := backend.Client.Get(t.Context(), leaseUUID)
		if errors.Is(err, e2e.ErrNotFound) {
			continue
		}
		require.NoError(t, err, "ask backend %s for lease %s", backend.Name, leaseUUID)
		hosts = append(hosts, backend)
	}
	require.Len(t, hosts, 1, "lease %s must be on exactly one backend", leaseUUID)
	return hosts[0]
}

// Metric sums the samples of one of providerd's metrics whose labels include
// every given `name="value"` pair. A metric that was never written reads 0.
func (p *Provider) Metric(t *testing.T, name string, labels ...string) float64 {
	t.Helper()
	var sum float64
samples:
	for _, line := range p.metricLines(t, name) {
		series, value, _ := strings.Cut(line, " ")
		for _, label := range labels {
			if !strings.Contains(series, label) {
				continue samples
			}
		}
		sample, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		require.NoError(t, err, "metric line %q", line)
		sum += sample
	}
	return sum
}

// metricLines returns the sample lines of one metric from providerd's
// /metrics endpoint.
func (p *Provider) metricLines(t *testing.T, name string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL+"/metrics", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var lines []string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
			lines = append(lines, line)
		}
	}
	return lines
}

// AwaitMetric waits until a counter has grown by at least growth since the
// call. This is how a test knows providerd looked at something and decided
// to leave it: a pass that changes nothing shows only in its counters.
func (p *Provider) AwaitMetric(t *testing.T, growth float64, name string, labels ...string) {
	t.Helper()
	start := p.Metric(t, name, labels...)
	deadline := time.Now().Add(3 * time.Minute)
	for p.Metric(t, name, labels...) < start+growth {
		if time.Now().After(deadline) {
			t.Fatalf("%s%v did not grow by %v within 3 minutes; providerd reports:\n%s",
				name, labels, growth, strings.Join(p.metricLines(t, name), "\n"))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// SweepOutcome is how providerd labels a finished reconciliation pass.
type SweepOutcome string

const (
	// SweepComplete is a pass that read the chain and every backend and hit
	// no error on any lease.
	SweepComplete SweepOutcome = "success"
	// SweepDegraded is a pass that could not see every backend.
	SweepDegraded SweepOutcome = "degraded"
	// SweepFailed is a pass that ended in an error, as when the chain does
	// not answer. It looked at no backend.
	SweepFailed SweepOutcome = "error"
)

// AwaitSweeps waits until that many reconciliation passes with the given
// outcome have finished since the call. The outcome matters: a failed pass
// inspects nothing, so it is no evidence that providerd left something
// alone. Two passes are needed to be sure one started after the call.
func (p *Provider) AwaitSweeps(t *testing.T, sweeps int, outcome SweepOutcome) {
	t.Helper()
	p.AwaitMetric(t, float64(sweeps), "fred_reconciler_runs_total", `outcome="`+string(outcome)+`"`)
}

// Dispatched counts the requests of one kind that providerd has sent to a
// backend since it started: "provision", "restore", "deprovision", "restart"
// or "update". Unlike the backend's own state, the count also shows a
// request the backend refused.
func (p *Provider) Dispatched(t *testing.T, backend *e2e.Backend, operation string) float64 {
	t.Helper()
	return p.Metric(t, "fred_backend_requests_total", `backend="`+backend.Name+`"`, `operation="`+operation+`"`)
}

// config is providerd's configuration: the chain, the provider's identity
// and key, the backends, and stores under state.
func (p *Provider) config(key Key, state, listenAddr string) map[string]any {
	backends := make([]map[string]any, len(p.Backends))
	for i, backend := range p.Backends {
		backends[i] = map[string]any{
			"name":                 backend.Name,
			"url":                  backend.URL,
			"tls_ca_file":          backend.CAFile,
			"tls_client_cert_file": backend.ClientCertFile,
			"tls_client_key_file":  backend.ClientKeyFile,
			"hmac_secret":          backend.HMACSecret,
			"timeout":              "30s",
			// A backend takes only the SKUs listed for it.
			"skus": []string{backend.SKUs.Stateless, backend.SKUs.Stateful, backend.SKUs.Pinned},
		}
	}
	return map[string]any{
		"production_mode":  false,
		"chain_id":         p.Chain.ID,
		"grpc_endpoint":    p.Chain.GRPC(),
		"websocket_url":    p.Chain.WebSocketURL(),
		"provider_uuid":    p.UUID,
		"provider_address": key.Address,
		"keyring_backend":  "test",
		"keyring_dir":      state,
		"key_name":         key.Name,
		"api_listen_addr":  listenAddr,
		"backends":         backends,
		// The backend calls back on this URL.
		"callback_base_url": "http://" + listenAddr,
		// A missed chain event is repaired by the next pass, and a test that
		// proves a pass left something alone must wait for passes. The
		// production default is five minutes.
		"reconciliation_interval": "5s",
		"token_tracker_db_path":   filepath.Join(state, "tokens.db"),
		"payload_store_db_path":   filepath.Join(state, "payloads.db"),
		"placement_store_db_path": filepath.Join(state, "placements.db"),
		// The node accepts zero fees.
		"gas_limit": 1500000,
		"gas_price": 0,
		"fee_denom": "umfx",
	}
}

// copyExecutable copies a binary to a path the service account can reach.
func copyExecutable(t *testing.T, from, to string) {
	t.Helper()
	source, err := os.Open(from) //nolint:gosec // G304: a binary under test
	require.NoError(t, err)
	defer func() { _ = source.Close() }()
	target, err := os.OpenFile(to, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o700) //nolint:gosec // G302, G304: an executable under t.TempDir
	require.NoError(t, err)
	_, err = io.Copy(target, source)
	require.NoError(t, errors.Join(err, target.Close()))
}

// stderrOf is what a failed command wrote to its standard error.
func stderrOf(err error) string {
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return string(exit.Stderr)
	}
	return ""
}

// createSKU registers a SKU for this provider and returns its UUID.
func (p *Provider) createSKU(name string) string {
	p.t.Helper()
	// Unit 1 is per hour; an hourly price must divide by 3600.
	created := p.Chain.Tx(p.t, providerKey, "sku", "create-sku", p.UUID, name, "1", "3600000"+billingDenom).Event(p.t, "sku_created")
	require.Equal(p.t, name, created["name"])
	require.Equal(p.t, p.UUID, created["provider_uuid"])
	return created["sku_uuid"]
}

// healthy reports whether providerd's API answers.
func (p *Provider) healthy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK
}
