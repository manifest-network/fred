//go:build integration

package chain

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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

// Provider is a whole provider: a chain, one docker-backend, and the providerd
// that joins them. Each test starts its own, so tests share only the Docker
// daemon.
type Provider struct {
	Chain   *Chain
	Backend *e2e.Backend
	// URL is the base URL of providerd's API.
	URL string
	// UUID is the provider's UUID on the chain.
	UUID string
	// SKUs are the chain's SKUs for the backend's two profiles.
	SKUs e2e.SKUs

	t       *testing.T
	tenants int
}

// StartProvider starts a chain, registers a provider and its SKUs on it,
// starts a backend serving those SKUs, and bootstraps providerd as an
// operator does: create the placement database against the empty backend,
// then start the daemon. It skips the test when the host cannot run the
// suite. Cleanup stops providerd first, then the backend, then the chain.
func StartProvider(t *testing.T) *Provider {
	t.Helper()
	if os.Getuid() != 0 {
		t.Skip("e2e needs root: it mounts an XFS loopback and runs the binaries as a service account")
	}
	binDir := os.Getenv(ProviderBinDirEnv)
	if binDir == "" {
		t.Skipf("%s is not set; run `make test-e2e`", ProviderBinDirEnv)
	}
	for _, name := range []string{"providerd", "placement-preflight"} {
		_, err := os.Stat(filepath.Join(binDir, name))
		require.NoError(t, err, "%s must hold a %s binary", ProviderBinDirEnv, name)
	}

	p := &Provider{t: t}
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

	p.Backend = e2e.StartBackendWithSKUs(t, p.SKUs)

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

	process := e2e.NewProcess(t, "providerd-"+p.Backend.Name)
	command := func(ctx context.Context, binary string, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, filepath.Join(state, binary), args...) //nolint:gosec // G204: a binary under test and fixture-built arguments
		cmd.Env = process.Env()
		e2e.AsServiceAccount(cmd)
		return cmd
	}

	// providerd never creates its placement database. A new provider's
	// operator does, once, with the backend running and still empty: the tool
	// prints an acknowledgement bound to this provider and its backends, and
	// creates the database only when given it back.
	roster := `["` + p.Backend.Name + `"]`
	confirmation, err := command(t.Context(), "placement-preflight", "-config", configPath,
		"-print-fresh-confirmation", "-expected-backends", roster).Output()
	require.NoError(t, err, "print the fresh-placement confirmation: %s", stderrOf(err))
	out, err := command(t.Context(), "placement-preflight", "-config", configPath,
		"-initialize-fresh", "-expected-backends", roster,
		"-confirm-insecure-chain", insecureChainConfirmation,
		"-confirm-quiesced", strings.TrimSpace(string(confirmation))).CombinedOutput()
	require.NoError(t, err, "create the placement database: %s", out)

	t.Cleanup(func() {
		process.Stop()
		process.LogOnFailure()
	})
	process.Start(
		func(ctx context.Context) *exec.Cmd { return command(ctx, "providerd", "--config", configPath) },
		func() bool { return p.healthy() },
	)
	return p
}

// config is providerd's configuration: the chain, the provider's identity
// and key, the one backend, and stores under state.
func (p *Provider) config(key Key, state, listenAddr string) map[string]any {
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
		"backends": []map[string]any{{
			"name":                 p.Backend.Name,
			"url":                  p.Backend.URL,
			"tls_ca_file":          p.Backend.CAFile,
			"tls_client_cert_file": p.Backend.ClientCertFile,
			"tls_client_key_file":  p.Backend.ClientKeyFile,
			"hmac_secret":          p.Backend.HMACSecret,
			"timeout":              "30s",
			"default":              true,
		}},
		// The backend calls back on this URL.
		"callback_base_url": "http://" + listenAddr,
		// A missed chain event is repaired by the next pass. The production
		// default is five minutes, longer than a test should wait.
		"reconciliation_interval": "10s",
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
