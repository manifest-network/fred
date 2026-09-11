package config

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDevInitProviderConfigTemplateIsValid(t *testing.T) {
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "PROVIDER_") {
			t.Setenv(name, "")
		}
	}

	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "dev-init.sh"))
	require.NoError(t, err)

	const heredocStart = `cat >| "$CONFIG_DOCKER_TMP" <<YAML`
	start := strings.Index(string(script), heredocStart)
	require.NotEqual(t, -1, start, "provider config template not found")

	template := string(script)[start+len(heredocStart):]
	end := strings.Index(template, "\nYAML\n")
	require.NotEqual(t, -1, end, "provider config template terminator not found")
	template = template[:end]

	callbackSecret := strings.Repeat("a", 32) + `"\suffix`
	callbackSecretJSON, err := json.Marshal(callbackSecret)
	require.NoError(t, err)
	values := map[string]string{
		"CALLBACK_BASE_URL_JSON":  `"https://localhost:8080"`,
		"CALLBACK_SECRET_JSON":    string(callbackSecretJSON),
		"CERT_FILE_JSON":          `"/work/fred-cert.pem"`,
		"CHAIN_ID_JSON":           `"manifest-dev"`,
		"DOCKER_BACKEND_URL_JSON": `"http://localhost:9001"`,
		"GRPC_ENDPOINT_JSON":      `"localhost:9090"`,
		"KEYRING_BACKEND_JSON":    `"test"`,
		"KEYRING_DIR_JSON":        `"/work/manifest/"`,
		"KEY_FILE_JSON":           `"/work/fred-key.pem"`,
		"KEY_NAME_JSON":           `"provider"`,
		"PAYLOAD_DB_PATH_JSON":    `"/work/fred/payloads.db"`,
		"PLACEMENT_DB_PATH_JSON":  `"/work/fred/placements.db"`,
		"PROVIDER_ADDRESS_JSON":   `"manifest1provider"`,
		"PROVIDER_UUID_JSON":      `"550e8400-e29b-41d4-a716-446655440000"`,
		"TOKEN_DB_PATH_JSON":      `"/work/fred/tokens.db"`,
		"WEBSOCKET_URL_JSON":      `"ws://localhost:26657/websocket"`,
	}
	var unknown []string
	generated := os.Expand(template, func(name string) string {
		value, ok := values[name]
		if !ok {
			unknown = append(unknown, name)
		}
		return value
	})
	require.Empty(t, unknown, "test fixture must expand every template variable")

	configPath := filepath.Join(t.TempDir(), "config.docker.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(generated), 0o600))

	cfg, err := Load(configPath)
	require.NoError(t, err, "dev-init must generate a config accepted by providerd")
	require.Equal(t, "/work/fred/placements.db", cfg.PlacementStoreDBPath)
	secret, err := cfg.ResolveBackendHMACSecret("docker")
	require.NoError(t, err)
	require.Equal(t, callbackSecret, string(secret),
		"JSON-compatible YAML quoting must preserve arbitrary HMAC secret bytes")
}

func TestDevInitPrintsSafeFreshPlacementBootstrapOrder(t *testing.T) {
	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "dev-init.sh"))
	require.NoError(t, err)
	text := string(script)

	seal := strings.Index(text, "-initialize-storage-identity new")
	initialize := strings.Index(text, "-initialize-fresh")
	verdict := strings.Index(text, "INITIALIZED_FOR_CUTOVER")
	backend := strings.Index(text, "./build/docker-backend -config docker-backend.yaml\"")
	provider := strings.Index(text, "./build/providerd --config config.docker.yaml")
	require.NotEqual(t, -1, seal)
	require.NotEqual(t, -1, backend)
	require.NotEqual(t, -1, initialize)
	require.NotEqual(t, -1, verdict)
	require.NotEqual(t, -1, provider)
	require.Less(t, seal, backend, "the one-shot seal must finish before normal backend startup")
	require.Less(t, backend, initialize, "the empty backend must be running before fleet proof")
	require.Less(t, seal, initialize, "backend lineage must be sealed before fleet proof")
	require.Less(t, initialize, verdict, "the script must require the initializer verdict")
	require.Less(t, verdict, provider, "providerd must be the final startup step")
	require.Contains(t, text,
		"I ACCEPT UNAUTHENTICATED CHAIN EVIDENCE FOR LOCAL DEVELOPMENT",
		"the plaintext dev chain requires an explicit non-production attestation")
	require.Contains(t, text, `REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"`)
	require.Contains(t, text, "-print-fresh-confirmation",
		"the placement package must render the exact encoding/json acknowledgement")
	require.Contains(t, text, `volume_mount_path: $VOLUME_MOUNT_PATH_JSON`)
	require.Contains(t, text, "umask 077", "generated HMAC-bearing config must be private")
	require.Contains(t, text, `publish_private_no_replace "$DOCKER_BACKEND_TMP" "$DOCKER_BACKEND_FILE"`)
	require.Contains(t, text, `publish_private_no_replace "$CONFIG_DOCKER_TMP" "$CONFIG_DOCKER_FILE"`)
	require.Contains(t, text, `ln -- "$source" "$target"`,
		"publication must use an atomic no-replace primitive")
	require.Contains(t, text, `[[ -e "$existing_path" || -L "$existing_path" ]]`,
		"the early guard must recognize dangling symlinks")
	require.Contains(t, text, `findmnt -n -o TARGET -T "$VOLUME_DATA_PATH"`)
	require.Contains(t, text, `"$VOLUME_MOUNT_PATH" != "$DETECTED_VOLUME_MOUNT_PATH"`,
		"an explicit mount must be checked against the active mount, not merely accepted")
}

func TestDevInitCallbackSecretValidationCountsBytes(t *testing.T) {
	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "dev-init.sh"))
	require.NoError(t, err)

	const marker = "validate_callback_secret() {"
	functionStart := strings.Index(string(script), marker)
	require.NotEqual(t, -1, functionStart, "callback-secret validator not found")
	functionText := string(script)[functionStart:]
	functionEnd := strings.Index(functionText, "\n}\n")
	require.NotEqual(t, -1, functionEnd, "callback-secret validator terminator not found")
	functionText = functionText[:functionEnd+2]

	harness := `set -euo pipefail
die() { printf '%s\n' "$*" >&2; exit 1; }
` + functionText + `
CALLBACK_SECRET="$1"
validate_callback_secret
`

	for _, test := range []struct {
		name    string
		secret  string
		wantErr bool
	}{
		{name: "31 ASCII bytes are rejected", secret: strings.Repeat("a", 31), wantErr: true},
		{name: "32 ASCII bytes are accepted", secret: strings.Repeat("a", 32)},
		{name: "16 two-byte runes are accepted", secret: strings.Repeat("é", 16)},
	} {
		t.Run(test.name, func(t *testing.T) {
			//nolint:gosec // The executable and harness are repository-owned test fixtures; the secret is one positional argument.
			command := exec.Command("bash", "-c", harness, "dev-init-secret-test", test.secret)
			output, commandErr := command.CombinedOutput()
			if test.wantErr {
				require.Error(t, commandErr)
				require.Contains(t, string(output), "CALLBACK_SECRET must be at least 32 bytes")
				return
			}
			require.NoError(t, commandErr, "validator output: %s", output)
		})
	}
}
