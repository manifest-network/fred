package manifest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoredPayloadSeparatesAdmissionPolicy(t *testing.T) {
	for _, payload := range []string{
		`{"image":"nginx:1","labels":{"com.docker.compose.project":"legacy"}}`,
		`{"services":{"web":{"image":"nginx:1","labels":{"traefiK.enable":"true"}}}}`,
		`{"image":"nginx:1","user":"1:2:3"}`,
		`{"image":"nginx:1","user":"user\fgroup"}`,
		`{"image":"nginx:1","user":"user\u000bgroup"}`,
	} {
		t.Run(payload, func(t *testing.T) {
			_, err := ParsePayload([]byte(payload))
			require.Error(t, err, "tenant admission must remain strict")
			_, err = ParseStoredPayload([]byte(payload))
			require.NoError(t, err, "stored history must not reapply current admission")
		})
	}
}

func TestStoredPayloadRejectsCorruptRecoveryTopology(t *testing.T) {
	for _, payload := range []string{
		``, `{`, `null`, `{"services":{}}`, `{"services":{"web":null}}`,
		`{"services":{"../web":{"image":"nginx"}}}`,
		`{"services":{"web":{}}}`, `{"image":"nginx","unknown":true}`,
		`{"image":"nginx","depends_on":{"peer":{"condition":"service_started"}}}`,
		`{"services":{"web":{"image":"nginx","depends_on":{"missing":{"condition":"service_started"}}}}}`,
		`{"services":{"web":{"image":"nginx","depends_on":{"web":{"condition":"service_started"}}}}}`,
	} {
		t.Run(payload, func(t *testing.T) {
			_, err := ParseStoredPayload([]byte(payload))
			require.Error(t, err)
		})
	}
}

func TestStoredPayloadPreservesPortExecutionEncoding(t *testing.T) {
	for _, ports := range []string{
		`{"80":{}}`, `{"80/tcp/extra":{}}`, `{"0/tcp":{}}`,
		`{"65536/tcp":{}}`, `{"bad/tcp":{}}`, `{"80/sctp":{}}`,
		`{"80/tcp":{"host_port":-1}}`, `{"80/tcp":{"host_port":65536}}`,
	} {
		t.Run(ports, func(t *testing.T) {
			payload := []byte(`{"services":{"app":{"image":"nginx:1","ports":` + ports + `}}}`)
			_, err := ParseStoredPayload(payload)
			require.ErrorContains(t, err, "invalid port")
		})
	}
	payload := []byte(`{"services":{"app":{"image":"nginx:1","ports":{"80/tcp":{"host_port":8080},"53/udp":{}},"labels":{"com.docker.compose.project":"legacy"},"user":"1:2:3"}}}`)
	_, err := ParseStoredPayload(payload)
	require.NoError(t, err, "stable port encoding must not reapply evolving label or USER policy")
	_, err = ParsePayload(payload)
	require.Error(t, err)
}
