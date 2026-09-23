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
