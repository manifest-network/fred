package callbackwire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type countedEnvelopeReader struct {
	reader io.Reader
	read   int
}

func (r *countedEnvelopeReader) Read(value []byte) (int, error) {
	n, err := r.reader.Read(value)
	r.read += n
	return n, err
}

func TestReadEnvelopeStopsStructuralFloodBeforeBufferingOrDecoding(t *testing.T) {
	var body strings.Builder
	body.WriteByte('{')
	for index := range 100_000 {
		fmt.Fprintf(&body, "\"%d\":0,", index)
	}
	body.WriteString(`"backend_storage_id":"550e8400-e29b-41d4-a716-446655440000"}`)
	payload := []byte(body.String())
	require.Less(t, len(payload), MaxPayloadBytes)

	reader := &countedEnvelopeReader{reader: bytes.NewReader(payload)}
	result, err := ReadEnvelope(reader)
	require.ErrorContains(t, err, "structural budget")
	require.Nil(t, result)
	require.LessOrEqual(t, reader.read, 2048, "reject before buffering the attacker-controlled object")
	_, err = SelectUntrustedStorageRoute(payload)
	require.ErrorContains(t, err, "structural budget", "direct decoder entry must enforce the same envelope")

	allocations := testing.AllocsPerRun(20, func() {
		_, readErr := ReadEnvelope(bytes.NewReader(payload))
		if readErr == nil {
			t.Fatal("structural flood was accepted")
		}
	})
	require.Less(t, allocations, float64(20), "the number of fields must not drive decoder allocations")
}

func TestReadEnvelopePreservesEscapedStringsAndByteLimit(t *testing.T) {
	errorText := strings.Repeat("escaped \"{[,]\\", 5000)
	payload, err := json.Marshal(map[string]string{
		"backend_storage_id": "550e8400-e29b-41d4-a716-446655440000", "error": errorText,
	})
	require.NoError(t, err)
	result, err := ReadEnvelope(bytes.NewReader(payload))
	require.NoError(t, err)
	require.Equal(t, payload, result, "signatures cover the original bytes, including escape sequences")
	_, err = SelectUntrustedStorageRoute(result)
	require.NoError(t, err)

	// A large failure string is still supported; syntax inside strings does not
	// spend the structural budget, including at reader chunk boundaries.
	atLimit := []byte(`{"error":"` + strings.Repeat("x", MaxPayloadBytes-len(`{"error":""}`)) + `"}`)
	result, err = ReadEnvelope(bytes.NewReader(atLimit))
	require.NoError(t, err)
	require.Equal(t, atLimit, result)
	_, err = ReadEnvelope(bytes.NewReader(append(atLimit, ' ')))
	require.ErrorContains(t, err, "byte budget")
}

func TestReadEnvelopeRejectsDeepUnknownValuesBeforeJSONDecoding(t *testing.T) {
	payload := []byte(`{"unknown":` + strings.Repeat("[", 1000) + "0" + strings.Repeat("]", 1000) + `}`)
	_, err := ReadEnvelope(bytes.NewReader(payload))
	require.ErrorContains(t, err, "nesting budget")
	_, err = SelectUntrustedStorageRoute(payload)
	require.ErrorContains(t, err, "nesting budget")
}

func BenchmarkReadEnvelopeStructuralFlood(b *testing.B) {
	var body strings.Builder
	body.WriteByte('{')
	for index := range 100_000 {
		fmt.Fprintf(&body, "\"%d\":0,", index)
	}
	body.WriteString(`"last":0}`)
	payload := []byte(body.String())
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := ReadEnvelope(bytes.NewReader(payload)); err == nil {
			b.Fatal("structural flood was accepted")
		}
	}
}
