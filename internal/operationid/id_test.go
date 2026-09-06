package operationid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const canonicalTestID = "550e8400-e29b-41d4-a716-446655440000"

func TestIDConstructionAndRoundTrip(t *testing.T) {
	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	require.True(t, id.Valid())
	assert.False(t, id.IsZero())
	assert.Equal(t, canonicalTestID, id.String())

	text, err := id.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, canonicalTestID, string(text))

	encoded, err := json.Marshal(id)
	require.NoError(t, err)
	assert.JSONEq(t, `"`+canonicalTestID+`"`, string(encoded))

	var decoded ID
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, id, decoded)

	random, err := New()
	require.NoError(t, err)
	assert.True(t, random.Valid())
}

func TestDiagnosticFingerprintDoesNotExposeCapability(t *testing.T) {
	id := mustParse(t, canonicalTestID)
	fingerprint := id.Fingerprint()
	assert.Equal(t, "op_2d94adb4b7737fbfc823eed3", fingerprint)
	assert.NotContains(t, fingerprint, canonicalTestID)
	assert.Equal(t, "invalid", (ID{}).Fingerprint())

	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	logger.Info("operation", "operation_fingerprint", id)
	assert.Contains(t, output.String(), "operation_fingerprint="+fingerprint)
	assert.NotContains(t, output.String(), canonicalTestID)

	for _, formatted := range []string{
		fmt.Sprint(id),
		fmt.Sprintf("%s", id),
		fmt.Sprintf("%q", id),
		fmt.Sprintf("%v", id),
		fmt.Sprintf("%#v", id),
		fmt.Errorf("operation %s", id).Error(),
	} {
		assert.Contains(t, formatted, fingerprint)
		assert.NotContains(t, formatted, canonicalTestID)
	}
}

func TestZeroIsInvalidAndOmittable(t *testing.T) {
	var id ID
	assert.False(t, id.Valid())
	assert.True(t, id.IsZero())
	assert.Empty(t, id.String())
	_, err := id.MarshalText()
	assert.ErrorIs(t, err, ErrInvalid)
	_, err = json.Marshal(id)
	assert.ErrorIs(t, err, ErrInvalid)

	type optional struct {
		ID ID `json:"id,omitzero"`
	}
	encoded, err := json.Marshal(optional{})
	require.NoError(t, err)
	assert.JSONEq(t, `{}`, string(encoded))
	encoded, err = json.Marshal(optional{ID: mustParse(t, canonicalTestID)})
	require.NoError(t, err)
	assert.JSONEq(t, `{"id":"`+canonicalTestID+`"}`, string(encoded))
}

func TestParseRejectsNonCanonicalOrNonV4Values(t *testing.T) {
	for _, value := range []string{
		"", strings.ToUpper(canonicalTestID),
		strings.ReplaceAll(canonicalTestID, "-", ""),
		"00000000-0000-0000-0000-000000000000",
		"550e8400-e29b-11d4-a716-446655440000",
		"550e8400-e29b-41d4-2716-446655440000",
	} {
		t.Run(value, func(t *testing.T) {
			_, err := Parse(value)
			assert.ErrorIs(t, err, ErrInvalid)
		})
	}
}

func TestInvalidDecodeDoesNotMutateReceiver(t *testing.T) {
	want := mustParse(t, canonicalTestID)
	for _, encoded := range []string{
		`null`, `123`, `"` + strings.ToUpper(canonicalTestID) + `"`,
	} {
		id := want
		assert.ErrorIs(t, json.Unmarshal([]byte(encoded), &id), ErrInvalid)
		assert.Equal(t, want, id)
	}
	id := want
	assert.Error(t, json.Unmarshal([]byte(`"`+canonicalTestID+`" true`), &id))
	assert.Equal(t, want, id)
	id = want
	assert.ErrorIs(t, id.UnmarshalText([]byte("invalid")), ErrInvalid)
	assert.Equal(t, want, id)
}

func mustParse(t testing.TB, text string) ID {
	t.Helper()
	id, err := Parse(text)
	require.NoError(t, err)
	return id
}
