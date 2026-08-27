package operation

import (
	"encoding"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const canonicalTestOperationID = "123e4567-e89b-42d3-a456-426614174000"

func mustTestOperationID(t testing.TB, text string) OperationID {
	t.Helper()
	id, err := ParseID(text)
	require.NoError(t, err)
	return id
}

func TestOperationIDZeroValueIsInvalid(t *testing.T) {
	var id OperationID

	assert.False(t, id.Valid())
	assert.Equal(t, "invalid", id.String())
	text, err := id.MarshalText()
	assert.ErrorIs(t, err, ErrInvalidID)
	assert.Nil(t, text)
	assert.Equal(t, OperationID{}, newOperationID(uuid.Nil))
}

func TestParseIDAcceptsOnlyCanonicalUUIDv4(t *testing.T) {
	id, err := ParseID(canonicalTestOperationID)
	require.NoError(t, err)
	require.True(t, id.Valid())
	assert.Equal(t, canonicalTestOperationID, id.String())

	text, err := id.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, canonicalTestOperationID, string(text))
}

func TestParseIDRejectsNonCanonicalOrNonV4Values(t *testing.T) {
	tests := []struct {
		name string
		wire string
	}{
		{name: "empty", wire: ""},
		{name: "nil UUID", wire: uuid.Nil.String()},
		{name: "uppercase", wire: strings.ToUpper(canonicalTestOperationID)},
		{name: "compact", wire: strings.ReplaceAll(canonicalTestOperationID, "-", "")},
		{name: "braced", wire: "{" + canonicalTestOperationID + "}"},
		{name: "urn", wire: "urn:uuid:" + canonicalTestOperationID},
		{name: "version one", wire: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{name: "version five", wire: "74738ff5-5367-5958-9aee-98fffdcd1876"},
		{name: "non-RFC variant", wire: "123e4567-e89b-42d3-2456-426614174000"},
		{name: "malformed", wire: "not-a-uuid"},
		{name: "leading whitespace", wire: " " + canonicalTestOperationID},
		{name: "trailing whitespace", wire: canonicalTestOperationID + " "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := ParseID(tt.wire)
			assert.ErrorIs(t, err, ErrInvalidID)
			assert.False(t, id.Valid())
		})
	}
}

func TestOperationIDImplementsTextMarshaler(t *testing.T) {
	var marshaler encoding.TextMarshaler = mustTestOperationID(t, canonicalTestOperationID)
	text, err := marshaler.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, canonicalTestOperationID, string(text))
}

func TestParseQuery(t *testing.T) {
	t.Run("absent supports v0.13 callback", func(t *testing.T) {
		id, present, err := ParseQuery(url.Values{"unrelated": {"value"}})
		require.NoError(t, err)
		assert.False(t, present)
		assert.False(t, id.Valid())
	})

	t.Run("present and valid", func(t *testing.T) {
		id, present, err := ParseQuery(url.Values{QueryParameter: {canonicalTestOperationID}})
		require.NoError(t, err)
		require.True(t, present)
		assert.Equal(t, canonicalTestOperationID, id.String())
	})

	invalid := []struct {
		name   string
		values []string
		want   error
	}{
		{name: "empty value", values: []string{""}, want: ErrInvalidID},
		{name: "malformed", values: []string{"abc"}, want: ErrInvalidID},
		{name: "non-v4", values: []string{"6ba7b810-9dad-11d1-80b4-00c04fd430c8"}, want: ErrInvalidID},
		{name: "no values", values: nil, want: ErrAmbiguousQuery},
		{name: "duplicate different values", values: []string{canonicalTestOperationID, "d9428888-122b-41e1-b85c-61c67afba0c6"}, want: ErrAmbiguousQuery},
		{name: "duplicate identical values", values: []string{canonicalTestOperationID, canonicalTestOperationID}, want: ErrAmbiguousQuery},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			id, present, err := ParseQuery(url.Values{QueryParameter: tt.values})
			assert.ErrorIs(t, err, tt.want)
			assert.True(t, present)
			assert.False(t, id.Valid())
		})
	}
}

func TestSetQuery(t *testing.T) {
	t.Run("writes canonical value and preserves unrelated values", func(t *testing.T) {
		values := url.Values{
			QueryParameter: {"stale", "duplicate"},
			"unrelated":    {"value"},
		}
		err := SetQuery(values, mustTestOperationID(t, canonicalTestOperationID))
		require.NoError(t, err)
		assert.Equal(t, []string{canonicalTestOperationID}, values[QueryParameter])
		assert.Equal(t, []string{"value"}, values["unrelated"])
	})

	t.Run("invalid ID does not mutate destination", func(t *testing.T) {
		values := url.Values{QueryParameter: {"existing"}}
		before := values.Encode()
		err := SetQuery(values, OperationID{})
		assert.ErrorIs(t, err, ErrInvalidID)
		assert.Equal(t, before, values.Encode())
	})

	t.Run("nil destination returns error", func(t *testing.T) {
		err := SetQuery(nil, mustTestOperationID(t, canonicalTestOperationID))
		assert.ErrorIs(t, err, ErrNilQuery)
	})
}

func TestOperationIDComparable(t *testing.T) {
	one := mustTestOperationID(t, canonicalTestOperationID)
	alsoOne, err := ParseID(canonicalTestOperationID)
	require.NoError(t, err)
	two := mustTestOperationID(t, "d9428888-122b-41e1-b85c-61c67afba0c6")

	set := map[OperationID]struct{}{one: {}}
	_, hasOne := set[alsoOne]
	_, hasTwo := set[two]
	assert.True(t, hasOne)
	assert.False(t, hasTwo)
}

func FuzzOperationIDTextRoundTrip(f *testing.F) {
	for _, seed := range []string{
		canonicalTestOperationID,
		"d9428888-122b-41e1-b85c-61c67afba0c6",
		"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"not-a-uuid",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, text string) {
		id, err := ParseID(text)
		if err != nil {
			return
		}
		encoded, err := id.MarshalText()
		require.NoError(t, err)
		assert.Equal(t, text, string(encoded))
	})
}

func TestOperationIDErrorsAreDistinct(t *testing.T) {
	assert.False(t, errors.Is(ErrInvalidID, ErrAmbiguousQuery))
	assert.False(t, errors.Is(ErrInvalidID, ErrNilQuery))
}
