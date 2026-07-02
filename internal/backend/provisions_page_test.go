package backend

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func provs(ids ...string) []ProvisionInfo {
	out := make([]ProvisionInfo, 0, len(ids))
	for _, id := range ids {
		out = append(out, ProvisionInfo{LeaseUUID: id})
	}
	return out
}

func ids(ps []ProvisionInfo) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.LeaseUUID)
	}
	return out
}

func TestPaginateProvisions_PassthroughWhenLimitNonPositive(t *testing.T) {
	all := provs("c", "a", "b")
	page, next := PaginateProvisions(all, "", 0)
	assert.Equal(t, []string{"a", "b", "c"}, ids(page), "limit<=0 returns all, sorted")
	assert.Empty(t, next)
}

func TestPaginateProvisions_EmptyInputIsNonNil(t *testing.T) {
	page, next := PaginateProvisions(nil, "", 100)
	assert.NotNil(t, page, "must be non-nil so it serializes as [] not null")
	assert.Len(t, page, 0)
	assert.Empty(t, next)
}

func TestPaginateProvisions_FirstPageAndContinue(t *testing.T) {
	all := provs("a", "b", "c", "d", "e")
	page, next := PaginateProvisions(all, "", 2)
	assert.Equal(t, []string{"a", "b"}, ids(page))
	assert.Equal(t, "b", next, "continue is last UUID of a full page with more remaining")
}

func TestPaginateProvisions_ResumeFromContinue(t *testing.T) {
	all := provs("a", "b", "c", "d", "e")
	page, next := PaginateProvisions(all, "b", 2)
	assert.Equal(t, []string{"c", "d"}, ids(page))
	assert.Equal(t, "d", next)
}

func TestPaginateProvisions_LastPageHasEmptyContinue(t *testing.T) {
	all := provs("a", "b", "c", "d", "e")
	page, next := PaginateProvisions(all, "d", 2)
	assert.Equal(t, []string{"e"}, ids(page))
	assert.Empty(t, next, "partial last page exhausts the set")
}

func TestPaginateProvisions_ExactMultipleBoundaryExhausts(t *testing.T) {
	all := provs("a", "b", "c", "d")
	page, next := PaginateProvisions(all, "b", 2)
	assert.Equal(t, []string{"c", "d"}, ids(page))
	assert.Empty(t, next, "a full page that consumes the remainder must signal exhaustion")
}

func TestPaginateProvisions_StaleCursorSeeksToNextGreater(t *testing.T) {
	// "bb" was deleted; resume returns the next-greater UUIDs, not an error.
	all := provs("a", "b", "c", "d")
	page, next := PaginateProvisions(all, "bb", 10)
	assert.Equal(t, []string{"c", "d"}, ids(page))
	assert.Empty(t, next)
}

func TestPaginateProvisions_CursorPastEndYieldsEmptyExhausted(t *testing.T) {
	all := provs("a", "b", "c")
	page, next := PaginateProvisions(all, "z", 10)
	assert.NotNil(t, page)
	assert.Len(t, page, 0, "never-resumes-past-end: empty page")
	assert.Empty(t, next, "empty continue = exhausted, not 'more available'")
}

func TestPaginateProvisions_NextStrictlyGreaterThanContinue(t *testing.T) {
	all := provs("a", "b", "c", "d", "e")
	cont := ""
	for i := 0; i < 10; i++ {
		page, next := PaginateProvisions(all, cont, 2)
		if next == "" {
			assert.NotEmpty(t, page, "non-final iterations return items")
			break
		}
		assert.Greater(t, next, cont, "monotonicity: next must strictly exceed the cursor sent")
		cont = next
	}
}

func TestPaginateProvisions_ClampsOversizedLimit(t *testing.T) {
	all := make([]ProvisionInfo, MaxPageLimit+10)
	for i := range all {
		// zero-padded so lexical order == numeric order
		all[i] = ProvisionInfo{LeaseUUID: padID(i)}
	}
	page, next := PaginateProvisions(all, "", MaxPageLimit*1000)
	assert.Len(t, page, MaxPageLimit, "an oversized limit is coerced down to MaxPageLimit")
	assert.NotEmpty(t, next)
}

// padID renders i as a fixed-width zero-padded decimal so lexical order equals
// numeric order (the keyset sort compares LeaseUUID strings). %08d carries the
// width correctly for any non-negative i; the hand-rolled byte loop alternative
// silently emits "00000000" for i<0 and truncates for i>=1e8 — avoid it.
func padID(i int) string {
	return fmt.Sprintf("%08d", i)
}

func TestParsePageParams(t *testing.T) {
	validUUID := "11111111-1111-1111-1111-111111111111"

	t.Run("absent params", func(t *testing.T) {
		limit, cont, err := ParsePageParams(url.Values{})
		require.NoError(t, err)
		assert.Equal(t, 0, limit)
		assert.Empty(t, cont)
	})
	t.Run("valid limit and continue", func(t *testing.T) {
		limit, cont, err := ParsePageParams(url.Values{"limit": {"500"}, "continue": {validUUID}})
		require.NoError(t, err)
		assert.Equal(t, 500, limit)
		assert.Equal(t, validUUID, cont)
	})
	t.Run("non-integer limit is rejected", func(t *testing.T) {
		_, _, err := ParsePageParams(url.Values{"limit": {"abc"}})
		require.Error(t, err)
	})
	t.Run("negative limit is rejected", func(t *testing.T) {
		_, _, err := ParsePageParams(url.Values{"limit": {"-1"}})
		require.Error(t, err)
	})
	t.Run("non-UUID continue is rejected", func(t *testing.T) {
		_, _, err := ParsePageParams(url.Values{"continue": {"not-a-uuid"}})
		require.Error(t, err)
	})
	t.Run("continue without limit is rejected", func(t *testing.T) {
		_, _, err := ParsePageParams(url.Values{"continue": {validUUID}})
		require.Error(t, err, "a cursor with no positive limit must 400, not silently return the full list")
	})
	t.Run("continue with limit=0 is rejected", func(t *testing.T) {
		_, _, err := ParsePageParams(url.Values{"continue": {validUUID}, "limit": {"0"}})
		require.Error(t, err)
	})
	t.Run("uppercase continue is canonicalized to lowercase", func(t *testing.T) {
		// uuid.Parse accepts uppercase; the cursor must be canonical lowercase so
		// lexical comparison matches the stored LeaseUUIDs (else duplicate/rewound pages).
		_, cont, err := ParsePageParams(url.Values{"limit": {"100"}, "continue": {"ABCDEF01-2345-6789-ABCD-EF0123456789"}})
		require.NoError(t, err)
		assert.Equal(t, "abcdef01-2345-6789-abcd-ef0123456789", cont)
	})
	t.Run("braced continue is canonicalized", func(t *testing.T) {
		_, cont, err := ParsePageParams(url.Values{"limit": {"100"}, "continue": {"{11111111-1111-1111-1111-111111111111}"}})
		require.NoError(t, err)
		assert.Equal(t, "11111111-1111-1111-1111-111111111111", cont)
	})
}
