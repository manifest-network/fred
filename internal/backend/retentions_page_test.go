package backend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func rets(ids ...string) []RetainedLease {
	out := make([]RetainedLease, 0, len(ids))
	for _, id := range ids {
		out = append(out, RetainedLease{LeaseUUID: id})
	}
	return out
}

func retIDs(rs []RetainedLease) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.LeaseUUID)
	}
	return out
}

func TestPaginateRetentions_PassthroughWhenLimitNonPositive(t *testing.T) {
	all := rets("c", "a", "b")
	page, next := PaginateRetentions(all, "", 0)
	assert.Equal(t, []string{"a", "b", "c"}, retIDs(page), "limit<=0 returns all, sorted")
	assert.Empty(t, next)
}

func TestPaginateRetentions_EmptyInputIsNonNil(t *testing.T) {
	page, next := PaginateRetentions(nil, "", 100)
	assert.NotNil(t, page, "must be non-nil so it serializes as [] not null")
	assert.Len(t, page, 0)
	assert.Empty(t, next)
}

func TestPaginateRetentions_FirstPageAndContinue(t *testing.T) {
	all := rets("a", "b", "c", "d", "e")
	page, next := PaginateRetentions(all, "", 2)
	assert.Equal(t, []string{"a", "b"}, retIDs(page))
	assert.Equal(t, "b", next, "continue is last UUID of a full page with more remaining")
}

func TestPaginateRetentions_ResumeFromContinue(t *testing.T) {
	all := rets("a", "b", "c", "d", "e")
	page, next := PaginateRetentions(all, "b", 2)
	assert.Equal(t, []string{"c", "d"}, retIDs(page))
	assert.Equal(t, "d", next)
}

func TestPaginateRetentions_LastPageHasEmptyContinue(t *testing.T) {
	all := rets("a", "b", "c", "d", "e")
	page, next := PaginateRetentions(all, "d", 2)
	assert.Equal(t, []string{"e"}, retIDs(page))
	assert.Empty(t, next, "partial last page exhausts the set")
}

func TestPaginateRetentions_ExactMultipleBoundaryExhausts(t *testing.T) {
	all := rets("a", "b", "c", "d")
	page, next := PaginateRetentions(all, "b", 2)
	assert.Equal(t, []string{"c", "d"}, retIDs(page))
	assert.Empty(t, next, "a full page that consumes the remainder must signal exhaustion")
}

func TestPaginateRetentions_StaleCursorSeeksToNextGreater(t *testing.T) {
	all := rets("a", "b", "c", "d")
	page, next := PaginateRetentions(all, "bb", 10)
	assert.Equal(t, []string{"c", "d"}, retIDs(page))
	assert.Empty(t, next)
}

func TestPaginateRetentions_CursorPastEndYieldsEmptyExhausted(t *testing.T) {
	all := rets("a", "b", "c")
	page, next := PaginateRetentions(all, "z", 10)
	assert.NotNil(t, page)
	assert.Len(t, page, 0, "never-resumes-past-end: empty page")
	assert.Empty(t, next, "empty continue = exhausted, not 'more available'")
}

func TestPaginateRetentions_ClampsOversizedLimit(t *testing.T) {
	all := make([]RetainedLease, MaxPageLimit+10)
	for i := range all {
		all[i] = RetainedLease{LeaseUUID: padID(i)}
	}
	page, next := PaginateRetentions(all, "", MaxPageLimit*1000)
	assert.Len(t, page, MaxPageLimit, "an oversized limit is coerced down to MaxPageLimit")
	assert.NotEmpty(t, next)
}
