package backend

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"

	"github.com/google/uuid"
)

// MaxPageLimit is the server-side ceiling on a requested /provisions page size.
// A larger requested limit is coerced down to this value (AIP-158 "coerce down
// to the maximum") so a mis-set client limit cannot build an oversized page that
// trips the per-page byte guard and re-aborts the reconcile.
const MaxPageLimit = 5000

// PaginateProvisions returns one keyset page of all, sorted by LeaseUUID
// ascending, containing the entries strictly greater than continueToken. It is a
// thin type-specialization of keysetPage; see that function for the full
// contract (limit semantics, cursor monotonicity, non-nil-empty, aliasing).
func PaginateProvisions(all []ProvisionInfo, continueToken string, limit int) (page []ProvisionInfo, next string) {
	return keysetPage(all, func(p ProvisionInfo) string { return p.LeaseUUID }, continueToken, limit)
}

// keysetPage returns one keyset page of all, sorted ascending by keyOf, holding
// the entries strictly greater than continueToken. It is the shared engine
// behind PaginateProvisions and PaginateRetentions.
//
//   - limit <= 0            -> returns (all, sorted) with next "" (unpaginated passthrough)
//   - limit  > MaxPageLimit -> limit is coerced down to MaxPageLimit
//
// next is keyOf(last returned element) iff a full page was returned AND more
// elements remain after it; otherwise "" (the set is exhausted). next is always
// strictly greater than continueToken, so a client may use a strict-increase loop
// guard without page-boundary false positives. The returned page is always
// non-nil so it serializes as [] rather than null.
//
// Sort a copy, never the caller's slice: the backend.Client interface does not
// guarantee an owned/mutable result, so sorting in place could corrupt shared
// state (the aliasing-can-be-load-bearing hazard). make+copy (not slices.Clone)
// also preserves the non-nil empty-slice guarantee the []-not-null JSON contract
// relies on. The O(N) copy is a constant factor on the O(N log N)-per-page sort;
// the asymptotic win (a per-tick sorted snapshot) is tracked as future work (ENG-381).
//
// Precondition: keyOf yields a unique key with a canonical, consistent string
// order — the same order the client's cursor and any store-level seek use (e.g.
// the retention store's bbolt byte-order cursor). Chain lease UUIDs satisfy this
// (canonical-lowercase UUIDv7 from GenerateUUIDv7), but the version is incidental
// — uniqueness plus canonical ordering is what matters. Keyset paging silently
// skips or duplicates at page boundaries if the sort key is non-unique; if it
// ever becomes composite, switch the cursor to an encoded versioned tuple per
// design spec §4.
func keysetPage[T any](all []T, keyOf func(T) string, continueToken string, limit int) (page []T, next string) {
	sorted := make([]T, len(all))
	copy(sorted, all)
	sort.Slice(sorted, func(i, j int) bool { return keyOf(sorted[i]) < keyOf(sorted[j]) })

	if limit <= 0 {
		return sorted, ""
	}
	if limit > MaxPageLimit {
		limit = MaxPageLimit
	}

	start := sort.Search(len(sorted), func(i int) bool { return keyOf(sorted[i]) > continueToken })
	rest := sorted[start:]
	if len(rest) <= limit {
		return rest, "" // last (or empty) page — exhausted
	}
	page = rest[:limit]
	return page, keyOf(page[len(page)-1])
}

// ParsePageParams extracts keyset pagination params from a paginated list query
// (/provisions, /retentions). It returns a non-nil error (which handlers map to
// HTTP 400) when 'limit' is present but not a non-negative integer, or 'continue'
// is present but not a valid UUID. Absent params yield (0, "", nil) — the
// unpaginated passthrough.
func ParsePageParams(q url.Values) (limit int, continueToken string, err error) {
	if v := q.Get("limit"); v != "" {
		n, perr := strconv.Atoi(v)
		if perr != nil || n < 0 {
			return 0, "", fmt.Errorf("invalid limit %q: must be a non-negative integer", v)
		}
		limit = n
	}
	if v := q.Get("continue"); v != "" {
		u, perr := uuid.Parse(v)
		if perr != nil {
			return 0, "", fmt.Errorf("invalid continue token %q: must be a UUID", v)
		}
		// Canonicalize: uuid.Parse accepts uppercase / {braces} / urn:uuid: /
		// no-hyphen forms, but the cursor is compared lexically against the
		// canonical-lowercase LeaseUUIDs in PaginateProvisions, so a non-canonical
		// token would page incorrectly (duplicate/rewound). u.String() is canonical.
		continueToken = u.String()
	}
	// A cursor without a positive page size is nonsensical: PaginateProvisions
	// treats limit<=0 as an unpaginated passthrough and would silently ignore the
	// cursor and return the full list. Reject it. (The back-compat "no params =>
	// full list" path is unaffected — it carries no continue token.)
	if continueToken != "" && limit <= 0 {
		return 0, "", fmt.Errorf("continue requires a positive limit")
	}
	return limit, continueToken, nil
}
