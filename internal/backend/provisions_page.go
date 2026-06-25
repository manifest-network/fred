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
// ascending, containing the entries strictly greater than continueToken.
//
//   - limit <= 0           -> returns (all, sorted) with next "" (unpaginated passthrough)
//   - limit  > MaxPageLimit -> limit is coerced down to MaxPageLimit
//
// next is the LeaseUUID of the last returned element iff a full page was returned
// AND more elements remain after it; otherwise "" (the set is exhausted). next is
// always strictly greater than continueToken, so a client may use a strict-increase
// loop guard without page-boundary false positives. The returned page is always
// non-nil so it serializes as [] rather than null.
//
// Precondition: LeaseUUID is a unique total order (lease identity is a UUIDv7).
// Keyset paging silently skips or duplicates at page boundaries if the sort key
// is non-unique; if the sort key ever becomes composite, switch the cursor to an
// encoded versioned tuple per design spec §4.
func PaginateProvisions(all []ProvisionInfo, continueToken string, limit int) (page []ProvisionInfo, next string) {
	// Sort a copy, never the caller's slice. The current backends return a freshly
	// built slice from ListProvisions, but the backend.Client interface does not
	// guarantee an owned/mutable result, so sorting in place could corrupt shared
	// state (the aliasing-can-be-load-bearing hazard). make+copy (not slices.Clone)
	// also preserves the non-nil empty-slice guarantee the []-not-null JSON contract
	// relies on. The O(N) copy is a constant factor on the O(N log N)-per-page sort;
	// the asymptotic win (a per-tick sorted snapshot) is tracked as future work (ENG-381).
	sorted := make([]ProvisionInfo, len(all))
	copy(sorted, all)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].LeaseUUID < sorted[j].LeaseUUID })

	if limit <= 0 {
		return sorted, ""
	}
	if limit > MaxPageLimit {
		limit = MaxPageLimit
	}

	start := sort.Search(len(sorted), func(i int) bool { return sorted[i].LeaseUUID > continueToken })
	rest := sorted[start:]
	if len(rest) <= limit {
		return rest, "" // last (or empty) page — exhausted
	}
	page = rest[:limit]
	return page, page[len(page)-1].LeaseUUID
}

// ParseProvisionsPageParams extracts pagination params from a /provisions query.
// It returns a non-nil error (which handlers map to HTTP 400) when 'limit' is
// present but not a non-negative integer, or 'continue' is present but not a
// valid UUID. Absent params yield (0, "", nil) — the unpaginated passthrough.
func ParseProvisionsPageParams(q url.Values) (limit int, continueToken string, err error) {
	if v := q.Get("limit"); v != "" {
		n, perr := strconv.Atoi(v)
		if perr != nil || n < 0 {
			return 0, "", fmt.Errorf("invalid limit %q: must be a non-negative integer", v)
		}
		limit = n
	}
	if v := q.Get("continue"); v != "" {
		if _, perr := uuid.Parse(v); perr != nil {
			return 0, "", fmt.Errorf("invalid continue token %q: must be a UUID", v)
		}
		continueToken = v
	}
	return limit, continueToken, nil
}
