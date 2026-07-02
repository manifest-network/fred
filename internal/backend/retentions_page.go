package backend

// PaginateRetentions returns one keyset page of all, sorted by LeaseUUID
// ascending, containing the entries strictly greater than continueToken. It is
// the /retentions sibling of PaginateProvisions; both delegate to keysetPage, so
// they share identical cursor semantics (see keysetPage for the full contract).
func PaginateRetentions(all []RetainedLease, continueToken string, limit int) (page []RetainedLease, next string) {
	return keysetPage(all, func(r RetainedLease) string { return r.LeaseUUID }, continueToken, limit)
}
