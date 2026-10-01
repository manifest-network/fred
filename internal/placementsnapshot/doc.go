// Package placementsnapshot takes periodic online snapshots of providerd's
// placements.db and payloads.db into one operator-chosen directory, so losing
// the host costs at most one snapshot interval of placement history instead of
// everything since the last manual backup.
//
// Each snapshot set is one consistent cut (placement.CaptureConsistentCut):
// both read transactions begin while the placement write gate excludes
// writers, so the pair is a state a crash could have left, which recovery
// already handles. A set is three files,
//
//	fred-snapshot-<provider>-<UTC yyyymmddThhmmssZ>-<8 hex>.placements.db
//	fred-snapshot-<provider>-<UTC yyyymmddThhmmssZ>-<8 hex>.payloads.db
//	fred-snapshot-<provider>-<UTC yyyymmddThhmmssZ>-<8 hex>.manifest.json
//
// and is complete only once its manifest exists. The manifest is published
// last and can be built only from a verifiedSet: the receipt of a stream that
// copied those exact bytes, plus a re-read that matched the receipt and passed
// bbolt's consistency check.
//
// Pruning keeps the newest retained complete sets and never deletes the set
// the same pass published. It considers only names carrying this provider's
// UUID, deletes only regular files owned by the service user, never unlinks a
// live database's inode, and deletes a set's manifest before its data. Any
// doubt keeps the file and counts
// fred_placement_snapshot_prune_failures_total{reason}.
package placementsnapshot
