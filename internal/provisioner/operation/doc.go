// Package operation defines the identities and capabilities used to coordinate
// provisioner lifecycle operations.
//
// OperationID is the only value that crosses the process boundary. Its text
// representation is a canonical UUIDv4 carried by the operation_id callback
// query parameter. The other types are opaque, process-local capabilities:
// callers can pass them back to the registry that issued them but cannot
// manufacture a valid value themselves.
//
// All exported types have invalid zero values. In particular, a registry must
// explicitly issue a TrackerSnapshot even when its mutation revision is zero.
package operation
