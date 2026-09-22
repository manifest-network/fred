// Package operation defines the identities and capabilities used to coordinate
// provisioner lifecycle operations.
//
// OperationID crosses the process boundary for an exact operation completion.
// Its text representation is a canonical UUIDv4 carried by the operation_id
// callback query parameter. Observational callbacks use the distinct typed ID
// in package lifecycle. The other operation types are opaque, process-local
// capabilities: callers can pass them back to the registry that issued them
// but cannot manufacture a valid value themselves.
//
// All exported types have invalid zero values. In particular, a registry must
// explicitly issue a TrackerSnapshot even when its mutation revision is zero.
package operation
