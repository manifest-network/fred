// Package lifecycle defines the opaque identity carried by observational
// backend callback URLs.
//
// An ID is a canonical UUIDv4 carried by the lifecycle_id query parameter.
// It is deliberately a different Go type from operation.OperationID: an exact
// operation completion and a later lifecycle observation have different
// authority even when their paired wire identities contain the same UUID.
package lifecycle
