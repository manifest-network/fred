// Package imageexec admits immutable container images and binds their use to
// typed Docker and Compose execution sinks. Admission resolves a runnable leaf,
// validates its metadata, and verifies its independent local image-store record
// before returning an Image. Images and prepared projects cannot be reconstructed
// from IDs or labels. Their zero values are inert.
// Runtime construction probes and negotiates the descriptor-capable Docker API
// before exposing admission or execution capabilities.
//
// Admission is read-only. A missing selected leaf returns MaterializationRequired;
// callers must perform bounded image ingestion before retrying its immutable ID.
// Execution stays inside the caller's existing storage mutation capabilities;
// an Image proves what content was admitted, not permission to mutate tenant
// substrate.
package imageexec
