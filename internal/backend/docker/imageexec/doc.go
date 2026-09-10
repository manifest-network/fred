// Package imageexec admits immutable container images and binds their use to
// typed Docker and Compose execution sinks. Admission resolves a runnable leaf,
// validates its metadata, and materializes its local image-store record before
// returning an Image. Images and prepared projects cannot be reconstructed from
// IDs or labels. Their zero values are inert.
//
// Admission may pull an immutable manifest. Callers must keep admission and
// execution inside their existing storage mutation capabilities; an Image proves
// what content was admitted, not permission to mutate tenant substrate.
package imageexec
