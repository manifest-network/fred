// Package imagefetch admits registry content before giving Docker any bytes.
// A Prepared capability owns bounded, verified image blobs; only its issuing
// Loader can import them. Docker never re-fetches content from a tenant registry.
// Mutable tags are re-resolved with a manifest HEAD on every preparation, and
// manifests the Loader already verified are reused by repository and digest.
package imagefetch
