// Package httpserver provides the substrate-agnostic HTTP server used by
// both docker-backend and k3s-backend. It contains the full route table,
// HMAC inbound middleware, SSRF guard for outbound callback URLs, JSON
// response helpers, and the shared request/response types.
//
// Each cmd/<backend>/main.go wires its concrete backend implementation into
// NewServer and runs the HTTP loop; no handler or middleware code lives in
// the command packages themselves.
package httpserver

import "github.com/manifest-network/fred/internal/backend"

// DeprovisionRequest is the request body for /deprovision.
type DeprovisionRequest struct {
	LeaseUUID string `json:"lease_uuid"`
}

// StatsResponse is the response body for /stats.
type StatsResponse struct {
	TotalCPUCores     float64 `json:"total_cpu_cores"`
	TotalMemoryMB     int64   `json:"total_memory_mb"`
	TotalDiskMB       int64   `json:"total_disk_mb"`
	AllocatedCPUCores float64 `json:"allocated_cpu_cores"`
	AllocatedMemoryMB int64   `json:"allocated_memory_mb"`
	AllocatedDiskMB   int64   `json:"allocated_disk_mb"`
	AvailableCPUCores float64 `json:"available_cpu_cores"`
	AvailableMemoryMB int64   `json:"available_memory_mb"`
	AvailableDiskMB   int64   `json:"available_disk_mb"`
	ActiveContainers  int     `json:"active_containers"`
}

// StatusResponse is a simple status response.
type StatusResponse struct {
	Status string `json:"status"`
}

// ErrorResponse is the response body for errors.
type ErrorResponse struct {
	Error          string                 `json:"error"`
	ValidationCode backend.ValidationCode `json:"validation_code,omitempty"`
	// Code is an omitempty discriminator for overloaded status codes. Restore
	// returns 409 for BOTH ErrInvalidState and ErrAlreadyProvisioned; the
	// client maps a bare 409 to ErrInvalidState, so the already-provisioned
	// case sets Code="already_provisioned" to let the client reconstruct the
	// correct sentinel. (Mirrors the validation_code body-discriminator pattern.)
	Code string `json:"code,omitempty"`
}
