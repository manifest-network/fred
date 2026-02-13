package backend

import "time"

// LeaseStatusEvent is published when a lease's provisioning status changes.
// Used for real-time delivery (e.g., WebSocket) to notify tenants of status transitions.
type LeaseStatusEvent struct {
	LeaseUUID string          `json:"lease_uuid"`
	Status    ProvisionStatus `json:"status"`
	Error     string          `json:"error,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}
