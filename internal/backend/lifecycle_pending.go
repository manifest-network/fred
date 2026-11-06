package backend

import (
	"errors"
	"fmt"
	"time"
)

// CodeLifecyclePending is a 503 observation of admitted lifecycle work. Unlike
// a capacity refusal it grants no settlement or no-dispatch authority.
const CodeLifecyclePending = "lifecycle_pending"

// DeprovisionResponseBudget is the longest a backend takes to answer
// /deprovision. A close that outlasts it keeps running on the backend, which
// answers 503 lifecycle_pending so Fred retries the close rather than timing
// out the request. Every Fred request timeout must exceed it.
const DeprovisionResponseBudget = 20 * time.Second

// ValidateRequestTimeout checks a configured backend request timeout. Zero
// selects the default. Any other value must leave a backend room to answer
// /deprovision within DeprovisionResponseBudget.
func ValidateRequestTimeout(timeout time.Duration) error {
	if timeout < 0 {
		return errors.New("must not be negative")
	}
	if timeout != 0 && timeout <= DeprovisionResponseBudget {
		return fmt.Errorf("must exceed %s, the longest a backend takes to answer /deprovision", DeprovisionResponseBudget)
	}
	return nil
}

// Only the bounded transport decoder mints this marker. It deliberately does
// not unwrap to any refusal or lifecycle sentinel.
type lifecyclePendingResponse struct{}

func (*lifecyclePendingResponse) Error() string { return "admitted lifecycle work remains pending" }

func isLifecyclePendingResponse(err error) bool {
	var pending *lifecyclePendingResponse
	var closePending *deprovisionLifecyclePendingResponse
	return errors.As(err, &pending) || errors.As(err, &closePending)
}

// parseMaintenanceAvailability shares the strict capacity-envelope decoder,
// but recognizes the observation only for lifecycle mutation endpoints.
func (c *HTTPClient) parseMaintenanceAvailability(body []byte, operation string) error {
	code, msg, err := c.parseErrorCode(body, operation)
	if err != nil {
		return err
	}
	if code == CodeLifecyclePending {
		return &lifecyclePendingResponse{}
	}
	return c.capacityError(code, msg, operation)
}
