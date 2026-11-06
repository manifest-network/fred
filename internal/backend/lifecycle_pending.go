package backend

import (
	"errors"
	"time"
)

// CodeLifecyclePending is a 503 observation of admitted lifecycle work. Unlike
// a capacity refusal it grants no settlement or no-dispatch authority.
const CodeLifecyclePending = "lifecycle_pending"

// DeprovisionResponseBudget is the longest a backend takes to answer
// /deprovision. A close that outlasts it keeps running on the backend, which
// answers 503 lifecycle_pending so Fred retries the close rather than timing
// out the request. Fred's client never gives /deprovision less than this
// budget plus time to deliver the answer.
const DeprovisionResponseBudget = 20 * time.Second

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
