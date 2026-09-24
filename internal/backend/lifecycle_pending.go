package backend

import "errors"

// CodeLifecyclePending is a 503 observation of admitted lifecycle work. Unlike
// a capacity refusal it grants no settlement or no-dispatch authority.
const CodeLifecyclePending = "lifecycle_pending"

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
