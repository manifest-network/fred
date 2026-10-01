package backend

import "context"

// callerAbandonedError is minted only inside a breaker invocation after its
// caller's lifetime ends. Transport timeouts with a live caller still count as
// health failures. Keep this private marker out of the returned error tree:
// cancellation does not alter any causal mutation outcome.
type callerAbandonedError struct{ cause error }

func (e *callerAbandonedError) Error() string { return e.cause.Error() }
func (e *callerAbandonedError) Unwrap() error { return e.cause }

func (c *HTTPClient) execute(ctx context.Context, call func() (any, error)) (any, error) {
	result, err := c.cb.Execute(func() (any, error) {
		result, err := call()
		if err != nil && ctx.Err() != nil {
			return result, &callerAbandonedError{cause: err}
		}
		return result, err
	})
	if abandoned, ok := err.(*callerAbandonedError); ok { //nolint:errorlint // Unwrap only the direct marker minted above.
		err = abandoned.cause
	}
	return result, err
}
