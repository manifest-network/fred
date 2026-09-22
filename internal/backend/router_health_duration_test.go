package backend

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type timedHealthBackend struct {
	*MockBackend
	outcome string
}

func (b *timedHealthBackend) Health(context.Context) error {
	time.Sleep(37 * time.Millisecond)
	switch b.outcome {
	case "failure":
		return errors.New("probe failed")
	case "panic":
		panic("probe panicked")
	default:
		return nil
	}
}

func TestRouterHealthProbeDurationIncludesFailureAndPanic(t *testing.T) {
	for _, outcome := range []string{"success", "failure", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := &timedHealthBackend{MockBackend: NewMockBackend(MockBackendConfig{Name: "timed"}), outcome: outcome}
				router, err := NewRouter(RouterConfig{Backends: []BackendEntry{{Backend: b, IsDefault: true}}})
				require.NoError(t, err)
				results, healthy := router.HealthCheck(t.Context())
				require.Len(t, results, 1)
				assert.Equal(t, outcome == "success", healthy)
				assert.Equal(t, 37*time.Millisecond, results[0].ProbeDuration(), "elapsed time must survive panic replacement of the result")
			})
		})
	}
}
