package api

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestRateLimiterConcurrentFirstRequestsShareOneBurst(t *testing.T) {
	ip := NewRateLimiter(0, 1, nil)
	tenant := NewTenantRateLimiter(0, 1, "manifest")
	for name, get := range map[string]func(string) *rate.Limiter{"IP": ip.getVisitor, "tenant": tenant.getLimiter} {
		t.Run(name, func(t *testing.T) {
			for round := range 100 {
				key := fmt.Sprintf("cold-key-%d", round)
				start := make(chan struct{})
				var admitted atomic.Int64
				var wg sync.WaitGroup
				for range 64 {
					wg.Go(func() {
						<-start
						if get(key).Allow() {
							admitted.Add(1)
						}
					})
				}
				close(start)
				wg.Wait()
				require.EqualValues(t, 1, admitted.Load(), "one shared burst per cold key")
			}
		})
	}
}
