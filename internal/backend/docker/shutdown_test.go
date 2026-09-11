package docker

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStopIsBoundedAndLeavesDependenciesOpenWhileWorkerMayRun(t *testing.T) {
	var closeCalls atomic.Int64
	mock := &mockDockerClient{CloseFn: func() error {
		closeCalls.Add(1)
		return nil
	}}
	b := newBackendForTest(mock, nil)
	b.shutdownDrainTimeout = 25 * time.Millisecond

	// Model a mutator which ignored stopCtx cancellation. Stop must return a
	// typed failure instead of hanging forever, but it must not close the Docker
	// client (or durable stores) under the still-running goroutine.
	b.wg.Add(1)
	started := time.Now()
	err := b.Stop()
	require.ErrorIs(t, err, ErrShutdownDrainTimeout)
	assert.Less(t, time.Since(started), time.Second)
	assert.Equal(t, int64(0), closeCalls.Load(),
		"dependencies must remain open until every worker has actually drained")
	assert.Error(t, b.stopCtx.Err(), "Stop must signal cancellation before waiting")

	// Stop installs only one waiter. Once the worker returns, a retry observes
	// the same closed drain channel and performs the normal resource close.
	b.wg.Done()
	require.Eventually(t, func() bool {
		select {
		case <-b.shutdownWaitDone:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
	require.NoError(t, b.Stop())
	assert.Equal(t, int64(1), closeCalls.Load())
}
