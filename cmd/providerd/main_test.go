package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/chain"
)

func TestSafeGo_PropagatesError(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 1)
	testErr := errors.New("test failure")

	safeGo(&wg, errChan, "mycomp", func() error {
		return testErr
	})
	wg.Wait()

	select {
	case err := <-errChan:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mycomp error")
		assert.ErrorIs(t, err, testErr)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for error")
	}
}

func TestSafeGo_RecoversPanic(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	safeGo(&wg, errChan, "panicker", func() error {
		panic("oh no")
	})
	wg.Wait()

	select {
	case err := <-errChan:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "panicker panic")
		assert.Contains(t, err.Error(), "oh no")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for panic error")
	}
}

func TestSafeGo_FiltersContextCanceled(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	safeGo(&wg, errChan, "canceler", func() error {
		return context.Canceled
	})
	wg.Wait()

	select {
	case err := <-errChan:
		t.Fatalf("expected no error, got: %v", err)
	default:
		// ok – nothing sent
	}
}

func TestSafeGo_FiltersWrappedContextCanceled(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	safeGo(&wg, errChan, "wrappedcancel", func() error {
		return fmt.Errorf("wrap: %w", context.Canceled)
	})
	wg.Wait()

	// errors.Is unwraps, so wrapped context.Canceled is also filtered.
	select {
	case err := <-errChan:
		t.Fatalf("expected no error for wrapped context.Canceled, got: %v", err)
	default:
		// ok
	}
}

func TestSafeGo_NilErrorNoSend(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	safeGo(&wg, errChan, "noop", func() error {
		return nil
	})
	wg.Wait()

	select {
	case err := <-errChan:
		t.Fatalf("expected no error, got: %v", err)
	default:
		// ok
	}
}

func TestSafeGo_WaitGroupTracking(t *testing.T) {
	var wg sync.WaitGroup
	errChan := make(chan error, 1)
	done := make(chan struct{})

	safeGo(&wg, errChan, "tracker", func() error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// wg.Wait completed after the goroutine returned
	case <-time.After(2 * time.Second):
		t.Fatal("wg.Wait() did not complete in time")
	}
}

// TestDemoteOnGrantSetupError guards the reachable half of the ENG-688 fix. The
// sentinel itself is covered in internal/chain, but this is the branch that
// actually decides whether providerd sheds its sub-signers, and an inverted
// condition here would silently reinstate the bug.
func TestDemoteOnGrantSetupError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "success does not demote",
			err:  nil,
			want: false,
		},
		{
			name: "unverified grants do not demote",
			err:  chain.ErrGrantsUnverified,
			want: false,
		},
		{
			name: "wrapped unverified grants do not demote",
			err:  fmt.Errorf("ensure authz grants after 3 attempts: %w: i/o timeout", chain.ErrGrantsUnverified),
			want: false,
		},
		{
			name: "grants known missing and uncreatable demotes",
			err:  errors.New("failed to create grant: insufficient fee"),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, demoteOnGrantSetupError(tt.err))
		})
	}
}
