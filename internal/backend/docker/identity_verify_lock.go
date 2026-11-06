package docker

import (
	"context"
	"sync"
)

// identityVerifyLock serializes storage identity verification and the
// identity latch. A verification waits for it under its caller's context, so
// a request whose deadline passes stops waiting behind a slow verification
// instead of outlasting its own deadline. The zero value is unlocked.
type identityVerifyLock struct {
	init sync.Once
	held chan struct{}
}

func (l *identityVerifyLock) slot() chan struct{} {
	l.init.Do(func() { l.held = make(chan struct{}, 1) })
	return l.held
}

// lockContext acquires the lock, or returns ctx's error holding nothing when
// ctx ends first.
func (l *identityVerifyLock) lockContext(ctx context.Context) error {
	select {
	case l.slot() <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// lock acquires the lock for work that has no caller deadline.
func (l *identityVerifyLock) lock() { l.slot() <- struct{}{} }

func (l *identityVerifyLock) unlock() { <-l.slot() }
