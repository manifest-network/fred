package docker

import (
	"context"
	"errors"
	"testing"
)

func TestNoopVolumeManager_Usage_Unsupported(t *testing.T) {
	n := &noopVolumeManager{}
	_, err := n.Usage(context.Background(), "fred-x-app-0")
	if !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("noop Usage error = %v; want wraps errors.ErrUnsupported", err)
	}
}

func TestNoopVolumeManager_Kind(t *testing.T) {
	if got := (&noopVolumeManager{}).Kind(); got != "noop" {
		t.Fatalf("noop Kind = %q; want %q", got, "noop")
	}
}
