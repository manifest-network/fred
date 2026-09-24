package imagefetch

import (
	"context"
	"io"
	"net/http"
	"testing"
	"testing/synctest"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/stretchr/testify/require"
)

type panickingImporter struct {
	entered  chan io.Reader
	panicNow chan struct{}
}

func (d panickingImporter) ImageLoad(_ context.Context, input io.Reader, _ ...client.ImageLoadOption) (image.LoadResponse, error) {
	d.entered <- input
	<-d.panicNow
	panic("daemon import panic")
}

func TestImportPanicJoinsUploadBeforeReleasingItsOwners(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("unconsumed staged content")))
	transport := f.server.Client().Transport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	stage := t.TempDir()
	synctest.Test(t, func(t *testing.T) {
		daemon := panickingImporter{entered: make(chan io.Reader, 1), panicNow: make(chan struct{})}
		loader, err := NewLoader(daemon, stage, 1<<20, WithRegistryTransport(transport))
		require.NoError(t, err)
		prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
		require.NoError(t, err)
		defer prepared.Close()
		finished := make(chan any, 1)
		go func() {
			defer func() { finished <- recover() }()
			_, _ = loader.Import(t.Context(), prepared)
		}()
		input := (<-daemon.entered).(io.ReadCloser)
		defer input.Close()
		synctest.Wait() // Both the archive writer and the foreign daemon are blocked.
		close(daemon.panicNow)
		require.Equal(t, "daemon import panic", <-finished)
		var firstByte [1]byte
		n, err := input.Read(firstByte[:])
		require.Zero(t, n, "panic cleanup must close and join the upload before returning control")
		require.ErrorIs(t, err, io.ErrClosedPipe)
		require.NoError(t, prepared.Close())
		require.NoError(t, loader.Shutdown(t.Context()), "no archive worker may retain the closed staging files")
		pending, err := loader.PendingBytes()
		require.NoError(t, err)
		require.Equal(t, prepared.ImportBytes(), pending, "a panic is not positive daemon completion evidence")
		unknown, err := loader.UnknownBytes()
		require.NoError(t, err)
		require.Equal(t, pending, unknown, "the ended owner must expose its still-durable debit")
	})
}
