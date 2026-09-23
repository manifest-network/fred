package imagefetch

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/stretchr/testify/require"
)

type coordinatedImporter struct {
	arrivals chan context.Context
	results  chan error
}

func (d *coordinatedImporter) ImageLoad(ctx context.Context, input io.Reader, _ ...client.ImageLoadOption) (image.LoadResponse, error) {
	if _, err := io.Copy(io.Discard, input); err != nil {
		return image.LoadResponse{}, err
	}
	d.arrivals <- ctx
	select {
	case err := <-d.results:
		if err != nil {
			return image.LoadResponse{}, err
		}
		return image.LoadResponse{Body: io.NopCloser(strings.NewReader(`{"stream":"Loaded image"}`)), JSON: true}, nil
	case <-ctx.Done():
		return image.LoadResponse{}, ctx.Err()
	}
}

func TestImportOwnsCompletionAfterCallerCancellation(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	daemon := &coordinatedImporter{arrivals: make(chan context.Context, 1), results: make(chan error, 1)}
	loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Close()) }()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := loader.Import(ctx, p); finished <- err }()
	work := <-daemon.arrivals
	pending, err := loader.PendingBytes()
	require.NoError(t, err)
	require.Equal(t, p.ImportBytes(), pending)
	cancel()
	require.NoError(t, work.Err(), "caller cancellation cannot detach Docker extraction from its admission owner")
	_, bounded := work.Deadline()
	require.False(t, bounded, "uncanceled work has no artificial import deadline")
	daemon.results <- nil
	require.NoError(t, <-finished)
	pending, err = loader.PendingBytes()
	require.NoError(t, err)
	require.Zero(t, pending)
}

func TestOutstandingImportDebitSurvivesReopenAndUnknownCompletion(t *testing.T) {
	for _, result := range []string{`{"error":"failed"}` + "\n" + `{"stream":`, `unrecognized completion`} {
		t.Run(result, func(t *testing.T) {
			f := newRegistry(t, layerTar(t, []byte("content")))
			stage := t.TempDir()
			loader, err := NewLoader(&recordingImporter{result: result}, stage, 1<<20, WithRegistryTransport(f.server.Client().Transport))
			require.NoError(t, err)
			p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
			require.NoError(t, err)
			_, err = loader.Import(t.Context(), p)
			require.Error(t, err)
			require.NoError(t, p.Close())
			reopened, err := NewLoader(&recordingImporter{}, stage, 1<<20, WithRegistryTransport(f.server.Client().Transport))
			require.NoError(t, err)
			pending, err := reopened.PendingBytes()
			require.NoError(t, err)
			require.Equal(t, p.ImportBytes(), pending)
			require.NoError(t, reopened.CleanupAbandoned())
			pending, err = reopened.PendingBytes()
			require.NoError(t, err)
			require.Equal(t, p.ImportBytes(), pending, "cleanup cannot forgive unproven import completion")
		})
	}
}

func TestConcurrentImportsSettleOnlyTheirOwnDebit(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	daemon := &coordinatedImporter{arrivals: make(chan context.Context, 2), results: make(chan error, 2)}
	loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	first, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer func() { require.NoError(t, first.Close()) }()
	second, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer func() { require.NoError(t, second.Close()) }()
	finished := make(chan error, 2)
	go func() { _, err := loader.Import(t.Context(), first); finished <- err }()
	go func() { _, err := loader.Import(t.Context(), second); finished <- err }()
	<-daemon.arrivals
	<-daemon.arrivals
	pending, err := loader.PendingBytes()
	require.NoError(t, err)
	require.Equal(t, first.ImportBytes()+second.ImportBytes(), pending)
	daemon.results <- errors.New("lost daemon response")
	require.ErrorContains(t, <-finished, "lost daemon response")
	daemon.results <- nil
	require.NoError(t, <-finished)
	pending, err = loader.PendingBytes()
	require.NoError(t, err)
	require.Equal(t, first.ImportBytes(), pending, "successful completion releases exactly one admission, leaving the ambiguous allocation")
}

func TestUnreadableDebitPreventsDaemonDispatch(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	stage := t.TempDir()
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, stage, 1<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Close()) }()
	require.NoError(t, os.WriteFile(filepath.Join(stage, debitFileName), []byte("corrupt"), 0o600))
	_, err = loader.PendingBytes()
	require.Error(t, err)
	_, err = loader.Import(t.Context(), p)
	require.ErrorContains(t, err, "reserve outstanding")
	require.Zero(t, daemon.loads)
}

func TestMissingLedgerDirectoryPreventsDaemonDispatch(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	stage := filepath.Join(t.TempDir(), "stage")
	require.NoError(t, os.Mkdir(stage, 0o700))
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, stage, 1<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	moved := stage + "-moved"
	require.NoError(t, os.Rename(stage, moved))
	_, err = loader.Import(t.Context(), p)
	require.ErrorContains(t, err, "reserve outstanding")
	require.Zero(t, daemon.loads)
	require.NoError(t, os.Rename(moved, stage))
	require.NoError(t, p.Close())
}

func TestCopiedLoaderSharesLedgerSynchronization(t *testing.T) {
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), 1<<20)
	require.NoError(t, err)
	copied := *loader
	require.Same(t, loader.ledger, copied.ledger)
	require.NoError(t, loader.changeDebit(11))
	require.NoError(t, copied.changeDebit(17))
	pending, err := loader.PendingBytes()
	require.NoError(t, err)
	require.Equal(t, int64(28), pending)
}

type completionEOFReader struct {
	reader io.Reader
	eof    bool
}

func (r *completionEOFReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err == io.EOF {
		r.eof = true
	}
	return n, err
}

func TestFailureCompletionStillDrainsToTerminalEOF(t *testing.T) {
	reader := &completionEOFReader{reader: strings.NewReader(`{"error":"failed"}` + "\n" + `{"stream":"cleanup complete"}`)}
	completion := readCompletion(reader)
	require.ErrorContains(t, completion.failure, "failed")
	require.True(t, completion.completed)
	require.True(t, reader.eof)
}

func TestPendingDebitRejectsChecksumCorruption(t *testing.T) {
	stage := t.TempDir()
	loader, err := NewLoader(&recordingImporter{}, stage, 1<<20)
	require.NoError(t, err)
	require.NoError(t, loader.changeDebit(123))
	file := filepath.Join(stage, debitFileName)
	record, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Len(t, record, debitRecordSize)
	record[15] ^= 1
	require.NoError(t, os.WriteFile(file, record, 0o600))
	_, err = loader.PendingBytes()
	require.ErrorContains(t, err, "corrupt")
}

func TestDefinitiveDaemonFailuresSettleTheirOwnDebit(t *testing.T) {
	for _, result := range []string{`{"error":"image rejected"}`, `{"errorDetail":{"message":"image rejected"}}`, `{"stream":"Error unpacking image example: disk full"}`} {
		t.Run(result, func(t *testing.T) {
			f := newRegistry(t, layerTar(t, []byte("content")))
			stage := t.TempDir()
			loader, err := NewLoader(&recordingImporter{result: result}, stage, 1<<20, WithRegistryTransport(f.server.Client().Transport))
			require.NoError(t, err)
			// A terminal refusal must settle only this attempt, preserving
			// unrelated earlier work whose completion remains unknown.
			require.NoError(t, loader.changeDebit(123))
			p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
			require.NoError(t, err)
			defer func() { require.NoError(t, p.Close()) }()
			imported, err := loader.Import(t.Context(), p)
			require.Error(t, err)
			require.Empty(t, imported.ManifestID(), "terminal failure is completion evidence, never image-use authority")
			pending, err := loader.PendingBytes()
			require.NoError(t, err)
			require.Equal(t, int64(123), pending)
			reopened, err := NewLoader(&recordingImporter{}, stage, 1<<20, WithRegistryTransport(f.server.Client().Transport))
			require.NoError(t, err)
			pending, err = reopened.PendingBytes()
			require.NoError(t, err)
			require.Equal(t, int64(123), pending)
		})
	}
}

type immediateCompletionImporter struct{}

func (immediateCompletionImporter) ImageLoad(context.Context, io.Reader, ...client.ImageLoadOption) (image.LoadResponse, error) {
	return image.LoadResponse{Body: io.NopCloser(strings.NewReader(`{"error":"rejected before upload completion"}`)), JSON: true}, nil
}

func TestTerminalResponseWithoutCompletedUploadRetainsDebit(t *testing.T) {
	f := newRegistry(t, layerTar(t, []byte("content")))
	loader, err := NewLoader(immediateCompletionImporter{}, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport))
	require.NoError(t, err)
	p, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Close()) }()
	_, err = loader.Import(t.Context(), p)
	require.Error(t, err)
	pending, err := loader.PendingBytes()
	require.NoError(t, err)
	require.Equal(t, p.ImportBytes(), pending)
}

func TestMalformedOrTruncatedCompletionCannotMintTerminalEvidence(t *testing.T) {
	for _, body := range []string{`{"error":"refused"}` + "\n" + `{"stream":`, `not JSON`} {
		completion := readCompletion(strings.NewReader(body))
		require.Error(t, completion.failure)
		require.False(t, completion.completed)
	}
}
