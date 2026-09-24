package imagefetch

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/util"
)

// Importer is the only daemon mutation available to a Loader.
type Importer interface {
	ImageLoad(context.Context, io.Reader, ...client.ImageLoadOption) (image.LoadResponse, error)
}

// Loader owns the registry preparation and exact-content import boundary.
type Loader struct {
	daemon    Importer
	stageRoot string
	maxBytes  int64
	ledger    *debitLedger
	transport http.RoundTripper
	life      *loaderLifetime
}

// NewLoader creates a bounded importer in an existing writable directory. Its
// caller must hold exclusive authority for that directory while this Loader is
// used. Copies of a Loader share the same durable debit synchronization.
func NewLoader(source Importer, stageRoot string, maxBytes int64, options ...Option) (*Loader, error) {
	if util.IsNilInterface(source) || !filepath.IsAbs(stageRoot) || !validByteLimit(maxBytes) {
		return nil, errors.New("image importer requires a daemon, absolute staging directory, and bounded positive byte limit")
	}
	if info, err := os.Stat(stageRoot); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("image staging directory is unavailable: %s", stageRoot)
	}
	shutdown, cancel := context.WithCancel(context.Background())
	loader := &Loader{daemon: source, stageRoot: stageRoot, maxBytes: maxBytes, ledger: &debitLedger{root: stageRoot}, transport: remote.DefaultTransport, life: &loaderLifetime{shutdown: shutdown, cancel: cancel, drained: make(chan struct{})}}
	for _, option := range options {
		if option == nil {
			cancel()
			return nil, errors.New("nil image loader option")
		}
		if err := option(loader); err != nil {
			cancel()
			return nil, err
		}
	}
	return loader, nil
}

func validByteLimit(maxBytes int64) bool { return maxBytes > 0 && maxBytes <= math.MaxInt64/8 }

// WithBudget derives a distinct preparation issuer for bounded recovery while
// retaining this owner's transport, durable debit ledger and shutdown lifetime.
// The caller supplies an independently verified recovery allowance, rather than
// changing the admission limit of the existing issuer.
func (l *Loader) WithBudget(maxBytes int64) (*Loader, error) {
	if l == nil || l.life == nil || !validByteLimit(maxBytes) {
		return nil, errors.New("image recovery requires a bounded positive byte limit")
	}
	derived := *l
	derived.maxBytes = maxBytes
	return &derived, nil
}

// Option configures registry transport while retaining the mandatory HTTPS and
// response-byte boundaries.
type Option func(*Loader) error

// WithRegistryTransport supplies operator trust or routing configuration. HTTP
// requests remain refused before they reach this transport.
func WithRegistryTransport(transport http.RoundTripper) Option {
	return func(l *Loader) error {
		if util.IsNilInterface(transport) {
			return errors.New("nil registry transport")
		}
		l.transport = transport
		return nil
	}
}

type loaderLifetime struct {
	mu          sync.Mutex
	shutdown    context.Context
	cancel      context.CancelFunc
	drained     chan struct{}
	closed      bool
	active      int
	activeBytes int64
}

// An admitted import owns its own deadline from dispatch. Tenant cancellation
// cannot truncate an accepted exchange, but a hung daemon cannot occupy its
// staging and allocation forever. This independent completion ceiling starts
// at dispatch; it is separate from the configurable registry pull timeout.
const importCompletionTimeout = 30 * time.Minute

// Shutdown closes admission and lets admitted exchanges finish within the
// backend's drain budget. Only expiry of that budget cancels daemon work;
// tenant cancellation cannot terminate an import this owner already dispatched.
func (l *Loader) Shutdown(ctx context.Context) error {
	if l == nil || l.life == nil {
		return nil
	}
	life := l.life
	life.mu.Lock()
	if !life.closed {
		life.closed = true
		if life.active == 0 {
			close(life.drained)
		}
	}
	drained := life.drained
	life.mu.Unlock()
	select {
	case <-drained:
		life.cancel()
		return nil
	case <-ctx.Done():
		life.cancel()
		return ctx.Err()
	}
}

// UnknownBytes excludes live owned admissions from durable unfinished imports.
// A reopened loader has no such owners and therefore charges the complete debt.
func (l *Loader) UnknownBytes() (int64, error) {
	if l == nil || l.life == nil {
		return 0, errors.New("image loader is unavailable")
	}
	l.life.mu.Lock()
	defer l.life.mu.Unlock()
	pending, err := l.PendingBytes()
	return max(0, pending-l.life.activeBytes), err
}

type blob struct {
	name string
	size int64
	file *os.File
	data []byte
}

// Prepared owns immutable, already verified content. Its zero value grants no
// import authority. Close releases all staging space, including after errors.
type Prepared struct {
	state *preparedState
}

// The mutable lifetime belongs to one shared state, so copying the exported
// capability cannot duplicate its consumption or close authority.
type preparedState struct {
	mu          sync.Mutex
	issuer      *Loader
	dir         string
	blobs       []blob
	imported    Imported
	metadata    imageexec.Metadata
	reservation *importAdmissionState
	importBytes int64
	consumed    bool
	closed      bool
	closeDone   chan struct{}
	closeErr    error
}

// ImportBytes returns a conservative allowance for the daemon's archive,
// expanded layer data and filesystem metadata; Fred's staging is already used.
func (p *Prepared) ImportBytes() int64 {
	if p == nil || p.state == nil {
		return 0
	}
	return p.state.importBytes
}

// SourceReference identifies the original repository and selected manifest.
func (p *Prepared) SourceReference() string {
	if p == nil || p.state == nil {
		return ""
	}
	return p.state.imported.source
}

// ManifestID and ConfigID expose the immutable identity before dispatch, for
// callers that durably protect content while admission is in flight.
func (p *Prepared) ManifestID() string {
	if p == nil || p.state == nil {
		return ""
	}
	return p.state.imported.ManifestID()
}
func (p *Prepared) ConfigID() string {
	if p == nil || p.state == nil {
		return ""
	}
	return p.state.imported.ConfigID()
}
func (p *Prepared) Platform() ocispec.Platform {
	if p == nil || p.state == nil {
		return ocispec.Platform{}
	}
	return p.state.imported.Platform()
}
func (p *Prepared) Metadata() imageexec.Metadata {
	if p == nil || p.state == nil {
		return imageexec.Metadata{}
	}
	return p.state.metadata
}

// Close is idempotent and serialized against an active import.
func (p *Prepared) Close() error {
	if p == nil || p.state == nil {
		return nil
	}
	state := p.state
	state.mu.Lock()
	if state.closeDone != nil {
		done := state.closeDone
		state.mu.Unlock()
		<-done
		return state.closeErr
	}
	// Closing consumes preparation authority before relinquishing the lock.
	// A concurrent reservation therefore cannot be inserted after this snapshot.
	state.closed = true
	state.closeDone = make(chan struct{})
	reservation := state.reservation
	state.mu.Unlock()
	var err error
	if reservation != nil {
		err = (&ImportAdmission{state: reservation}).Close()
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, b := range state.blobs {
		if b.file != nil {
			err = errors.Join(err, b.file.Close())
		}
	}
	if state.dir != "" {
		err = errors.Join(err, os.Remove(state.dir))
	}
	state.closeErr = err
	close(state.closeDone)
	return err
}

// Imported is the identity established by a completed exact-content import.
type Imported struct {
	manifest, config, source string
	platform                 ocispec.Platform
}

func (i Imported) ManifestID() string      { return i.manifest }
func (i Imported) ConfigID() string        { return i.config }
func (i Imported) SourceReference() string { return i.source }
func (i Imported) Platform() ocispec.Platform {
	return clonePlatform(i.platform)
}

func clonePlatform(p ocispec.Platform) ocispec.Platform {
	p.OSFeatures = slices.Clone(p.OSFeatures)
	return p
}

// ImportAdmission owns a persisted allocation and a single possible dispatch.
// Copies share its lifecycle. Close cancels an undispatched admission with
// positive evidence that Docker never received it, or waits for its exchange.
type ImportAdmission struct{ state *importAdmissionState }
type importAdmissionState struct {
	mu         sync.Mutex
	issuer     *Loader
	prepared   *preparedState
	finished   bool
	dispatched bool
}

// ReserveImport durably charges allocation before a caller releases its storage
// admission gate. The returned owner must be closed on every subsequent path.
func (l *Loader) ReserveImport(ctx context.Context, p *Prepared) (*ImportAdmission, error) {
	if l == nil || p == nil || p.state == nil {
		return nil, errors.New("invalid prepared image")
	}
	state := p.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.issuer != l || state.closed || state.consumed || !state.metadata.Valid() {
		return nil, errors.New("invalid, foreign, closed or consumed prepared image")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	life := l.life
	life.mu.Lock()
	defer life.mu.Unlock()
	if life.closed {
		return nil, errors.New("image loader is shut down")
	}
	state.consumed = true
	if err := l.changeDebit(state.importBytes); err != nil {
		return nil, fmt.Errorf("reserve outstanding image import allocation: %w", err)
	}
	life.active++
	life.activeBytes += state.importBytes
	admission := &importAdmissionState{issuer: l, prepared: state}
	state.reservation = admission
	return &ImportAdmission{state: admission}, nil
}

func (a *importAdmissionState) finish(settle bool) error {
	if a.finished {
		return nil
	}
	life := a.issuer.life
	life.mu.Lock()
	defer life.mu.Unlock()
	var err error
	if settle {
		err = a.issuer.changeDebit(-a.prepared.importBytes)
	}
	a.finished = true
	life.active--
	life.activeBytes -= a.prepared.importBytes
	if life.closed && life.active == 0 {
		close(life.drained)
	}
	return err
}

// Close releases only an allocation that is proven never dispatched. An
// uncertain completed request retains its durable debit for operator recovery.
func (a *ImportAdmission) Close() error {
	if a == nil || a.state == nil {
		return nil
	}
	state := a.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.finished {
		return nil
	}
	return state.finish(true)
}

// CancelBeforeDispatch consumes this exact admission and proves it cannot issue
// a daemon request. A copied capability cannot race a later dispatch past this
// check. False means the admission already dispatched or was consumed; an error
// means allocation release could not be proven. Neither permits replacement work.
func (a *ImportAdmission) CancelBeforeDispatch() (bool, error) {
	if a == nil || a.state == nil {
		return false, errors.New("invalid image import admission")
	}
	state := a.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dispatched || state.finished {
		return false, nil
	}
	if err := state.finish(true); err != nil {
		return false, err
	}
	return true, nil
}

// Import is the combined reservation and dispatch convenience boundary.
func (l *Loader) Import(ctx context.Context, p *Prepared) (Imported, error) {
	admission, err := l.ReserveImport(ctx, p)
	if err != nil {
		return Imported{}, err
	}
	defer func() { _ = admission.Close() }()
	return l.ImportAdmitted(ctx, admission)
}

// ImportAdmitted dispatches only this Loader's single-use reserved capability.
// Original verified blobs prevent registry changes from introducing new data.
func (l *Loader) ImportAdmitted(ctx context.Context, admission *ImportAdmission) (Imported, error) {
	if l == nil || admission == nil || admission.state == nil {
		return Imported{}, errors.New("invalid image import admission")
	}
	owned := admission.state
	owned.mu.Lock()
	defer owned.mu.Unlock()
	if owned.issuer != l || owned.finished || owned.dispatched {
		return Imported{}, errors.New("foreign, closed or consumed image import admission")
	}
	if err := ctx.Err(); err != nil {
		return Imported{}, err
	}
	l.life.mu.Lock()
	if l.life.closed {
		l.life.mu.Unlock()
		return Imported{}, errors.New("image loader is shut down")
	}
	owned.dispatched = true
	l.life.mu.Unlock()
	defer func() { _ = owned.finish(false) }()
	state := owned.prepared
	state.mu.Lock()
	defer state.mu.Unlock()
	// Dispatch transfers execution lifetime to the loader. Keep the prepared
	// files and allocation owned until the actual daemon exchange completes,
	// even when the tenant closes its lease or its pull deadline expires.
	work, cancel := context.WithTimeout(l.life.shutdown, importCompletionTimeout)
	defer cancel()
	finishObservation := observeImport(work)
	success := false
	defer func() { finishObservation(success) }()
	reader, writer := io.Pipe()
	written := make(chan error, 1)
	go func() { err := writeArchive(work, writer, state.blobs); _ = writer.CloseWithError(err); written <- err }()
	// Foreign daemon/response code can panic. The upload still owns the staged
	// blobs until its writer has stopped; cleanup must join it before releasing
	// either the preparation mutex or the loader's active admission.
	finishUpload := sync.OnceValue(func() error {
		_ = reader.Close()
		return <-written
	})
	defer func() { _ = finishUpload() }()
	response, err := l.daemon.ImageLoad(work, reader, client.ImageLoadWithQuiet(true), client.ImageLoadWithPlatforms(state.imported.platform))
	var completion importCompletion
	if err == nil {
		if response.Body == nil {
			err = errors.New("image import returned no completion stream")
		} else {
			completion = readCompletion(response.Body)
			err = errors.Join(completion.failure, response.Body.Close())
		}
	}
	_ = reader.CloseWithError(err)
	uploadErr := finishUpload()
	err = errors.Join(err, uploadErr)
	if completion.completed && uploadErr == nil {
		if settleErr := owned.finish(true); settleErr != nil {
			err = errors.Join(err, fmt.Errorf("settle completed image import allocation: %w", settleErr))
		}
	}
	if err != nil {
		return Imported{}, fmt.Errorf("import verified image: %w", err)
	}
	success = true
	return state.imported, nil
}

func writeArchive(ctx context.Context, out io.Writer, blobs []blob) error {
	w := tar.NewWriter(out)
	for _, b := range blobs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := w.WriteHeader(&tar.Header{Name: b.name, Mode: 0o600, Size: b.size, Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		if b.file == nil {
			if _, err := w.Write(b.data); err != nil {
				return err
			}
		} else {
			if _, err := b.file.Seek(0, io.SeekStart); err != nil {
				return err
			}
			if _, err := io.CopyN(w, contextReader{ctx, b.file}, b.size); err != nil {
				return err
			}
		}
	}
	return w.Close()
}

// importCompletion separates terminal daemon evidence from deployment success.
// Only the bounded decoder can establish completed by observing actual EOF.
type importCompletion struct {
	failure   error
	completed bool
}

func readCompletion(body io.Reader) importCompletion {
	decoder := json.NewDecoder(&budgetReader{reader: body, remaining: 4 << 20})
	var failure error
	for {
		var message struct {
			Error  string `json:"error"`
			Stream string `json:"stream"`
			Detail *struct {
				Message string `json:"message"`
			} `json:"errorDetail"`
		}
		if err := decoder.Decode(&message); err != nil {
			if err == io.EOF {
				return importCompletion{failure: failure, completed: true}
			}
			return importCompletion{failure: errors.Join(failure, err)}
		}
		if failure == nil && message.Error != "" {
			failure = errors.New(message.Error)
		}
		if failure == nil && message.Detail != nil && message.Detail.Message != "" {
			failure = errors.New(message.Detail.Message)
		}
		if failure == nil && strings.Contains(message.Stream, "Error unpacking image") {
			failure = errors.New("docker reported image unpacking failure")
		}
	}
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

// budgetReader distinguishes actual EOF from truncation at an admission limit.
type budgetReader struct {
	reader    io.Reader
	remaining int64
}

func (r *budgetReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		var extra [1]byte
		n, err := r.reader.Read(extra[:])
		if n != 0 {
			return 0, errors.New("image content exceeds byte limit")
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	return n, err
}
