package imagefetch

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

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
}

// NewLoader creates a bounded importer in an existing writable directory. Its
// caller must hold exclusive authority for that directory while this Loader is
// used. Copies of a Loader share the same durable debit synchronization.
func NewLoader(source Importer, stageRoot string, maxBytes int64) (*Loader, error) {
	if util.IsNilInterface(source) || !filepath.IsAbs(stageRoot) || maxBytes <= 0 || maxBytes > math.MaxInt64/8 {
		return nil, errors.New("image importer requires a daemon, absolute staging directory, and bounded positive byte limit")
	}
	if info, err := os.Stat(stageRoot); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("image staging directory is unavailable: %s", stageRoot)
	}
	return &Loader{daemon: source, stageRoot: stageRoot, maxBytes: maxBytes, ledger: &debitLedger{root: stageRoot}}, nil
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
	importBytes int64
	consumed    bool
	closed      bool
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

// Close is idempotent and serialized against an active import.
func (p *Prepared) Close() error {
	if p == nil || p.state == nil {
		return nil
	}
	state := p.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return nil
	}
	state.closed = true
	var err error
	for _, b := range state.blobs {
		if b.file != nil {
			err = errors.Join(err, b.file.Close())
		}
	}
	if state.dir != "" {
		err = errors.Join(err, os.Remove(state.dir))
	}
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
	p := i.platform
	p.OSFeatures = slices.Clone(p.OSFeatures)
	return p
}

// Import accepts only this Loader's unconsumed capability. The upload contains
// original verified blobs, so neither tag races nor registry response changes
// can introduce unmeasured content into Docker.
func (l *Loader) Import(ctx context.Context, p *Prepared) (Imported, error) {
	if l == nil || p == nil || p.state == nil {
		return Imported{}, errors.New("invalid prepared image")
	}
	state := p.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.issuer != l || state.closed || state.consumed {
		return Imported{}, errors.New("invalid, foreign, closed or consumed prepared image")
	}
	if err := ctx.Err(); err != nil {
		return Imported{}, err
	}
	state.consumed = true
	// Persist the complete allocation before dispatch. A lost response can
	// leave Docker extracting after its client disconnects, so uncertainty
	// must continue consuming admission capacity, including after a restart.
	if err := l.changeDebit(state.importBytes); err != nil {
		return Imported{}, fmt.Errorf("reserve outstanding image import allocation: %w", err)
	}
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Minute)
	defer cancel()
	reader, writer := io.Pipe()
	written := make(chan error, 1)
	go func() {
		err := writeArchive(work, writer, state.blobs)
		_ = writer.CloseWithError(err)
		written <- err
	}()
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
	uploadErr := <-written
	err = errors.Join(err, uploadErr)
	// A daemon refusal is still completed work when its entire response and
	// upload terminated. Preserve the business failure without inventing debt.
	if completion.completed && uploadErr == nil {
		if settleErr := l.changeDebit(-state.importBytes); settleErr != nil {
			err = errors.Join(err, fmt.Errorf("settle completed image import allocation: %w", settleErr))
		}
	}
	if err != nil {
		return Imported{}, fmt.Errorf("import verified image: %w", err)
	}
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
