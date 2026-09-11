package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/errdefs"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

const imageInspectionCleanupTimeout = 5 * time.Second
const imageInspectionRecoveryTimeout = 10 * time.Second

// imageInspectionCoordinator is the constructor-bound owner of helper effects.
// Preparation receives a live Started subject; recovery receives only journal
// receipts. Neither path exposes a raw Create/Remove or a caller-selected ID.
type imageInspectionCoordinator struct {
	journal   *shared.ImageInspectionJournal
	creator   *imageexec.DockerCreator
	sdk       dockerSDKView
	lifetime  context.Context
	authorize substratemutation.Authorize
	complete  substratemutation.Complete
	resolve   func(string, substratemutation.StepResult) error
	authority func() error
	mu        sync.Mutex
	active    map[string]struct{}
}

func newImageInspectionCoordinator(
	client *DockerClient,
	callbacks *shared.CallbackStore,
	lifetime context.Context,
	authorize substratemutation.Authorize,
	complete substratemutation.Complete,
	resolve func(string, substratemutation.StepResult) error,
	authority func() error,
) (*imageInspectionCoordinator, error) {
	if client == nil || client.creator == nil || lifetime == nil || authorize == nil || complete == nil || resolve == nil || authority == nil {
		return nil, errors.New("image inspection requires a client, journal and backend mutation lifetime")
	}
	if client.inspections != nil {
		return nil, errors.New("docker image inspection owner is already bound")
	}
	if err := lifetime.Err(); err != nil {
		return nil, err
	}
	journal, err := shared.NewImageInspectionJournal(callbacks)
	if err != nil {
		return nil, err
	}
	c := &imageInspectionCoordinator{
		journal: journal, creator: client.creator, sdk: client.client,
		lifetime: lifetime, authorize: authorize, complete: complete, resolve: resolve, authority: authority,
		active: make(map[string]struct{}),
	}
	client.inspections = c
	return c, nil
}

// imageInspectionSession never escapes a single high-level helper. Its private
// container ID comes only from Create plus exact inspect, while Close consumes
// the receipt through a separately bounded, still-attested backend lifetime.
type imageInspectionSession struct {
	owner       *imageInspectionCoordinator
	receipt     shared.ImageInspectionReceipt
	containerID string
	closed      bool
}

func (d *DockerClient) openImageInspection(ctx context.Context, image imageexec.Image, origin shared.ImageInspectionOrigin) (*imageInspectionSession, error) {
	if err := d.creator.ValidateImage(image); err != nil {
		return nil, err
	}
	if d.inspections == nil {
		return nil, errors.New("docker image inspection owner is not bound")
	}
	return d.inspections.open(ctx, image, origin)
}

func (c *imageInspectionCoordinator) open(ctx context.Context, image imageexec.Image, origin shared.ImageInspectionOrigin) (_ *imageInspectionSession, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.lifetime.Err(); err != nil {
		return nil, err
	}
	// Serialize allocation with recovery's active check. No recovery can observe
	// the write-ahead row without also observing its live session reservation.
	c.mu.Lock()
	receipt, err := c.journal.Reserve(origin, image.ID(), image.Reference())
	if err == nil {
		c.active[receipt.ID()] = struct{}{}
	}
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	s := &imageInspectionSession{owner: c, receipt: receipt}
	ready := false
	defer func() {
		if !ready {
			err = errors.Join(err, s.close())
		}
	}()
	result := substratemutation.RunStep(ctx, "create image inspection helper", c.authorize, c.complete, func(ctx context.Context) error {
		response, createErr := c.creator.Create(ctx, image, &container.Config{Labels: inspectionLabels(receipt)}, nil, nil, receipt.Name())
		if createErr != nil {
			return createErr
		}
		// A successful SDK response is the only transition out of uncertain
		// Create. Persist failure keeps the original receipt, including if the
		// daemon side effect succeeded but the process loses its journal authority.
		created, recordErr := c.journal.RecordCreated(receipt, response.ID)
		if recordErr != nil {
			return recordErr
		}
		s.receipt = created
		return nil
	})
	if err := c.resolve("create image inspection helper", result); err != nil {
		return nil, err
	}
	// A response ID is not by itself ownership evidence. Every copy and cleanup
	// starts from the receipt's exact immutable identity and a fresh inspection.
	id, absent, err := c.inspect(ctx, s.receipt)
	if err != nil {
		return nil, err
	}
	if absent {
		return nil, errors.New("created image inspection helper is absent")
	}
	s.containerID = id
	ready = true
	return s, nil
}

func inspectionLabels(r shared.ImageInspectionReceipt) map[string]string {
	return map[string]string{
		"fred.inspection.schema":     "1",
		"fred.inspection.id":         r.ID(),
		"fred.inspection.backend":    r.Backend(),
		"fred.inspection.storage_id": r.StorageID().String(),
		"fred.inspection.kind":       r.Kind(),
		"fred.inspection.subject_id": r.SubjectID(),
		"fred.inspection.lease_uuid": r.LeaseUUID(),
		LabelImageID:                 r.ImageID(),
		LabelImageReference:          r.ImageReference(),
	}
}

func (s *imageInspectionSession) copy(ctx context.Context, path string) (io.ReadCloser, container.PathStat, error) {
	if s == nil || s.closed {
		return nil, container.PathStat{}, errors.New("image inspection session is closed")
	}
	// Read authorization joins work and backend cancellation, then re-attests
	// lineage. A replacement daemon cannot provide a same-ID helper for a copy.
	readCtx, done, err := s.owner.authorize(ctx, "read image inspection helper")
	if err != nil {
		return nil, container.PathStat{}, err
	}
	id, absent, err := s.owner.inspect(readCtx, s.receipt)
	if err != nil || absent || id != s.containerID {
		done()
		return nil, container.PathStat{}, errors.Join(err, errors.New("image inspection helper no longer present with its exact identity"))
	}
	reader, stat, err := s.owner.sdk.CopyFromContainer(readCtx, id, path)
	if err != nil {
		done()
		return nil, stat, err
	}
	return &inspectionReadCloser{ReadCloser: reader, done: done}, stat, nil
}

type inspectionReadCloser struct {
	io.ReadCloser
	done func()
}

func (r *inspectionReadCloser) Close() error { defer r.done(); return r.ReadCloser.Close() }

// readFile adapts the existing bounded passwd/group tar reader without exposing
// the helper ID outside its owner. The adapter rejects any attempted rebinding.
func (s *imageInspectionSession) readFile(ctx context.Context, path string) ([]byte, error) {
	return readFileFromContainer(ctx, inspectionFileReader{s}, "", path)
}

type inspectionFileReader struct{ session *imageInspectionSession }

func (r inspectionFileReader) CopyFromContainer(ctx context.Context, id, path string) (io.ReadCloser, container.PathStat, error) {
	if id != "" {
		return nil, container.PathStat{}, errors.New("inspection file reader has no caller-selectable container")
	}
	return r.session.copy(ctx, path)
}

func (s *imageInspectionSession) close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	defer s.owner.release(s.receipt.ID())
	return s.owner.cleanup(s.receipt)
}

func (c *imageInspectionCoordinator) release(id string) {
	c.mu.Lock()
	delete(c.active, id)
	c.mu.Unlock()
}

// inspectionRecoveryReport describes independently retained helper obligations.
// It grants no mutation authority and cannot be used to clear a receipt.
type inspectionRecoveryReport struct {
	pending  []inspectionRecoveryPending
	deferred error
}

type inspectionRecoveryPending struct {
	id    string
	cause error
}

// inspectionJournalFailure is issued only at a journal operation boundary.
// Arbitrary daemon errors cannot masquerade as a durable-store failure.
type inspectionJournalFailure struct{ cause error }

func (e *inspectionJournalFailure) Error() string { return e.cause.Error() }
func (e *inspectionJournalFailure) Unwrap() error { return e.cause }

// RecoverAndReport is the complete backend recovery workflow. Independent
// helper cleanup failures remain visible and retryable without preventing
// workload recovery; lost backend authority or journal failures still abort.
func (c *imageInspectionCoordinator) RecoverAndReport(ctx context.Context, logger *slog.Logger) error {
	if logger == nil {
		return errors.New("image inspection recovery logger is required")
	}
	// Helper debt owns a phase cap within startup/reconciliation. It cannot
	// consume the entire workload-recovery budget through a long receipt list.
	recoveryCtx, cancel := context.WithTimeout(ctx, imageInspectionRecoveryTimeout)
	defer cancel()
	report, err := c.Recover(recoveryCtx)
	for _, pending := range report.pending {
		logger.WarnContext(ctx, "Image inspection cleanup remains pending", "inspection_id", pending.id, "error", pending.cause)
	}
	if report.deferred != nil {
		logger.WarnContext(ctx, "Image inspection recovery deferred", "error", report.deferred)
	}
	return err
}

// Recover scans both pending helpers and permanent response-loss receipts.
// Live sessions exclude recovery until their own cleanup handoff completes.
// Closing a workload never removes these independently owned obligations.
func (c *imageInspectionCoordinator) Recover(ctx context.Context) (inspectionRecoveryReport, error) {
	var report inspectionRecoveryReport
	if c == nil {
		return report, errors.New("image inspection recovery owner is unavailable")
	}
	if err := errors.Join(c.authority(), c.lifetime.Err()); err != nil {
		return report, err
	}
	if err := ctx.Err(); err != nil {
		report.deferred = err
		return report, nil
	}
	// List and active claim use the same mutex as Reserve; the list is bounded
	// by the journal's admission ceiling, including permanent uncertain receipts.
	c.mu.Lock()
	receipts, err := c.journal.List()
	var claimed []shared.ImageInspectionReceipt
	if err == nil {
		for _, receipt := range receipts {
			if _, busy := c.active[receipt.ID()]; !busy {
				c.active[receipt.ID()] = struct{}{}
				claimed = append(claimed, receipt)
			}
		}
	}
	c.mu.Unlock()
	if err != nil {
		return report, err
	}
	defer func() {
		for _, receipt := range claimed {
			c.release(receipt.ID())
		}
	}()
	for _, receipt := range claimed {
		if err := errors.Join(c.authority(), c.lifetime.Err()); err != nil {
			return report, err
		}
		if err := ctx.Err(); err != nil {
			report.deferred = err
			return report, nil
		}
		err := c.cleanup(receipt)
		// Authority comes from the constructor-bound backend lifetime, never
		// from daemon error classification. A panic/postcheck failure can
		// withdraw it even if an intermediate helper returned another error.
		if authorityErr := errors.Join(c.authority(), c.lifetime.Err()); authorityErr != nil {
			return report, errors.Join(err, authorityErr)
		}
		var journalFailure *inspectionJournalFailure
		if errors.As(err, &journalFailure) {
			return report, err
		}
		if err != nil {
			report.pending = append(report.pending, inspectionRecoveryPending{id: receipt.ID(), cause: err})
		} else if receipt.ContainerID() == "" {
			report.pending = append(report.pending, inspectionRecoveryPending{id: receipt.ID(), cause: errors.New("Create response was not durably recorded; retained for possible late appearance")})
		}
	}
	return report, nil
}

func (c *imageInspectionCoordinator) cleanup(receipt shared.ImageInspectionReceipt) error {
	// This context belongs to the constructor-bound backend owner, not the
	// canceled inspection work. Authorization still revokes removal on backend
	// stop or failed daemon/storage attestation before and after the effect.
	ctx, cancel := context.WithTimeout(c.lifetime, imageInspectionCleanupTimeout)
	defer cancel()
	result := substratemutation.RunStep(ctx, "remove image inspection helper", c.authorize, c.complete, func(ctx context.Context) error {
		id, absent, err := c.inspect(ctx, receipt)
		if err != nil {
			return err
		}
		if !absent {
			if err := c.sdk.ContainerRemove(ctx, id, container.RemoveOptions{RemoveVolumes: true}); err != nil && !errdefs.IsNotFound(err) {
				return err
			}
			_, absent, err = c.inspect(ctx, receipt)
			if err != nil {
				return err
			}
			if !absent {
				return errors.New("image inspection helper is still present after removal")
			}
		}
		return nil
	})
	if err := c.resolve("remove image inspection helper", result); err != nil {
		return err
	}
	if receipt.ContainerID() != "" {
		if err := c.journal.ForgetRemoved(receipt); err != nil {
			return &inspectionJournalFailure{cause: err}
		}
	}
	// Empty inventory for an uncertain Create remains a permanent recovery
	// receipt. A future sweep can discover a response-lost, late daemon effect.
	return nil
}

func (c *imageInspectionCoordinator) inspect(ctx context.Context, receipt shared.ImageInspectionReceipt) (id string, absent bool, err error) {
	actual, err := c.sdk.ContainerInspect(ctx, receipt.Name())
	if errdefs.IsNotFound(err) {
		if receipt.ContainerID() == "" {
			return "", true, nil
		}
		// Check both exact name and exact recorded ID. A renamed/replaced name
		// cannot become an invented absence proof for an extant owned container.
		actual, err = c.sdk.ContainerInspect(ctx, receipt.ContainerID())
		if errdefs.IsNotFound(err) {
			return "", true, nil
		}
	}
	if err != nil {
		return "", false, err
	}
	if actual.ContainerJSONBase == nil || actual.Config == nil || actual.State == nil ||
		strings.TrimPrefix(actual.Name, "/") != receipt.Name() ||
		actual.Image != receipt.ImageID() || actual.Config.Image != receipt.ImageID() ||
		len(actual.ID) != 64 || strings.Trim(actual.ID, "0123456789abcdef") != "" ||
		(receipt.ContainerID() != "" && actual.ID != receipt.ContainerID()) ||
		actual.State.Running || actual.State.Restarting || actual.State.Paused {
		return "", false, errors.New("image inspection name or container belongs to a foreign or ambiguous identity")
	}
	labels := inspectionLabels(receipt)
	for key, expected := range labels {
		if actual.Config.Labels[key] != expected {
			return "", false, fmt.Errorf("image inspection identity label %s differs", key)
		}
	}
	for key := range actual.Config.Labels {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "fred.") || strings.HasPrefix(lower, "com.docker.compose.") || strings.HasPrefix(lower, "traefik.") {
			if _, owned := labels[key]; !owned {
				return "", false, errors.New("image inspection carries foreign reserved labels")
			}
		}
	}
	return actual.ID, false, nil
}
