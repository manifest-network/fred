package docker

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sync"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// imageAdmission owns the interval between choosing a pin and publishing the
// execution identity. Copies share release ownership, so none can reopen GC
// while another copy still owns registry/import work.
type imageAdmission struct{ state *imageAdmissionState }
type imageAdmissionState struct {
	owner  *imageCapacityManager
	pin    *shared.ImagePin
	closed sync.Once
}

func (m *imageCapacityManager) beginAdmission(ctx context.Context, mutations *storageMutations, ref string) (imageAdmission, error) {
	if err := m.lock(ctx); err != nil {
		return imageAdmission{}, err
	}
	defer m.unlock()
	if err := m.collect(ctx); err != nil {
		slog.Warn("image collection inhibited", "backend", m.cfg.Name, "error", err)
	}
	pin, err := m.selectedPin(mutations, ref)
	if err != nil {
		return imageAdmission{}, err
	}
	m.active++
	return imageAdmission{state: &imageAdmissionState{owner: m, pin: pin}}, nil
}

func (a imageAdmission) close() {
	a.state.closed.Do(func() {
		m := a.state.owner
		_ = m.lock(context.Background())
		m.active--
		m.unlock()
	})
}

// imageStaging owns both a bounded worker slot and the maximum unlinked-file
// allocation. No registry body is downloaded before this constructor succeeds.
type imageStaging struct{ state *imageStagingState }
type imageStagingState struct {
	preparation imageTenantPreparation
	owner       *imageCapacityManager
	bytes       int64
	closed      sync.Once
}

func (m *imageCapacityManager) reserveStaging(ctx context.Context, preparation imageTenantPreparation, bytes int64) (stage imageStaging, err error) {
	if err := preparation.startStaging(m); err != nil {
		return imageStaging{}, err
	}
	slotAcquired := false
	defer func() {
		if stage.state == nil {
			if slotAcquired {
				<-m.stageSlots
			}
			preparation.finishStaging()
		}
	}()
	select {
	case m.stageSlots <- struct{}{}:
		slotAcquired = true
	case <-ctx.Done():
		return imageStaging{}, ctx.Err()
	}
	if err := m.lock(ctx); err != nil {
		return imageStaging{}, err
	}
	defer m.unlock()
	if bytes <= 0 || bytes > math.MaxInt64-m.staging {
		return imageStaging{}, errors.New("invalid image staging allocation")
	}
	if err := m.headroom(ctx, false); err != nil {
		return imageStaging{}, err
	}
	pending, err := m.loader.PendingBytes()
	if err != nil {
		return imageStaging{}, err
	}
	if pending > math.MaxInt64-m.staging-bytes || m.probing > math.MaxInt64-m.staging-bytes-pending {
		return imageStaging{}, errors.New("image staging allocation exceeds accounting range")
	}
	if err := requireImageImportSpace(m.fs, []string{m.stageRoot}, pending+m.staging+bytes+m.probing, m.cfg.ImageDiskMinFreeMB*imageMiB); err != nil {
		return imageStaging{}, err
	}
	m.staging += bytes
	return imageStaging{state: &imageStagingState{owner: m, bytes: bytes, preparation: preparation}}, nil
}

// imageUnpackAllocation owns the peak extraction allowance while a stopped
// probe is outstanding. Its durable inspection receipt retains the exclusion
// after an unknown outcome or a crash; only its live owner releases this charge.
type imageUnpackAllocation struct{ state *imageUnpackAllocationState }
type imageUnpackAllocationState struct {
	owner  *imageCapacityManager
	bytes  int64
	closed sync.Once
}

func (m *imageCapacityManager) reserveUnpack(ctx context.Context, bytes int64) (imageUnpackAllocation, error) {
	if err := m.lock(ctx); err != nil {
		return imageUnpackAllocation{}, err
	}
	defer m.unlock()
	if bytes <= 0 {
		return imageUnpackAllocation{}, errors.New("image unpack requires a verified allocation")
	}
	if err := m.importHeadroom(ctx, bytes); err != nil {
		return imageUnpackAllocation{}, err
	}
	m.probing += bytes
	return imageUnpackAllocation{state: &imageUnpackAllocationState{owner: m, bytes: bytes}}, nil
}

func (a imageUnpackAllocation) close() {
	a.state.closed.Do(func() {
		m := a.state.owner
		_ = m.lock(context.Background())
		m.probing -= a.state.bytes
		m.unlock()
	})
}

func (s imageStaging) close() {
	s.state.closed.Do(func() {
		m := s.state.owner
		_ = m.lock(context.Background())
		m.staging -= s.state.bytes
		m.unlock()
		<-m.stageSlots
		s.state.preparation.finishStaging()
	})
}
