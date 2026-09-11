package docker

import (
	"context"
	"errors"
	"fmt"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// priorVolumeWriter is the fixed retirement of one inspected container from
// the launch subject's exact prior release. Current-target cleanup authority
// cannot construct it, even when generations share the same callback routes.
type priorVolumeWriter struct {
	mutations *storageMutations
	release   shared.Release
	id        string
}

func (m *storageMutations) priorVolumeWriter(ctx context.Context, id string) (priorVolumeWriter, error) {
	if m == nil || id == "" {
		return priorVolumeWriter{}, errors.New("volume writer requires exact launch authority")
	}
	var source shared.Release
	var ok bool
	switch {
	case m.compensationSubject.Valid():
		source, ok = m.compensationSubject.SourceRelease()
	case m.maintenanceSubject.Valid():
		source, ok = m.maintenanceSubject.SourceRelease()
	case m.operationSubject.Valid():
		source, ok = m.operationSubject.PredecessorRelease()
	}
	if !ok {
		return priorVolumeWriter{}, errors.New("launch has no admitted prior writer generation")
	}
	writer := priorVolumeWriter{mutations: m, release: source, id: id}
	if _, err := writer.inspect(ctx); err != nil {
		return priorVolumeWriter{}, err
	}
	return writer, nil
}

func (writer priorVolumeWriter) inspect(ctx context.Context) (*ContainerInfo, error) {
	m := writer.mutations
	if m == nil || writer.id == "" {
		return nil, errors.New("prior volume writer authority is unavailable")
	}
	info, err := m.ops.backend.docker.InspectContainer(ctx, writer.id)
	if err != nil {
		return nil, fmt.Errorf("inspect prior volume writer: %w", err)
	}
	if info == nil || info.ContainerID != writer.id {
		return nil, errors.New("prior volume writer identity changed")
	}
	if err := validateMaintenanceGenerationContainer(m.leaseUUID, writer.release.MaintenanceID,
		m.ops.backend.cfg.Name, writer.release, *info); err != nil {
		return nil, err
	}
	return info, nil
}
