package docker

import (
	"context"
	"errors"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// diagnosticCallbackPublisher makes durable diagnostic publication a necessary
// predecessor of definitive failure settlement. Pending heads remain retryable
// if diagnostic persistence fails; successful captures are never reconstructed
// from a mutable actor projection or post-cleanup container read.
type diagnosticCallbackPublisher struct {
	callbackPublicationService
	diagnostics *shared.FailureDiagnostics
}

func newDiagnosticCallbackPublisher(publisher callbackPublicationService, diagnostics *shared.FailureDiagnostics) (callbackPublicationService, error) {
	if publisher == nil || diagnostics == nil {
		return nil, errors.New("diagnostic callback publisher requires journal publication and attempt diagnostics")
	}
	return &diagnosticCallbackPublisher{callbackPublicationService: publisher, diagnostics: diagnostics}, nil
}

func (publisher *diagnosticCallbackPublisher) PublishOperationFailureContext(ctx context.Context, proof shared.OperationReleaseUncommitted, message string) error {
	publication, err := publisher.diagnostics.OperationFailureContext(ctx, proof, shared.FailureDiagnosticObservation{
		Error: message, Message: message, Reason: backend.ReasonInternal, Status: shared.DiagnosticCaptureUnavailable,
	})
	if err != nil {
		return err
	}
	if err := publication.PublishContext(ctx, 1); err != nil {
		return err
	}
	snapshot, _, err := publication.Snapshot()
	if err != nil {
		return err
	}
	return publisher.callbackPublicationService.PublishOperationFailureContext(ctx, proof, diagnosticCallbackMessage(snapshot.Message, message))
}

func (publisher *diagnosticCallbackPublisher) prepareMaintenanceFailure(ctx context.Context, proof shared.MaintenanceReleaseFailure, message string) (string, error) {
	publication, err := publisher.diagnostics.MaintenanceFailureContext(ctx, proof, shared.FailureDiagnosticObservation{
		Error: message, Message: message, Reason: maintenanceFailureReason(proof.Intent().Kind()), Status: shared.DiagnosticCaptureUnavailable,
	})
	if err != nil {
		return "", err
	}
	if err := publication.PublishContext(ctx, 1); err != nil {
		return "", err
	}
	snapshot, _, err := publication.Snapshot()
	if err != nil {
		return "", err
	}
	return diagnosticCallbackMessage(snapshot.Message, message), nil
}

func (publisher *diagnosticCallbackPublisher) PublishMaintenanceFailureContext(ctx context.Context, proof shared.MaintenanceReleaseFailure, message string) error {
	message, err := publisher.prepareMaintenanceFailure(ctx, proof, message)
	if err != nil {
		return err
	}
	return publisher.callbackPublicationService.PublishMaintenanceFailureContext(ctx, proof, message)
}

func (publisher *diagnosticCallbackPublisher) TryPublishMaintenanceFailureContext(ctx context.Context, proof shared.MaintenanceReleaseFailure, message string) (bool, error) {
	if ctx == nil {
		return false, errors.New("diagnostic publication requires an ownership context")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	entry, acquired, err := publisher.diagnostics.TryPublishMaintenanceFailure(proof, shared.FailureDiagnosticObservation{
		Error: message, Message: message, Reason: maintenanceFailureReason(proof.Intent().Kind()), Status: shared.DiagnosticCaptureUnavailable,
	}, 1)
	if err != nil || !acquired {
		return acquired, err
	}
	return publisher.callbackPublicationService.TryPublishMaintenanceFailureContext(ctx, proof, diagnosticCallbackMessage(entry.Message, message))
}

func diagnosticCallbackMessage(captured, fallback string) string {
	message := captured
	if message == "" {
		message = fallback
	}
	if len(message) > callbackMaxErrorLen {
		message = message[:callbackMaxErrorLen-3] + "..."
	}
	return message
}
