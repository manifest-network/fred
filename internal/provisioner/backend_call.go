package provisioner

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
)

// publishProvisionStartingBestEffort contains application-owned event sinks at
// the observability boundary. Once an operation has entered Calling and its
// durable attempt exists, a sink panic must not suppress the backend dispatch
// or leave the operation wedged behind the call barrier.
func publishProvisionStartingBestEffort(
	sink ProvisionStartEventSink,
	leaseUUID, backendName string,
) {
	if sink == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
				metrics.LifecycleEventProvisionStarting,
			).Inc()
			slog.Error("provision-starting event sink panicked; continuing backend dispatch",
				"lease_uuid", leaseUUID,
				"backend", backendName,
				"event", metrics.LifecycleEventProvisionStarting,
				"panic", recovered,
				"stack", string(debug.Stack()),
			)
		}
	}()
	sink.PublishProvisionStarting(leaseUUID)
}

// invokeBackendProvision is the panic boundary shared by both provision
// application paths. A backend implementation is outside the lifecycle state
// machine's trust boundary: a panic is an ambiguous synchronous outcome, not
// permission to strand the operation in Calling or clear its durable attempt.
func invokeBackendProvision(
	ctx context.Context,
	backendClient backend.Backend,
	request backend.ProvisionRequest,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("backend Provision panicked",
				"lease_uuid", request.LeaseUUID,
				"backend", backendClient.Name(),
				"panic", recovered,
				"stack", string(debug.Stack()),
			)
			err = fmt.Errorf("backend Provision panicked: %v", recovered)
		}
	}()
	return backendClient.Provision(ctx, request)
}
