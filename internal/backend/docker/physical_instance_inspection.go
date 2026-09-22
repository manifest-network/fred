package docker

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// An inspection scope observes only the exact request made by the concrete SDK
// adapter. An arbitrary error implementing Docker's NotFound marker is not
// evidence that the configured daemon has lost this instance.
type instanceInspectionScope struct {
	mu         sync.Mutex
	observer   *daemonLaunchObserver
	instanceID string
	closed     bool
	absent     bool
}

type instanceInspectionContextKey struct{}

func (s *instanceInspectionScope) observe(observer *daemonLaunchObserver, request *http.Request, response *http.Response, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || observer == nil || observer != s.observer || request.Method != http.MethodGet {
		return
	}
	_, endpoint, ok := strings.Cut(request.URL.Path, "/containers/")
	if !ok || endpoint != s.instanceID+"/json" {
		return
	}
	s.absent = err == nil && response != nil && response.StatusCode == http.StatusNotFound
}

func (s *instanceInspectionScope) finish() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return s.absent
}

func (d *DockerClient) inspectInstance(ctx context.Context, instanceID string) (*leasesm.InstanceState, error) {
	scope := &instanceInspectionScope{observer: d.launchObserver, instanceID: instanceID}
	defer scope.finish()
	info, err := d.InspectContainer(context.WithValue(ctx, instanceInspectionContextKey{}, scope), instanceID)
	absent := scope.finish()
	if err != nil {
		if absent && ctx.Err() == nil {
			return &leasesm.InstanceState{Phase: leasesm.PhaseAbsent}, nil
		}
		return nil, err
	}
	return containerInfoToInstanceState(info), nil
}

// Keep this closure inside the read projection: adding an optional interface
// would let arbitrary clients claim to have observed a daemon's 404 response.
func concreteInstanceInspection(client dockerReadClient) func(context.Context, string) (*leasesm.InstanceState, error) {
	switch value := client.(type) {
	case *DockerClient:
		return value.inspectInstance
	case dockerReadView:
		return value.inspectInstance
	default:
		return nil
	}
}
