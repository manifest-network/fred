package docker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

var (
	startupGatedService = &manifest.Manifest{Image: "nginx:latest", HealthCheck: &manifest.HealthCheckConfig{
		Test: []string{"CMD-SHELL", "true"}, Retries: 1,
	}}
	startupPlainService = &manifest.Manifest{Image: "nginx:latest"}
)

// The live startup state table is total: every Docker status, health value
// and member fact, including empty and unknown statuses, has exactly one
// verdict. Only the workload's own account (an exit, a health verdict) or the
// daemon's refusal of a Start it was sent is a failure; a member that passed
// its health check is watched only for its exit.
func TestClassifyStartupInstance_IsTotal(t *testing.T) {
	healths := []HealthStatus{HealthStatusHealthy, HealthStatusUnhealthy, HealthStatusStarting, HealthStatusNone, "bogus"}
	want := func(status string, facts startupMemberFacts, health HealthStatus) startupInstanceVerdict {
		switch status {
		case "exited", "EXITED":
			return startupInstanceExited
		case "running":
			switch {
			case !facts.healthGated, facts.passedHealth, health == HealthStatusHealthy:
				return startupInstanceReady
			case health == HealthStatusUnhealthy:
				return startupInstanceUnhealthy
			default:
				return startupInstancePending
			}
		case "created", "restarting", "paused":
			if status == "created" && facts.startRefused {
				return startupInstanceStartRefused
			}
			if facts.healthGated {
				return startupInstancePending
			}
			return startupInstanceUnverified
		default:
			return startupInstanceUnverified
		}
	}
	for _, status := range []string{"exited", "EXITED", "running", "created", "restarting", "paused", "removing", "dead", "", "bogus"} {
		for _, gated := range []bool{false, true} {
			for _, passed := range []bool{false, true} {
				for _, refused := range []bool{false, true} {
					facts := startupMemberFacts{healthGated: gated, passedHealth: passed, startRefused: refused}
					for _, health := range healths {
						got := classifyStartupInstance(&ContainerInfo{Status: status, Health: health, ExitCode: 3}, facts)
						assert.Equal(t, want(status, facts, health), got, "status=%q facts=%+v health=%q", status, facts, health)
						assert.NotZero(t, got)
					}
				}
			}
		}
	}
	assert.Equal(t, startupInstanceUnverified, classifyStartupInstance(nil, startupMemberFacts{healthGated: true}),
		"no inspection is no fact")
}

func startupState(status string, health HealthStatus) func(context.Context, string) (*ContainerInfo, error) {
	return func(_ context.Context, id string) (*ContainerInfo, error) {
		return &ContainerInfo{ContainerID: id, Status: status, Health: health, ExitCode: 3}, nil
	}
}

// Each watch outcome carries the curated surface authored where it was
// observed (ENG-508, ENG-1125). Only an observed exit is ContainerExited;
// a health check that reported unhealthy or never passed is HealthCheckFailed;
// a failed read, a cancellation or a state that says nothing about the
// workload is unverified and Internal.
func TestWatchStartup_AuthorsCuratedSurface(t *testing.T) {
	unreadable := func(context.Context, string) (*ContainerInfo, error) {
		return nil, errors.New("docker daemon error")
	}
	tests := []struct {
		name         string
		service      *manifest.Manifest
		inspect      func(context.Context, string) (*ContainerInfo, error)
		observeFor   time.Duration
		parentFor    time.Duration
		cancel       bool
		wantVerdict  startupVerdict
		wantCallback string
		wantReason   backend.Reason
	}{
		{"exited during startup", startupPlainService, startupState("exited", HealthStatusNone), 0, 0, false,
			startupVerdictExited, backend.MsgContainerExitedDuringStartup, backend.ReasonContainerExited},
		{"dead during startup", startupPlainService, startupState("dead", HealthStatusNone), 0, 0, false,
			startupVerdictUnverified, backend.MsgStartupUnverified, backend.ReasonInternal},
		{"created during startup", startupPlainService, startupState("created", HealthStatusNone), 0, 0, false,
			startupVerdictUnverified, backend.MsgStartupUnverified, backend.ReasonInternal},
		{"startup inspect failure", startupPlainService, unreadable, 0, 0, false,
			startupVerdictUnverified, backend.MsgStartupUnverified, backend.ReasonInternal},
		{"startup canceled", startupPlainService, startupState("running", HealthStatusNone), 0, 0, true,
			startupVerdictUnverified, backend.MsgStartupCanceled, backend.ReasonInternal},
		{"exited during health check", startupGatedService, startupState("exited", HealthStatusNone), 0, 0, false,
			startupVerdictExited, backend.MsgContainerExitedDuringHealthCheck, backend.ReasonContainerExited},
		{"removed during health check", startupGatedService, startupState("removing", HealthStatusNone), 0, 0, false,
			startupVerdictUnverified, backend.MsgStartupUnverified, backend.ReasonInternal},
		{"unhealthy", startupGatedService, startupState("running", HealthStatusUnhealthy), 0, 0, false,
			startupVerdictUnhealthy, backend.MsgContainerUnhealthy, backend.ReasonHealthCheckFailed},
		{"never healthy before the observation deadline", startupGatedService,
			startupState("running", HealthStatusStarting), 5 * time.Second, 0, false,
			startupVerdictNeverHealthy, backend.MsgHealthCheckDeadline, backend.ReasonHealthCheckFailed},
		{"no health status before the observation deadline", startupGatedService,
			startupState("running", HealthStatusNone), 5 * time.Second, 0, false,
			startupVerdictNeverHealthy, backend.MsgHealthCheckDeadline, backend.ReasonHealthCheckFailed},
		{"never started before the observation deadline", startupGatedService,
			startupState("created", HealthStatusNone), 5 * time.Second, 0, false,
			startupVerdictUnverified, backend.MsgStartupUnverified, backend.ReasonInternal},
		{"parent deadline during a health wait", startupGatedService,
			startupState("running", HealthStatusStarting), 0, 5 * time.Second, false,
			startupVerdictUnverified, backend.MsgHealthCheckDeadline, backend.ReasonHealthCheckFailed},
		{"health inspect failure", startupGatedService, unreadable, 0, 0, false,
			startupVerdictUnverified, backend.MsgStartupUnverified, backend.ReasonInternal},
		{"health check canceled", startupGatedService, startupState("running", HealthStatusStarting), 0, 0, true,
			startupVerdictUnverified, backend.MsgStartupCanceled, backend.ReasonInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newBackendForTest(&mockDockerClient{
					InspectContainerFn: tt.inspect,
					ContainerLogsFn:    func(context.Context, string, int) (string, error) { return "", nil },
				}, nil)
				b.cfg.StartupVerifyDuration = time.Second
				parentFor := tt.parentFor
				if parentFor == 0 {
					parentFor = time.Hour
				}
				ctx, cancel := context.WithTimeout(t.Context(), parentFor)
				defer cancel()
				if tt.cancel {
					cancel()
				}
				var observeUntil time.Time
				if tt.observeFor > 0 {
					observeUntil = time.Now().Add(tt.observeFor)
				}
				cohort, err := newStartupCohort(&manifest.StackManifest{
					Services: map[string]*manifest.Manifest{"app": tt.service},
				}, map[string][]string{"app": {"c1"}})
				require.NoError(t, err)

				watch := b.watchStartup(ctx, cohort, newStartupMemory(settledLaunch{}), observeUntil, b.logger)
				assert.Equal(t, tt.wantVerdict, watch.verdict)
				require.NotNil(t, watch.surface)
				assert.Equal(t, tt.wantCallback, watch.surface.callback)
				assert.Equal(t, tt.wantReason, watch.surface.reason)
				if watch.failed() {
					assert.Equal(t, "c1", watch.container.id)
					require.NotNil(t, watch.info)
				}

				// A replacement's error-only form carries the observation but
				// never the provision surface, which would override its reason.
				flattened := flattenStartupWatch(watch)
				var physical *physicalOperationError
				assert.False(t, errors.As(flattened, &physical))
				assert.Contains(t, flattened.Error(), tt.wantCallback)
			})
		})
	}
}

// startupScript serves per-container inspection sequences: the n-th inspection
// of a container returns its n-th state, and the last state repeats.
type startupScript struct {
	mu     sync.Mutex
	states map[string][]ContainerInfo
	calls  map[string]int
}

func (s *startupScript) inspect(_ context.Context, id string) (*ContainerInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	states, ok := s.states[id]
	if !ok {
		return nil, errors.New("no such container")
	}
	call := s.calls[id]
	s.calls[id]++
	info := states[min(call, len(states)-1)]
	info.ContainerID = id
	return &info, nil
}

// The multi-service window (ENG-1125 added scope): a service that passed its
// own startup check and exits while another service still waits for its
// health check is seen, as a definite exit of that container, and never
// reaches Ready.
func TestWatchStartup_SiblingExitDuringLaterHealthWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		script := &startupScript{calls: make(map[string]int), states: map[string][]ContainerInfo{
			"db-1":  {{Status: "running", Health: HealthStatusStarting}},
			"web-1": {{Status: "running"}, {Status: "running"}, {Status: "exited", ExitCode: 1}},
		}}
		b := newBackendForTest(&mockDockerClient{
			InspectContainerFn: script.inspect,
			ContainerLogsFn:    func(context.Context, string, int) (string, error) { return "boom", nil },
		}, nil)
		b.cfg.StartupVerifyDuration = time.Second
		cohort, err := newStartupCohort(&manifest.StackManifest{Services: map[string]*manifest.Manifest{
			"db": startupGatedService, "web": startupPlainService,
		}}, map[string][]string{"db": {"db-1"}, "web": {"web-1"}})
		require.NoError(t, err)

		watch := b.watchStartup(t.Context(), cohort, newStartupMemory(settledLaunch{}), time.Time{}, b.logger)
		require.Equal(t, startupVerdictExited, watch.verdict)
		assert.Equal(t, startupContainer{id: "web-1", service: "web"}, watch.container)
		assert.Equal(t, backend.MsgContainerExitedDuringStartup, watch.surface.callback)
		assert.Contains(t, watch.surface.cause.Error(), "exit_code=1")
	})
}

// Ready comes only from one whole-cohort pass in which every container is
// ready: a container that became healthy and then exited before the others
// finished is a failure, not a Ready cohort.
func TestWatchStartup_ReadyRequiresOneWholeCohortPass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		script := &startupScript{calls: make(map[string]int), states: map[string][]ContainerInfo{
			"a": {{Status: "running", Health: HealthStatusHealthy}, {Status: "exited", ExitCode: 2}},
			"b": {{Status: "running", Health: HealthStatusStarting}, {Status: "running", Health: HealthStatusHealthy}},
		}}
		b := newBackendForTest(&mockDockerClient{
			InspectContainerFn: script.inspect,
			ContainerLogsFn:    func(context.Context, string, int) (string, error) { return "", nil },
		}, nil)
		cohort, err := newStartupCohort(&manifest.StackManifest{Services: map[string]*manifest.Manifest{
			"svc": startupGatedService,
		}}, map[string][]string{"svc": {"a", "b"}})
		require.NoError(t, err)

		watch := b.watchStartup(t.Context(), cohort, newStartupMemory(settledLaunch{}), time.Time{}, b.logger)
		require.Equal(t, startupVerdictExited, watch.verdict)
		assert.Equal(t, "a", watch.container.id)
		assert.Equal(t, backend.MsgContainerExitedDuringHealthCheck, watch.surface.callback)
	})

	synctest.Test(t, func(t *testing.T) {
		script := &startupScript{calls: make(map[string]int), states: map[string][]ContainerInfo{
			"a": {{Status: "running", Health: HealthStatusStarting}, {Status: "running", Health: HealthStatusHealthy}},
			"b": {{Status: "running"}},
		}}
		b := newBackendForTest(&mockDockerClient{InspectContainerFn: script.inspect}, nil)
		b.cfg.StartupVerifyDuration = 3 * time.Second
		cohort, err := newStartupCohort(&manifest.StackManifest{Services: map[string]*manifest.Manifest{
			"gated": startupGatedService, "plain": startupPlainService,
		}}, map[string][]string{"gated": {"a"}, "plain": {"b"}})
		require.NoError(t, err)

		start := time.Now()
		watch := b.watchStartup(t.Context(), cohort, newStartupMemory(settledLaunch{}), time.Time{}, b.logger)
		require.Equal(t, startupVerdictReady, watch.verdict)
		assert.GreaterOrEqual(t, time.Since(start), 3*time.Second,
			"a fixed-wait container is ready only after its settle period")
		script.mu.Lock()
		defer script.mu.Unlock()
		assert.Equal(t, script.calls["a"], script.calls["b"], "every pass inspects the whole cohort")
	})
}

// A health-gated member that reported healthy once is from then on watched
// only for its exit: a later unhealthy report while a sibling is still
// starting is a flap, as steady state treats it, not a startup failure. The
// replacement path shares the rule. An exit after healthy still fails.
func TestWatchStartup_HealthIsStickyOnceAMemberPassed(t *testing.T) {
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		"api": startupGatedService, "db": startupGatedService,
	}}
	services := map[string][]string{"api": {"api-1"}, "db": {"db-1"}}
	for _, flap := range []struct {
		name string
		api  []ContainerInfo
	}{
		{"healthy, unhealthy, healthy", []ContainerInfo{
			{Status: "running", Health: HealthStatusHealthy},
			{Status: "running", Health: HealthStatusUnhealthy},
			{Status: "running", Health: HealthStatusHealthy},
		}},
		{"healthy, then unhealthy for good", []ContainerInfo{
			{Status: "running", Health: HealthStatusHealthy},
			{Status: "running", Health: HealthStatusUnhealthy},
		}},
	} {
		flapping := func() *Backend {
			script := &startupScript{calls: make(map[string]int), states: map[string][]ContainerInfo{
				"api-1": flap.api,
				"db-1": {
					{Status: "running", Health: HealthStatusStarting},
					{Status: "running", Health: HealthStatusStarting},
					{Status: "running", Health: HealthStatusStarting},
					{Status: "running", Health: HealthStatusHealthy},
				},
			}}
			return newBackendForTest(&mockDockerClient{
				InspectContainerFn: script.inspect,
				ContainerLogsFn:    func(context.Context, string, int) (string, error) { return "", nil },
			}, nil)
		}
		t.Run(flap.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := flapping()
				cohort, err := newStartupCohort(stack, services)
				require.NoError(t, err)
				watch := b.watchStartup(t.Context(), cohort, newStartupMemory(settledLaunch{}), time.Time{}, b.logger)
				require.Equal(t, startupVerdictReady, watch.verdict, "a flap after healthy is not a startup failure")
			})
			synctest.Test(t, func(t *testing.T) {
				b := flapping()
				require.NoError(t, b.observeReplacementStartup(t.Context(), stack, services, b.logger),
					"a replacement's watch applies the same rule")
			})
		})
	}

	synctest.Test(t, func(t *testing.T) {
		script := &startupScript{calls: make(map[string]int), states: map[string][]ContainerInfo{
			"api-1": {{Status: "running", Health: HealthStatusHealthy}, {Status: "exited", ExitCode: 1}},
			"db-1":  {{Status: "running", Health: HealthStatusStarting}},
		}}
		b := newBackendForTest(&mockDockerClient{
			InspectContainerFn: script.inspect,
			ContainerLogsFn:    func(context.Context, string, int) (string, error) { return "", nil },
		}, nil)
		cohort, err := newStartupCohort(stack, services)
		require.NoError(t, err)
		watch := b.watchStartup(t.Context(), cohort, newStartupMemory(settledLaunch{}), time.Time{}, b.logger)
		require.Equal(t, startupVerdictExited, watch.verdict, "an exit after healthy still fails")
		assert.Equal(t, "api-1", watch.container.id)
	})

	// Without an earlier healthy report, unhealthy fails at once.
	synctest.Test(t, func(t *testing.T) {
		script := &startupScript{calls: make(map[string]int), states: map[string][]ContainerInfo{
			"api-1": {{Status: "running", Health: HealthStatusUnhealthy}},
			"db-1":  {{Status: "running", Health: HealthStatusStarting}},
		}}
		b := newBackendForTest(&mockDockerClient{
			InspectContainerFn: script.inspect,
			ContainerLogsFn:    func(context.Context, string, int) (string, error) { return "", nil },
		}, nil)
		cohort, err := newStartupCohort(stack, services)
		require.NoError(t, err)
		watch := b.watchStartup(t.Context(), cohort, newStartupMemory(settledLaunch{}), time.Time{}, b.logger)
		require.Equal(t, startupVerdictUnhealthy, watch.verdict)
	})
}

// A rejected launch exchange is observed once and fails definitely only on a
// positive account of one container: an exit wins over an unhealthy report,
// which wins over a Start the daemon refused on a container that never ran.
// A created container whose Start the daemon did not refuse, and a running
// one, show nothing.
func TestDecideStartupFailure_PrefersTheMostSpecificPositiveFact(t *testing.T) {
	member := func(id string, gated bool) startupContainer {
		return startupContainer{id: id, service: id, healthGated: gated}
	}
	pass := func(entries ...struct {
		member startupContainer
		info   ContainerInfo
		facts  startupMemberFacts
	}) startupPass {
		var p startupPass
		for _, entry := range entries {
			info := entry.info
			info.ContainerID = entry.member.id
			p.members = append(p.members, entry.member)
			p.infos = append(p.infos, &info)
			p.verdicts = append(p.verdicts, classifyStartupInstance(&info, entry.facts))
		}
		return p
	}
	type entry = struct {
		member startupContainer
		info   ContainerInfo
		facts  startupMemberFacts
	}
	refused := entry{member("refused", false), ContainerInfo{Status: "created"}, startupMemberFacts{startRefused: true}}
	created := entry{member("created", false), ContainerInfo{Status: "created"}, startupMemberFacts{}}
	running := entry{member("running", false), ContainerInfo{Status: "running"}, startupMemberFacts{}}
	unhealthy := entry{member("unhealthy", true), ContainerInfo{Status: "running", Health: HealthStatusUnhealthy},
		startupMemberFacts{healthGated: true}}
	exited := entry{member("exited", false), ContainerInfo{Status: "exited", ExitCode: 1}, startupMemberFacts{}}

	b := newBackendForTest(&mockDockerClient{
		ContainerLogsFn: func(context.Context, string, int) (string, error) { return "", nil },
	}, nil)
	for _, tt := range []struct {
		name    string
		pass    startupPass
		want    startupVerdict
		id      string
		reason  backend.Reason
		message string
	}{
		{"exit wins", pass(refused, unhealthy, exited, running), startupVerdictExited, "exited",
			backend.ReasonContainerExited, backend.MsgContainerExitedDuringStartup},
		{"unhealthy over a refused start", pass(refused, unhealthy, running), startupVerdictUnhealthy, "unhealthy",
			backend.ReasonHealthCheckFailed, backend.MsgContainerUnhealthy},
		{"a refused start", pass(created, refused, running), startupVerdictStartRefused, "refused",
			backend.ReasonContainerStartFailed, backend.MsgContainerStartRefused},
	} {
		t.Run(tt.name, func(t *testing.T) {
			watch, failed := b.decideStartupFailure(t.Context(), tt.pass)
			require.True(t, failed)
			assert.Equal(t, tt.want, watch.verdict)
			assert.Equal(t, tt.id, watch.container.id)
			assert.Equal(t, tt.reason, watch.surface.reason)
			assert.Equal(t, tt.message, watch.surface.callback)
		})
	}
	_, failed := b.decideStartupFailure(t.Context(), pass(created, running))
	assert.False(t, failed, "a created container whose Start was not refused shows nothing")
}

// A restart or update whose new cohort fails startup keeps its own reason:
// the replacement path flattens every startup observation, including a failed
// read that the provision path authors as Internal, into a plain error.
func TestReplacementStartupFailureKeepsTheMaintenanceReason(t *testing.T) {
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{"app": startupPlainService}}
	for _, tt := range []struct {
		name    string
		inspect func(context.Context, string) (*ContainerInfo, error)
	}{
		{"inspect error", func(context.Context, string) (*ContainerInfo, error) {
			return nil, errors.New("docker daemon error")
		}},
		{"exited", startupState("exited", HealthStatusNone)},
		{"dead", startupState("dead", HealthStatusNone)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newBackendForTest(&mockDockerClient{
					InspectContainerFn: tt.inspect,
					ContainerLogsFn:    func(context.Context, string, int) (string, error) { return "", nil },
				}, nil)
				cause := b.observeReplacementStartup(t.Context(), stack, map[string][]string{"app": {"c1"}}, b.logger)
				require.Error(t, cause)
				for kind, want := range map[shared.MaintenanceIntentKind]backend.Reason{
					shared.MaintenanceIntentRestart: backend.ReasonRestartFailed,
					shared.MaintenanceIntentUpdate:  backend.ReasonUpdateFailed,
				} {
					reason, callback := maintenanceFailureDetails(kind, cause)
					assert.Equal(t, want, reason, "%s keeps its own reason", kind)
					assert.Equal(t, string(kind)+" failed", callback)
				}
			})
		})
	}
	// A source-authored surface still wins, as an update's image pull does.
	reason, callback := maintenanceFailureDetails(shared.MaintenanceIntentUpdate, &physicalOperationError{
		callback: backend.MsgImagePullFailed, reason: backend.ReasonImagePullFailed, cause: errors.New("pull"),
	})
	assert.Equal(t, backend.ReasonImagePullFailed, reason)
	assert.Equal(t, backend.MsgImagePullFailed, callback)
}

// A provision keeps part of its deadline for the rollback of a definite
// failure: startup observation ends early enough to leave it.
func TestStartupObservationDeadline_KeepsTheRollbackReserve(t *testing.T) {
	now := time.Now()
	ctx, cancel := context.WithDeadline(t.Context(), now.Add(45*time.Minute))
	defer cancel()
	assert.Equal(t, now.Add(45*time.Minute-startupRollbackReserve), startupObservationDeadline(ctx, now))

	short, cancelShort := context.WithDeadline(t.Context(), now.Add(10*time.Second))
	defer cancelShort()
	assert.Equal(t, now.Add(5*time.Second), startupObservationDeadline(short, now),
		"a short budget keeps half of it in reserve")

	assert.True(t, startupObservationDeadline(t.Context(), now).IsZero(), "no deadline, no observation deadline")
}
