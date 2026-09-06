package substratemutation

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

type testCapability struct {
	down    func(context.Context) error
	prepare func(context.Context) error
}

func allow(ctx context.Context, _ string) (context.Context, func(), error) {
	return ctx, func() {}, nil
}

func completeOK(context.Context, string, error) error { return nil }

func newTestExecutor(
	t *testing.T,
	protocol *Protocol[string],
	authorize Authorize,
	complete Complete,
	action func(context.Context, string) error,
	classify func(context.Context, string) (string, error),
	workflows ...func(context.Context, testCapability, string) error,
) (*Guard[testCapability, string, string], *RecoveryAttestor[string, string]) {
	t.Helper()
	binding, err := protocol.NewGuardBinding()
	if err != nil {
		t.Fatalf("NewGuardBinding: %v", err)
	}
	workflow := func(ctx context.Context, capability testCapability, _ string) error {
		return capability.down(ctx)
	}
	if len(workflows) == 1 {
		workflow = workflows[0]
	} else if len(workflows) > 1 {
		t.Fatal("at most one test workflow may be supplied")
	}
	guard, attestor, err := NewExecutor(
		binding, authorize, complete,
		func(runner Runner, subject string) testCapability {
			invoke := func(ctx context.Context) error { return action(ctx, subject) }
			return testCapability{
				down: func(ctx context.Context) error {
					return runner.Step(ctx, "compose down", invoke)
				},
				prepare: func(ctx context.Context) error {
					return runner.Prepare(ctx, "pull image", invoke)
				},
			}
		},
		workflow,
		classify,
	)
	if err != nil {
		t.Fatalf("NewExecutor: %v", err)
	}
	return guard, attestor
}

func defaultExecutor(t *testing.T, protocol *Protocol[string]) (*Guard[testCapability, string, string], *RecoveryAttestor[string, string]) {
	t.Helper()
	return newTestExecutor(
		t, protocol, allow, completeOK,
		func(context.Context, string) error { return nil },
		func(_ context.Context, subject string) (string, error) { return "ready:" + subject, nil },
	)
}

func begin(t *testing.T, protocol *Protocol[string], subject string) LiveExecution[string] {
	t.Helper()
	started, err := protocol.BeginAfter(func() (string, error) { return subject, nil })
	if err != nil {
		t.Fatal(err)
	}
	return started
}

func recoverStarted(t *testing.T, protocol *Protocol[string], subject string) RecoveryExecution[string] {
	t.Helper()
	started, err := protocol.RecoverAfter(func() (string, error) { return subject, nil })
	if err != nil {
		t.Fatal(err)
	}
	return started
}

func executeDown(
	guard *Guard[testCapability, string, string],
	started LiveExecution[string],
) Result[string, string] {
	return guard.Execute(started, context.Background())
}

func requireAttested(t *testing.T, result Result[string, string], want string) {
	t.Helper()
	if result.Kind() != Attested || result.Err() != nil {
		t.Fatalf("result = %s, %v", result.Kind(), result.Err())
	}
	evidence, ok := result.Evidence()
	if !ok || evidence != want {
		t.Fatalf("evidence = %q, %v; want %q, true", evidence, ok, want)
	}
}

func TestProtocolBindsResultToExactGuardAndExecution(t *testing.T) {
	p := NewProtocol[string]()
	g, _ := defaultExecutor(t, p)
	started := begin(t, p, "lease-a")
	result := executeDown(g, started)
	requireAttested(t, result, "ready:lease-a")
	if err := ValidateLiveResult(p, started, result); err != nil {
		t.Fatalf("validate exact result: %v", err)
	}
	otherExecution := begin(t, p, "lease-b")
	if err := ValidateLiveResult(p, otherExecution, result); err == nil {
		t.Fatal("cross-operation result validated")
	}
	if _, err := p.NewGuardBinding(); err == nil {
		t.Fatal("second executor bound to one protocol")
	}

	otherProtocol := NewProtocol[string]()
	otherGuard, _ := defaultExecutor(t, otherProtocol)
	forged := executeDown(otherGuard, started)
	if err := ValidateLiveResult(p, started, forged); err == nil {
		t.Fatal("cross-executor result validated")
	}
}

func TestCapabilityTargetComesFromExactExecutionSubject(t *testing.T) {
	p := NewProtocol[string]()
	var target string
	g, _ := newTestExecutor(
		t, p, allow, completeOK,
		func(_ context.Context, subject string) error {
			target = subject
			return nil
		},
		func(_ context.Context, subject string) (string, error) { return "ready:" + subject, nil },
	)
	result := executeDown(g, begin(t, p, "lease-a"))
	requireAttested(t, result, "ready:lease-a")
	if target != "lease-a" {
		t.Fatalf("raw target = %q, want execution subject %q", target, "lease-a")
	}
}

func TestLiveStartedCopyCanExecuteOnlyOnce(t *testing.T) {
	p := NewProtocol[string]()
	var calls atomic.Int32
	g, _ := newTestExecutor(
		t, p, allow, completeOK,
		func(context.Context, string) error { calls.Add(1); return nil },
		func(context.Context, string) (string, error) { return "ready", nil },
	)
	started := begin(t, p, "lease")
	results := make(chan Result[string, string], 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func(authority LiveExecution[string]) {
			defer wg.Done()
			results <- executeDown(g, authority)
		}(started)
	}
	wg.Wait()
	close(results)
	var attested, invalid int
	for result := range results {
		switch result.Kind() {
		case Attested:
			attested++
		case Invalid:
			invalid++
		default:
			t.Fatalf("unexpected result %s: %v", result.Kind(), result.Err())
		}
	}
	if attested != 1 || invalid != 1 || calls.Load() != 1 {
		t.Fatalf("attested=%d invalid=%d raw calls=%d", attested, invalid, calls.Load())
	}
}

func TestLiveAndRecoveryAuthoritiesHaveDisjointAPIs(t *testing.T) {
	live := reflect.TypeFor[LiveExecution[string]]()
	recovery := reflect.TypeFor[RecoveryExecution[string]]()
	if live == recovery || live.ConvertibleTo(recovery) || recovery.ConvertibleTo(live) {
		t.Fatal("live and recovery authority types are interchangeable")
	}
	execute, ok := reflect.TypeFor[*Guard[testCapability, string, string]]().MethodByName("Execute")
	if !ok || execute.Type.In(1) != live {
		t.Fatalf("Guard.Execute authority = %v, want %v", execute.Type.In(1), live)
	}
	inspect, ok := reflect.TypeFor[*RecoveryAttestor[string, string]]().MethodByName("Inspect")
	if !ok || inspect.Type.In(1) != recovery {
		t.Fatalf("RecoveryAttestor.Inspect authority = %v, want %v", inspect.Type.In(1), recovery)
	}
}

func TestNilWorkflowIsRejectedBeforeBinding(t *testing.T) {
	p := NewProtocol[string]()
	binding, err := p.NewGuardBinding()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewExecutor[testCapability, string, string](
		binding, allow, completeOK,
		func(Runner, string) testCapability { return testCapability{} },
		nil,
		func(context.Context, string) (string, error) { return "ready", nil },
	); err == nil {
		t.Fatal("nil workflow accepted")
	}
	if _, err := p.NewGuardBinding(); err != nil {
		t.Fatalf("invalid workflow consumed binding: %v", err)
	}
}

func TestOnlyPreEntryFailureIsRefused(t *testing.T) {
	refusal := errors.New("authority refused")
	p := NewProtocol[string]()
	var raw, classified atomic.Int32
	g, _ := newTestExecutor(
		t, p,
		func(context.Context, string) (context.Context, func(), error) { return nil, nil, refusal },
		completeOK,
		func(context.Context, string) error { raw.Add(1); return nil },
		func(context.Context, string) (string, error) { classified.Add(1); return "ready", nil },
	)
	result := executeDown(g, begin(t, p, "lease"))
	if result.Kind() != Refused || !errors.Is(result.Err(), refusal) {
		t.Fatalf("result = %s, %v", result.Kind(), result.Err())
	}
	if raw.Load() != 0 || classified.Load() != 0 {
		t.Fatalf("raw=%d classified=%d", raw.Load(), classified.Load())
	}
}

func TestAuxiliaryFailureWithoutTenantEffectIsRefused(t *testing.T) {
	auxiliaryFailure := errors.New("image pull failed")
	p := NewProtocol[string]()
	var classified atomic.Int32
	g, _ := newTestExecutor(
		t, p, allow, completeOK,
		func(context.Context, string) error { return auxiliaryFailure },
		func(context.Context, string) (string, error) {
			classified.Add(1)
			return "ready", nil
		},
		func(ctx context.Context, capability testCapability, _ string) error {
			return capability.prepare(ctx)
		},
	)
	result := g.Execute(begin(t, p, "lease"), context.Background())
	if result.Kind() != Refused || !errors.Is(result.Err(), auxiliaryFailure) {
		t.Fatalf("result = %s, %v", result.Kind(), result.Err())
	}
	if classified.Load() != 0 {
		t.Fatalf("classifier calls = %d, want 0", classified.Load())
	}
}

func TestSwallowedAuxiliaryFailurePoisonsLaterTenantEffect(t *testing.T) {
	auxiliaryFailure := errors.New("image inspection failed")
	p := NewProtocol[string]()
	var calls atomic.Int32
	g, _ := newTestExecutor(
		t, p, allow, completeOK,
		func(context.Context, string) error {
			if calls.Add(1) == 1 {
				return auxiliaryFailure
			}
			return nil
		},
		func(context.Context, string) (string, error) { return "ready", nil },
		func(ctx context.Context, capability testCapability, _ string) error {
			_ = capability.prepare(ctx)
			return capability.down(ctx)
		},
	)
	result := g.Execute(begin(t, p, "lease"), context.Background())
	if result.Kind() != Ambiguous || !errors.Is(result.Err(), auxiliaryFailure) {
		t.Fatalf("result = %s, %v", result.Kind(), result.Err())
	}
}

func TestPostEntryErrorRemainsAmbiguousDespiteSuccessfulSameTurnClassification(t *testing.T) {
	rawFailure := errors.New("transport timed out")
	p := NewProtocol[string]()
	var classified atomic.Int32
	g, _ := newTestExecutor(
		t, p, allow, completeOK,
		func(context.Context, string) error { return rawFailure },
		func(context.Context, string) (string, error) { classified.Add(1); return "absent", nil },
	)
	result := executeDown(g, begin(t, p, "lease"))
	if result.Kind() != Ambiguous || !errors.Is(result.Err(), rawFailure) {
		t.Fatalf("result = %s, %v", result.Kind(), result.Err())
	}
	if classified.Load() != 1 {
		t.Fatalf("classifier calls = %d, want 1", classified.Load())
	}
	if _, ok := result.Evidence(); ok {
		t.Fatal("ambiguous result exposed diagnostic classifier evidence")
	}
}

func TestPostEntryPanicIsContainedAndAmbiguous(t *testing.T) {
	p := NewProtocol[string]()
	g, _ := newTestExecutor(
		t, p, allow, completeOK,
		func(context.Context, string) error { panic("boom") },
		func(context.Context, string) (string, error) { return "absent", nil },
	)
	result := executeDown(g, begin(t, p, "lease"))
	if result.Kind() != Ambiguous || result.Err() == nil {
		t.Fatalf("result = %s, %v", result.Kind(), result.Err())
	}
}

func TestEarlierEffectThenLaterRefusalIsAmbiguous(t *testing.T) {
	refusal := errors.New("authority withdrawn")
	checks := 0
	p := NewProtocol[string]()
	g, _ := newTestExecutor(
		t, p,
		func(ctx context.Context, _ string) (context.Context, func(), error) {
			checks++
			if checks == 2 {
				return nil, nil, refusal
			}
			return ctx, func() {}, nil
		},
		completeOK,
		func(context.Context, string) error { return nil },
		func(context.Context, string) (string, error) { return "ready", nil },
		func(ctx context.Context, capability testCapability, _ string) error {
			if err := capability.down(ctx); err != nil {
				return err
			}
			return capability.down(ctx)
		},
	)
	result := g.Execute(begin(t, p, "lease"), context.Background())
	if result.Kind() != Ambiguous || !errors.Is(result.Err(), refusal) {
		t.Fatalf("result = %s, %v", result.Kind(), result.Err())
	}
}

func TestLeakedCapabilityBecomesInert(t *testing.T) {
	p := NewProtocol[string]()
	var calls atomic.Int32
	var leaked testCapability
	g, _ := newTestExecutor(
		t, p, allow, completeOK,
		func(context.Context, string) error { calls.Add(1); return nil },
		func(context.Context, string) (string, error) { return "ready", nil },
		func(ctx context.Context, capability testCapability, _ string) error {
			leaked = capability
			return capability.down(ctx)
		},
	)
	result := g.Execute(begin(t, p, "lease"), context.Background())
	requireAttested(t, result, "ready")
	if err := leaked.down(context.Background()); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("leaked capability error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("raw calls = %d, want 1", calls.Load())
	}
}

func TestExecuteWaitsForRacingEnteredStepBeforeReturning(t *testing.T) {
	p := NewProtocol[string]()
	actionEntered := make(chan struct{})
	releaseAction := make(chan struct{})
	actionFinished := make(chan struct{})
	g, _ := newTestExecutor(
		t, p, allow, completeOK,
		func(context.Context, string) error {
			close(actionEntered)
			<-releaseAction
			close(actionFinished)
			return nil
		},
		func(context.Context, string) (string, error) { return "ready", nil },
		func(_ context.Context, capability testCapability, _ string) error {
			go func() { _ = capability.down(context.Background()) }()
			<-actionEntered
			return nil
		},
	)
	execution := begin(t, p, "lease")
	resultReady := make(chan Result[string, string], 1)
	go func() {
		resultReady <- g.Execute(execution, context.Background())
	}()
	<-actionEntered
	select {
	case result := <-resultReady:
		t.Fatalf("Execute returned while an entered step was running: %s, %v", result.Kind(), result.Err())
	default:
	}
	close(releaseAction)
	result := <-resultReady
	select {
	case <-actionFinished:
	default:
		t.Fatal("physical action completed after Execute returned")
	}
	requireAttested(t, result, "ready")
}

func TestNewExecutorValidatesClassifierBeforeBinding(t *testing.T) {
	p := NewProtocol[string]()
	binding, err := p.NewGuardBinding()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewExecutor[testCapability, string, string](
		binding, allow, completeOK,
		func(Runner, string) testCapability { return testCapability{} },
		func(context.Context, testCapability, string) error { return nil },
		nil,
	); err == nil {
		t.Fatal("nil classifier accepted")
	}
	binding, err = p.NewGuardBinding()
	if err != nil {
		t.Fatalf("binding consumed by invalid config: %v", err)
	}
	if _, _, err := NewExecutor(
		binding, allow, completeOK,
		func(Runner, string) testCapability { return testCapability{} },
		func(context.Context, testCapability, string) error { return nil },
		func(context.Context, string) (string, error) { return "ready", nil },
	); err != nil {
		t.Fatalf("valid executor after rejected config: %v", err)
	}
}

func TestRecoveryInspectIsOneShotAndReturnsBoundEvidence(t *testing.T) {
	p := NewProtocol[string]()
	_, attestor := defaultExecutor(t, p)
	started := recoverStarted(t, p, "lease")
	result := attestor.Inspect(started, context.Background())
	requireAttested(t, result, "ready:lease")
	if err := ValidateRecoveryResult(p, started, result); err != nil {
		t.Fatalf("validate recovery result: %v", err)
	}
	if second := attestor.Inspect(started, context.Background()); second.Kind() != Invalid {
		t.Fatalf("reused recovery authority = %s", second.Kind())
	}
}

func TestRecoveryIdentityWithdrawalBeforeDuringAndAfterClassification(t *testing.T) {
	withdrawn := errors.New("storage identity withdrawn")
	tests := []struct {
		name      string
		authorize Authorize
		complete  Complete
		classify  func(context.Context, string) (string, error)
	}{
		{
			name: "before",
			authorize: func(context.Context, string) (context.Context, func(), error) {
				return nil, nil, withdrawn
			},
			complete: completeOK,
			classify: func(context.Context, string) (string, error) {
				t.Fatal("classifier ran without authority")
				return "", nil
			},
		},
		{
			name:      "during",
			authorize: allow,
			complete:  completeOK,
			classify: func(context.Context, string) (string, error) {
				return "", withdrawn
			},
		},
		{
			name:      "after",
			authorize: allow,
			complete: func(context.Context, string, error) error {
				return withdrawn
			},
			classify: func(context.Context, string) (string, error) { return "ready", nil },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := NewProtocol[string]()
			_, attestor := newTestExecutor(
				t, p, test.authorize, test.complete,
				func(context.Context, string) error { return nil }, test.classify,
			)
			result := attestor.Inspect(recoverStarted(t, p, "lease"), context.Background())
			if result.Kind() != Ambiguous || !errors.Is(result.Err(), withdrawn) {
				t.Fatalf("result = %s, %v", result.Kind(), result.Err())
			}
		})
	}
}

func TestRecoveryBracketPanicsAreContained(t *testing.T) {
	tests := []struct {
		name      string
		authorize Authorize
		complete  Complete
		classify  func(context.Context, string) (string, error)
	}{
		{
			name: "authorize",
			authorize: func(context.Context, string) (context.Context, func(), error) {
				panic("authorize")
			},
			complete: completeOK,
			classify: func(context.Context, string) (string, error) { return "ready", nil },
		},
		{
			name: "classifier", authorize: allow, complete: completeOK,
			classify: func(context.Context, string) (string, error) { panic("classify") },
		},
		{
			name: "complete", authorize: allow,
			complete: func(context.Context, string, error) error { panic("complete") },
			classify: func(context.Context, string) (string, error) { return "ready", nil },
		},
		{
			name: "release",
			authorize: func(ctx context.Context, _ string) (context.Context, func(), error) {
				return ctx, func() { panic("release") }, nil
			},
			complete: completeOK,
			classify: func(context.Context, string) (string, error) { return "ready", nil },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := NewProtocol[string]()
			_, attestor := newTestExecutor(
				t, p, test.authorize, test.complete,
				func(context.Context, string) error { return nil }, test.classify,
			)
			result := attestor.Inspect(recoverStarted(t, p, "lease"), context.Background())
			if result.Kind() != Ambiguous || result.Err() == nil {
				t.Fatalf("result = %s, %v", result.Kind(), result.Err())
			}
		})
	}
}

func TestExecuteRecoveryCanAttestAlreadySatisfiedPostconditionWithoutMutation(t *testing.T) {
	p := NewProtocol[string]()
	var raw atomic.Int32
	g, _ := newTestExecutor(
		t, p, allow, completeOK,
		func(context.Context, string) error { raw.Add(1); return nil },
		func(_ context.Context, subject string) (string, error) { return "absent:" + subject, nil },
		func(context.Context, testCapability, string) error { return nil },
	)
	execution := recoverStarted(t, p, "lease-a")
	result := g.ExecuteRecovery(execution, context.Background())
	requireAttested(t, result, "absent:lease-a")
	if err := ValidateRecoveryResult(p, execution, result); err != nil {
		t.Fatalf("validate recovery result: %v", err)
	}
	if raw.Load() != 0 {
		t.Fatalf("raw mutations = %d, want 0", raw.Load())
	}
}

func TestExecuteRecoveryFailureBeforeTenantEffectIsRefused(t *testing.T) {
	preflightErr := errors.New("recovery input unavailable")
	p := NewProtocol[string]()
	var classified atomic.Int32
	g, _ := newTestExecutor(
		t, p, allow, completeOK,
		func(context.Context, string) error { t.Fatal("raw mutation ran"); return nil },
		func(context.Context, string) (string, error) {
			classified.Add(1)
			return "absent", nil
		},
		func(context.Context, testCapability, string) error { return preflightErr },
	)
	result := g.ExecuteRecovery(recoverStarted(t, p, "lease"), context.Background())
	if result.Kind() != Refused || !errors.Is(result.Err(), preflightErr) {
		t.Fatalf("result = %s, %v", result.Kind(), result.Err())
	}
	if classified.Load() != 0 {
		t.Fatalf("classifier calls = %d, want 0", classified.Load())
	}
}

func TestExecuteRecoveryPostEffectFailureCannotBecomeExactAbsence(t *testing.T) {
	mutationErr := errors.New("remove timed out")
	p := NewProtocol[string]()
	var classified atomic.Int32
	g, _ := newTestExecutor(
		t, p, allow, completeOK,
		func(context.Context, string) error { return mutationErr },
		func(context.Context, string) (string, error) {
			classified.Add(1)
			return "absent", nil
		},
	)
	result := g.ExecuteRecovery(recoverStarted(t, p, "lease"), context.Background())
	if result.Kind() != Ambiguous || !errors.Is(result.Err(), mutationErr) {
		t.Fatalf("result = %s, %v", result.Kind(), result.Err())
	}
	if classified.Load() != 1 {
		t.Fatalf("classifier calls = %d, want 1", classified.Load())
	}
	if _, ok := result.Evidence(); ok {
		t.Fatal("ambiguous recovery exposed absence as terminal evidence")
	}
}

func TestExecuteRecoveryBindsMutationTargetAndResultToExactSubject(t *testing.T) {
	p := NewProtocol[string]()
	var target string
	g, _ := newTestExecutor(
		t, p, allow, completeOK,
		func(_ context.Context, subject string) error {
			target = subject
			return nil
		},
		func(_ context.Context, subject string) (string, error) { return "absent:" + subject, nil },
	)
	execution := recoverStarted(t, p, "lease-a")
	result := g.ExecuteRecovery(execution, context.Background())
	requireAttested(t, result, "absent:lease-a")
	if target != "lease-a" {
		t.Fatalf("raw target = %q, want exact recovery subject %q", target, "lease-a")
	}
	other := recoverStarted(t, p, "lease-b")
	if err := ValidateRecoveryResult(p, other, result); err == nil {
		t.Fatal("cross-subject recovery result validated")
	}
	if second := g.ExecuteRecovery(execution, context.Background()); second.Kind() != Invalid {
		t.Fatalf("copied recovery authority executed twice: %s, %v", second.Kind(), second.Err())
	}
}
