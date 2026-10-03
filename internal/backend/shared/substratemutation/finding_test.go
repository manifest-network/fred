package substratemutation

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// findingCapability exposes one tenant Step, one Prepare and the session's
// Accept to a workflow.
type findingCapability struct {
	step    func(context.Context, error) error
	prepare func(context.Context, error) error
	accept  func(string) (Accepted[string], error)
}

// findingProbe records the Finding each classifier invocation received; ""
// stands for the absent value.
type findingProbe struct {
	mu   sync.Mutex
	seen []string
}

func (p *findingProbe) classify(_ context.Context, subject string, accepted Accepted[string]) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	finding, present := accepted.Finding()
	if !present {
		p.seen = append(p.seen, "")
		return "ready:" + subject, nil
	}
	p.seen = append(p.seen, finding)
	return "failed:" + subject + ":" + finding, nil
}

func (p *findingProbe) findings() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.seen...)
}

func newFindingExecutor(
	t *testing.T,
	protocol *Protocol[string],
	workflow func(context.Context, findingCapability, string) (Accepted[string], error),
	probe *findingProbe,
) (*Guard[findingCapability, string, string, string], *RecoveryAttestor[string, string]) {
	t.Helper()
	binding, err := protocol.NewGuardBinding()
	if err != nil {
		t.Fatal(err)
	}
	guard, attestor, err := NewFindingExecutor(binding, allow, completeOK,
		func(runner Runner, _ string) findingCapability {
			return findingCapability{
				step: func(ctx context.Context, result error) error {
					return runner.Step(ctx, "remove container", func(context.Context) error { return result })
				},
				prepare: func(ctx context.Context, result error) error {
					return runner.Prepare(ctx, "inspect image", func(context.Context) error { return result })
				},
				accept: func(finding string) (Accepted[string], error) { return Accept(runner, finding) },
			}
		},
		workflow,
		probe.classify,
	)
	if err != nil {
		t.Fatal(err)
	}
	return guard, attestor
}

// acceptOrPanic accepts a finding a test expects the session to accept.
func acceptOrPanic(c findingCapability, finding string) Accepted[string] {
	accepted, err := c.accept(finding)
	if err != nil {
		panic(err)
	}
	return accepted
}

// A wholly successful session hands its accepted finding to the classifier,
// which turns it into Attested evidence; any build, workflow, Step or Prepare
// error, before or after acceptance, or a panic, discards it, so the result is
// Ambiguous and the classifier sees the absent value.
func TestFindingReachesClassifierOnlyFromAWhollySuccessfulSession(t *testing.T) {
	stepFailure := errors.New("remove failed")
	tests := []struct {
		name     string
		workflow func(context.Context, findingCapability, string) (Accepted[string], error)
		want     Kind
		evidence string
	}{
		{"clean session with an accepted finding", func(ctx context.Context, c findingCapability, _ string) (Accepted[string], error) {
			accepted := acceptOrPanic(c, "exited")
			return accepted, c.step(ctx, nil)
		}, Attested, "failed:lease:exited"},
		{"clean session without a finding", func(ctx context.Context, c findingCapability, _ string) (Accepted[string], error) {
			return Accepted[string]{}, c.step(ctx, nil)
		}, Attested, "ready:lease"},
		{"step error after acceptance the workflow returns", func(ctx context.Context, c findingCapability, _ string) (Accepted[string], error) {
			accepted := acceptOrPanic(c, "exited")
			return accepted, c.step(ctx, stepFailure)
		}, Ambiguous, ""},
		{"step error after acceptance the workflow swallows", func(ctx context.Context, c findingCapability, _ string) (Accepted[string], error) {
			_ = c.step(ctx, nil)
			accepted := acceptOrPanic(c, "exited")
			_ = c.step(ctx, stepFailure)
			return accepted, nil
		}, Ambiguous, ""},
		{"workflow error after acceptance", func(ctx context.Context, c findingCapability, _ string) (Accepted[string], error) {
			_ = c.step(ctx, nil)
			return acceptOrPanic(c, "exited"), errors.New("unverified")
		}, Ambiguous, ""},
		{"panic after acceptance", func(ctx context.Context, c findingCapability, _ string) (Accepted[string], error) {
			_ = c.step(ctx, nil)
			_ = acceptOrPanic(c, "exited")
			panic("boom")
		}, Ambiguous, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := NewProtocol[string]()
			probe := &findingProbe{}
			guard, _ := newFindingExecutor(t, p, test.workflow, probe)
			result := guard.Execute(begin(t, p, "lease"), context.Background())
			if result.Kind() != test.want {
				t.Fatalf("result = %s, %v; want %s", result.Kind(), result.Err(), test.want)
			}
			evidence, ok := result.Evidence()
			if ok != (test.want == Attested) || evidence != test.evidence {
				t.Fatalf("evidence = %q, %v; want %q", evidence, ok, test.evidence)
			}
			seen := probe.findings()
			if len(seen) != 1 {
				t.Fatalf("classifier ran %d times, want once", len(seen))
			}
			if test.want != Attested && seen[0] != "" {
				t.Fatalf("an unsuccessful session handed finding %q to the classifier", seen[0])
			}
		})
	}
}

// Accept refuses once the session holds any issue, even one the workflow
// swallowed: the Guard would discard that finding, so whatever depends on it
// must not run. The refusal leaves nothing to report, and the session's own
// issue keeps the result Ambiguous.
func TestAcceptRefusesAfterAnEarlierIssue(t *testing.T) {
	failure := errors.New("detection failed")
	for _, test := range []struct {
		name   string
		poison func(context.Context, findingCapability)
	}{
		{"swallowed step error", func(ctx context.Context, c findingCapability) { _ = c.step(ctx, failure) }},
		{"swallowed prepare error", func(ctx context.Context, c findingCapability) { _ = c.prepare(ctx, failure) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := NewProtocol[string]()
			probe := &findingProbe{}
			var acceptErr error
			dependentWorkRan := false
			guard, _ := newFindingExecutor(t, p, func(ctx context.Context, c findingCapability, _ string) (Accepted[string], error) {
				_ = c.step(ctx, nil)
				test.poison(ctx, c)
				accepted, err := c.accept("exited")
				acceptErr = err
				if err == nil {
					dependentWorkRan = true
				}
				// A refusal returns the absent value; the session's own issue
				// keeps the result Ambiguous.
				return accepted, nil
			}, probe)
			result := guard.Execute(begin(t, p, "lease"), context.Background())
			if acceptErr == nil || !errors.Is(acceptErr, failure) {
				t.Fatalf("Accept after an issue = %v, want a refusal naming the issue", acceptErr)
			}
			if dependentWorkRan {
				t.Fatal("work depending on a refused finding ran")
			}
			if result.Kind() != Ambiguous {
				t.Fatalf("result = %s, want ambiguous", result.Kind())
			}
			if seen := probe.findings(); len(seen) != 1 || seen[0] != "" {
				t.Fatalf("classifier saw %q, want only the absent finding", seen)
			}
		})
	}
}

// A finding accepted by one execution cannot be reported by another: the
// second result is Ambiguous and its classifier sees no finding.
func TestFindingAcceptedByAnotherExecutionIsRejected(t *testing.T) {
	p := NewProtocol[string]()
	probe := &findingProbe{}
	var smuggled Accepted[string]
	first := true
	guard, _ := newFindingExecutor(t, p, func(ctx context.Context, c findingCapability, _ string) (Accepted[string], error) {
		if first {
			first = false
			smuggled = acceptOrPanic(c, "exited")
			return Accepted[string]{}, c.step(ctx, nil)
		}
		return smuggled, c.step(ctx, nil)
	}, probe)
	if result := guard.Execute(begin(t, p, "lease"), context.Background()); result.Kind() != Attested {
		t.Fatalf("first result = %s, %v", result.Kind(), result.Err())
	}
	second := guard.Execute(begin(t, p, "lease"), context.Background())
	if second.Kind() != Ambiguous || !strings.Contains(second.Err().Error(), "another execution") {
		t.Fatalf("second result = %s, %v; want ambiguous for a foreign finding", second.Kind(), second.Err())
	}
	for _, finding := range probe.findings() {
		if finding != "" {
			t.Fatalf("a foreign finding %q reached the classifier", finding)
		}
	}
}

// Accept is unavailable through an inert Runner: the zero Runner, and a
// Runner whose Execute call already returned.
func TestAcceptRequiresAnActiveRunner(t *testing.T) {
	if _, err := Accept(Runner{}, "exited"); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("Accept on the zero Runner = %v", err)
	}
	p := NewProtocol[string]()
	var escaped func(string) (Accepted[string], error)
	guard, _ := newFindingExecutor(t, p, func(ctx context.Context, c findingCapability, _ string) (Accepted[string], error) {
		escaped = c.accept
		return Accepted[string]{}, c.step(ctx, nil)
	}, &findingProbe{})
	if result := guard.Execute(begin(t, p, "lease"), context.Background()); result.Kind() != Attested {
		t.Fatalf("result = %s, %v", result.Kind(), result.Err())
	}
	if accepted, err := escaped("exited"); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("Accept after Execute returned = %v", err)
	} else if _, present := accepted.Finding(); present {
		t.Fatal("a refused Accept returned a present finding")
	}
}

// Without an entered Step the result is Refused whatever the workflow
// reported, and the classifier is never consulted.
func TestFindingWithoutAnEffectIsRefused(t *testing.T) {
	p := NewProtocol[string]()
	probe := &findingProbe{}
	guard, _ := newFindingExecutor(t, p, func(_ context.Context, c findingCapability, _ string) (Accepted[string], error) {
		return acceptOrPanic(c, "exited"), nil
	}, probe)
	result := guard.Execute(begin(t, p, "lease"), context.Background())
	if result.Kind() != Refused {
		t.Fatalf("result = %s, want refused", result.Kind())
	}
	if len(probe.findings()) != 0 {
		t.Fatal("a refused session consulted the classifier")
	}
}

// Recovery never carries a finding: its session refuses Accept, ExecuteRecovery
// discards whatever the workflow reports, and Inspect has none to give.
func TestRecoveryNeverSeesAFinding(t *testing.T) {
	p := NewProtocol[string]()
	probe := &findingProbe{}
	var recoveryAcceptErr error
	guard, attestor := newFindingExecutor(t, p, func(ctx context.Context, c findingCapability, _ string) (Accepted[string], error) {
		accepted, err := c.accept("exited")
		recoveryAcceptErr = err
		return accepted, c.step(ctx, nil)
	}, probe)
	recovered := guard.ExecuteRecovery(recoverStarted(t, p, "lease"), context.Background())
	if evidence, _ := recovered.Evidence(); recovered.Kind() != Attested || evidence != "ready:lease" {
		t.Fatalf("recovery result = %s %q, %v", recovered.Kind(), evidence, recovered.Err())
	}
	if recoveryAcceptErr == nil {
		t.Fatal("a recovery session accepted a finding")
	}
	inspected := attestor.Inspect(recoverStarted(t, p, "lease"), context.Background())
	if evidence, _ := inspected.Evidence(); inspected.Kind() != Attested || evidence != "ready:lease" {
		t.Fatalf("inspection result = %s %q, %v", inspected.Kind(), evidence, inspected.Err())
	}
	for _, finding := range probe.findings() {
		if finding != "" {
			t.Fatalf("recovery handed finding %q to the classifier", finding)
		}
	}
}

// Invalid finding-executor configuration is rejected before the one-shot
// binding is consumed.
func TestNilFindingWorkflowIsRejectedBeforeBinding(t *testing.T) {
	p := NewProtocol[string]()
	binding, err := p.NewGuardBinding()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewFindingExecutor[findingCapability, string, string, string](
		binding, allow, completeOK,
		func(Runner, string) findingCapability { return findingCapability{} },
		nil,
		(&findingProbe{}).classify,
	); err == nil {
		t.Fatal("nil workflow accepted")
	}
	if _, _, err := NewFindingExecutor[findingCapability, string, string, string](
		binding, allow, completeOK,
		func(Runner, string) findingCapability { return findingCapability{} },
		func(context.Context, findingCapability, string) (Accepted[string], error) {
			return Accepted[string]{}, nil
		},
		nil,
	); err == nil {
		t.Fatal("nil classifier accepted")
	}
	if _, err := p.NewGuardBinding(); err != nil {
		t.Fatalf("invalid configuration consumed the binding: %v", err)
	}
}
