package substratemutation

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// findingCapability exposes one tenant Step and one Prepare to a workflow.
type findingCapability struct {
	step    func(context.Context, error) error
	prepare func(context.Context, error) error
}

// findingProbe records the Finding each classifier invocation received.
type findingProbe struct {
	mu   sync.Mutex
	seen []string
}

func (p *findingProbe) classify(_ context.Context, subject, finding string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = append(p.seen, finding)
	if finding != "" {
		return "failed:" + subject + ":" + finding, nil
	}
	return "ready:" + subject, nil
}

func (p *findingProbe) findings() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.seen...)
}

func newFindingExecutor(
	t *testing.T,
	protocol *Protocol[string],
	workflow func(context.Context, findingCapability, string) (string, error),
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

// A wholly successful session hands its finding to the classifier, which turns
// it into Attested evidence; any build, workflow, Step or Prepare error, or a
// panic, discards it, so the result is Ambiguous and the classifier sees none.
func TestFindingReachesClassifierOnlyFromAWhollySuccessfulSession(t *testing.T) {
	stepFailure := errors.New("remove failed")
	tests := []struct {
		name     string
		workflow func(context.Context, findingCapability, string) (string, error)
		want     Kind
		evidence string
	}{
		{"clean session with a finding", func(ctx context.Context, c findingCapability, _ string) (string, error) {
			return "exited", c.step(ctx, nil)
		}, Attested, "failed:lease:exited"},
		{"clean session without a finding", func(ctx context.Context, c findingCapability, _ string) (string, error) {
			return "", c.step(ctx, nil)
		}, Attested, "ready:lease"},
		{"step error the workflow returns", func(ctx context.Context, c findingCapability, _ string) (string, error) {
			return "exited", c.step(ctx, stepFailure)
		}, Ambiguous, ""},
		{"step error the workflow swallows", func(ctx context.Context, c findingCapability, _ string) (string, error) {
			_ = c.step(ctx, nil)
			_ = c.step(ctx, stepFailure)
			return "exited", nil
		}, Ambiguous, ""},
		{"prepare error the workflow swallows", func(ctx context.Context, c findingCapability, _ string) (string, error) {
			_ = c.prepare(ctx, stepFailure)
			return "exited", c.step(ctx, nil)
		}, Ambiguous, ""},
		{"workflow error after an effect", func(ctx context.Context, c findingCapability, _ string) (string, error) {
			_ = c.step(ctx, nil)
			return "exited", errors.New("unverified")
		}, Ambiguous, ""},
		{"panic after an effect", func(ctx context.Context, c findingCapability, _ string) (string, error) {
			_ = c.step(ctx, nil)
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

// Without an entered Step the result is Refused whatever the workflow
// reported, and the classifier is never consulted.
func TestFindingWithoutAnEffectIsRefused(t *testing.T) {
	p := NewProtocol[string]()
	probe := &findingProbe{}
	guard, _ := newFindingExecutor(t, p, func(context.Context, findingCapability, string) (string, error) {
		return "exited", nil
	}, probe)
	result := guard.Execute(begin(t, p, "lease"), context.Background())
	if result.Kind() != Refused {
		t.Fatalf("result = %s, want refused", result.Kind())
	}
	if len(probe.findings()) != 0 {
		t.Fatal("a refused session consulted the classifier")
	}
}

// Recovery never carries a finding: ExecuteRecovery discards the workflow's,
// and Inspect has none to give.
func TestRecoveryNeverSeesAFinding(t *testing.T) {
	p := NewProtocol[string]()
	probe := &findingProbe{}
	guard, attestor := newFindingExecutor(t, p, func(ctx context.Context, c findingCapability, _ string) (string, error) {
		return "exited", c.step(ctx, nil)
	}, probe)
	recovered := guard.ExecuteRecovery(recoverStarted(t, p, "lease"), context.Background())
	if evidence, _ := recovered.Evidence(); recovered.Kind() != Attested || evidence != "ready:lease" {
		t.Fatalf("recovery result = %s %q, %v", recovered.Kind(), evidence, recovered.Err())
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
		func(context.Context, findingCapability, string) (string, error) { return "", nil },
		nil,
	); err == nil {
		t.Fatal("nil classifier accepted")
	}
	if _, err := p.NewGuardBinding(); err != nil {
		t.Fatalf("invalid configuration consumed the binding: %v", err)
	}
}
