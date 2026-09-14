package substratemutation

import (
	"context"
	"errors"
	"testing"
)

func TestCompletedStepBindsSubjectEffectAndActiveConsumption(t *testing.T) {
	p := NewProtocol[string]()
	binding, err := p.NewGuardBinding()
	if err != nil {
		t.Fatal(err)
	}
	var escaped CompletedStep
	guard, _, err := NewExecutor(binding, allow, completeOK,
		func(runner Runner, subject string) func(context.Context) error {
			return func(ctx context.Context) error {
				receipt, stepResult := runner.StepCompleted(ctx, "launch", func(context.Context) error { return nil })
				if err := stepResult.Err(); err != nil {
					return err
				}
				escaped = receipt
				if err := consumeCompletedStep(receipt, "other", "launch"); err == nil {
					t.Fatal("foreign subject consumed receipt")
				}
				if err := consumeCompletedStep(receipt, subject, "helper"); err == nil {
					t.Fatal("other effect consumed receipt")
				}
				if err := consumeCompletedStep(receipt, subject, "launch"); err != nil {
					return err
				}
				if err := consumeCompletedStep(receipt, subject, "launch"); err == nil {
					t.Fatal("copied receipt was reusable")
				}
				return nil
			}
		},
		func(ctx context.Context, run func(context.Context) error, _ string) error { return run(ctx) },
		func(context.Context, string) (string, error) { return "ready", nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	result := guard.Execute(begin(t, p, "target"), t.Context())
	if result.Kind() != Attested {
		t.Fatalf("execution: %s %v", result.Kind(), result.Err())
	}
	if err := consumeCompletedStep(escaped, "target", "launch"); err == nil {
		t.Fatal("escaped receipt remained live")
	}
	if err := consumeCompletedStep(CompletedStep{}, "target", "launch"); err == nil {
		t.Fatal("zero receipt granted authority")
	}
}

func TestCompletedStepDoesNotMintAfterActionOrCompletionFailure(t *testing.T) {
	for _, postcheck := range []bool{false, true} {
		t.Run(map[bool]string{false: "action", true: "postcheck"}[postcheck], func(t *testing.T) {
			p := NewProtocol[string]()
			binding, _ := p.NewGuardBinding()
			complete := completeOK
			if postcheck {
				complete = func(context.Context, string, error) error { return errors.New("withdrawn") }
			}
			guard, _, err := NewExecutor(binding, allow, complete,
				func(runner Runner, _ string) func(context.Context) error {
					return func(ctx context.Context) error {
						receipt, stepResult := runner.StepCompleted(ctx, "launch", func(context.Context) error {
							if !postcheck {
								return errors.New("transport lost")
							}
							return nil
						})
						err := stepResult.Err()
						if receipt.state != nil || err == nil {
							t.Fatal("uncertain step minted a completed receipt")
						}
						return err
					}
				}, func(ctx context.Context, run func(context.Context) error, _ string) error { return run(ctx) }, func(context.Context, string) (string, error) { return "ready", nil })
			if err != nil {
				t.Fatal(err)
			}
			if result := guard.Execute(begin(t, p, "target"), t.Context()); result.Kind() != Ambiguous {
				t.Fatalf("got %s", result.Kind())
			}
		})
	}
}

func TestCompletedStepCannotHideLaterUncertainEffect(t *testing.T) {
	p := NewProtocol[string]()
	binding, err := p.NewGuardBinding()
	if err != nil {
		t.Fatal(err)
	}
	guard, _, err := NewExecutor(binding, allow, completeOK,
		func(runner Runner, subject string) func(context.Context) error {
			return func(ctx context.Context) error {
				receipt, stepResult := runner.StepCompleted(ctx, "launch", func(context.Context) error { return nil })
				if err := stepResult.Err(); err != nil {
					return err
				}
				if err := runner.Step(ctx, "later start", func(context.Context) error { return errors.New("response lost") }); err == nil {
					t.Fatal("expected uncertain later step")
				}
				if err := consumeCompletedStep(receipt, subject, "launch"); err == nil {
					t.Fatal("earlier successful receipt concealed a later uncertain effect")
				}
				return nil
			}
		}, func(ctx context.Context, run func(context.Context) error, _ string) error { return run(ctx) }, func(context.Context, string) (string, error) { return "ready", nil })
	if err != nil {
		t.Fatal(err)
	}
	if result := guard.Execute(begin(t, p, "source"), t.Context()); result.Kind() != Ambiguous {
		t.Fatalf("got %s", result.Kind())
	}
}

func TestCompletedStepSettlesLaterSuccessWithoutErasingEarlierPreparationFailure(t *testing.T) {
	p := NewProtocol[string]()
	binding, err := p.NewGuardBinding()
	if err != nil {
		t.Fatal(err)
	}
	guard, _, err := NewExecutor(binding, allow, completeOK,
		func(runner Runner, subject string) func(context.Context) error {
			return func(ctx context.Context) error {
				if err := runner.Prepare(ctx, "optional inspection", func(context.Context) error { return errors.New("inspection unavailable") }); err == nil {
					t.Fatal("expected earlier preparation error")
				}
				receipt, stepResult := runner.StepCompleted(ctx, "launch", func(context.Context) error { return nil })
				if err := stepResult.Err(); err != nil {
					t.Fatalf("successful launch did not mint completion: %v", err)
				}
				if err := consumeCompletedStep(receipt, subject, "launch"); err != nil {
					t.Fatalf("earlier preparation error blocked causal launch completion: %v", err)
				}
				return nil
			}
		}, func(ctx context.Context, run func(context.Context) error, _ string) error { return run(ctx) }, func(context.Context, string) (string, error) { return "ready", nil })
	if err != nil {
		t.Fatal(err)
	}
	if result := guard.Execute(begin(t, p, "source"), t.Context()); result.Kind() != Ambiguous {
		t.Fatalf("successful launch receipt erased earlier workflow uncertainty: %s", result.Kind())
	}
}

// Tests which only check receipt ownership use a successful in-memory commit.
func consumeCompletedStep[Subject comparable](receipt CompletedStep, subject Subject, operation string) error {
	return CommitCompletedStep(receipt, subject, operation, func() error { return nil })
}

func TestCompletedStepFailedCommitCanRetryWithoutRepeatingEffect(t *testing.T) {
	p := NewProtocol[string]()
	binding, err := p.NewGuardBinding()
	if err != nil {
		t.Fatal(err)
	}
	effects, commits := 0, 0
	transient := errors.New("transaction refused before commit")
	guard, _, err := NewExecutor(binding, allow, completeOK,
		func(runner Runner, subject string) func(context.Context) error {
			return func(ctx context.Context) error {
				receipt, stepResult := runner.StepCompleted(ctx, "launch", func(context.Context) error { effects++; return nil })
				if err := stepResult.Err(); err != nil {
					return err
				}
				err := CommitCompletedStep(receipt, subject, "launch", func() error { commits++; return transient })
				if !errors.Is(err, transient) {
					t.Fatalf("failed commit: %v", err)
				}
				if err := CommitCompletedStep(receipt, subject, "launch", func() error { commits++; return nil }); err != nil {
					return err
				}
				if err := CommitCompletedStep(receipt, subject, "launch", func() error { commits++; return nil }); err == nil {
					t.Fatal("receipt reused after successful commit")
				}
				return nil
			}
		}, func(ctx context.Context, run func(context.Context) error, _ string) error { return run(ctx) },
		func(context.Context, string) (string, error) { return "ready", nil })
	if err != nil {
		t.Fatal(err)
	}
	result := guard.Execute(begin(t, p, "target"), t.Context())
	if result.Kind() != Attested || effects != 1 || commits != 2 {
		t.Fatalf("result=%s error=%v effects=%d commits=%d", result.Kind(), result.Err(), effects, commits)
	}
}

func TestCompletedStepPreservesSingleBracketPanicEvidence(t *testing.T) {
	for _, phase := range []string{"authorization", "action", "completion", "release"} {
		t.Run(phase, func(t *testing.T) {
			p := NewProtocol[string]()
			binding, err := p.NewGuardBinding()
			if err != nil {
				t.Fatal(err)
			}
			actions, completions, releases := 0, 0, 0
			authorize := func(ctx context.Context, operation string) (context.Context, func(), error) {
				if operation != "launch" {
					return ctx, func() {}, nil
				}
				if phase == "authorization" {
					panic("authorization failed")
				}
				return ctx, func() {
					releases++
					if phase == "release" {
						panic("release failed")
					}
				}, nil
			}
			complete := func(_ context.Context, operation string, _ error) error {
				if operation == "launch" {
					completions++
					if phase == "completion" {
						panic("completion failed")
					}
				}
				return nil
			}
			guard, _, err := NewExecutor(binding, authorize, complete,
				func(runner Runner, _ string) func(context.Context) error {
					return func(ctx context.Context) error {
						receipt, result := runner.StepCompleted(ctx, "launch", func(context.Context) error {
							actions++
							if phase == "action" {
								panic("action failed")
							}
							return nil
						})
						if receipt.state != nil || !result.Panicked() || result.EffectEntered() != (phase != "authorization") {
							t.Fatalf("lost exact bracket evidence: %+v", result)
						}
						return result.Err()
					}
				}, func(ctx context.Context, run func(context.Context) error, _ string) error { return run(ctx) },
				func(context.Context, string) (string, error) { return "observed", nil })
			if err != nil {
				t.Fatal(err)
			}
			_ = guard.Execute(begin(t, p, "helper"), t.Context())
			want := 1
			if phase == "authorization" {
				want = 0
			}
			if actions != want || completions != want || releases != want {
				t.Fatalf("bracket executed more than once: action=%d completion=%d release=%d", actions, completions, releases)
			}
		})
	}
}
