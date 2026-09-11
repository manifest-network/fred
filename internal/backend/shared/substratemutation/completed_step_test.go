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
				receipt, err := runner.StepCompleted(ctx, "launch", func(context.Context) error { return nil })
				if err != nil {
					return err
				}
				escaped = receipt
				if err := ConsumeCompletedStep(receipt, "other", "launch"); err == nil {
					t.Fatal("foreign subject consumed receipt")
				}
				if err := ConsumeCompletedStep(receipt, subject, "helper"); err == nil {
					t.Fatal("other effect consumed receipt")
				}
				if err := ConsumeCompletedStep(receipt, subject, "launch"); err != nil {
					return err
				}
				if err := ConsumeCompletedStep(receipt, subject, "launch"); err == nil {
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
	if err := ConsumeCompletedStep(escaped, "target", "launch"); err == nil {
		t.Fatal("escaped receipt remained live")
	}
	if err := ConsumeCompletedStep(CompletedStep{}, "target", "launch"); err == nil {
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
						receipt, err := runner.StepCompleted(ctx, "launch", func(context.Context) error {
							if !postcheck {
								return errors.New("transport lost")
							}
							return nil
						})
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
				receipt, err := runner.StepCompleted(ctx, "launch", func(context.Context) error { return nil })
				if err != nil {
					return err
				}
				if err := runner.Step(ctx, "later start", func(context.Context) error { return errors.New("response lost") }); err == nil {
					t.Fatal("expected uncertain later step")
				}
				if err := ConsumeCompletedStep(receipt, subject, "launch"); err == nil {
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
				receipt, err := runner.StepCompleted(ctx, "launch", func(context.Context) error { return nil })
				if err != nil {
					t.Fatalf("successful launch did not mint completion: %v", err)
				}
				if err := ConsumeCompletedStep(receipt, subject, "launch"); err != nil {
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
