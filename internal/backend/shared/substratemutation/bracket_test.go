package substratemutation

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRunStepContainsPanicsAndPreservesTheEffectBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		authorizePanic   bool
		actionPanic      bool
		completionPanic  bool
		releasePanic     bool
		wantKind         Kind
		wantCompleteCall bool
		wantReleaseCall  bool
		wantPhase        string
	}{
		{
			name: "authorization is pre-effect", authorizePanic: true,
			wantKind: Refused, wantPhase: "authorization panicked",
		},
		{
			name: "action", actionPanic: true, wantKind: Ambiguous,
			wantCompleteCall: true, wantReleaseCall: true, wantPhase: "action panicked",
		},
		{
			name: "completion", completionPanic: true, wantKind: Ambiguous,
			wantCompleteCall: true, wantReleaseCall: true, wantPhase: "completion attestation panicked",
		},
		{
			name: "release", releasePanic: true, wantKind: Ambiguous,
			wantCompleteCall: true, wantReleaseCall: true, wantPhase: "authorization release panicked",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			completeCalled := false
			releaseCalled := false
			result := RunStep(
				context.Background(),
				"test mutation",
				func(ctx context.Context, _ string) (context.Context, func(), error) {
					if test.authorizePanic {
						panic("authorize boom")
					}
					return ctx, func() {
						releaseCalled = true
						if test.releasePanic {
							panic("release boom")
						}
					}, nil
				},
				func(_ context.Context, _ string, _ error) error {
					completeCalled = true
					if test.completionPanic {
						panic("complete boom")
					}
					return nil
				},
				func(context.Context) error {
					if test.actionPanic {
						panic("action boom")
					}
					return nil
				},
			)

			if result.Kind() != test.wantKind {
				t.Fatalf("kind = %s, want %s", result.Kind(), test.wantKind)
			}
			if !result.Panicked() {
				t.Fatal("panic was not retained in the bracket result")
			}
			if got := result.Err().Error(); !strings.Contains(got, test.wantPhase) {
				t.Fatalf("error %q does not contain phase %q", got, test.wantPhase)
			}
			if completeCalled != test.wantCompleteCall {
				t.Fatalf("completion called = %t, want %t", completeCalled, test.wantCompleteCall)
			}
			if releaseCalled != test.wantReleaseCall {
				t.Fatalf("release called = %t, want %t", releaseCalled, test.wantReleaseCall)
			}
		})
	}
}

func TestRunStepOrdinaryErrorIsNotMarkedAsPanic(t *testing.T) {
	t.Parallel()
	if kind := (StepResult{}).Kind(); kind != Invalid {
		t.Fatalf("zero StepResult kind = %s, want invalid", kind)
	}
	want := errors.New("retryable")
	result := RunStep(
		context.Background(),
		"test mutation",
		func(ctx context.Context, _ string) (context.Context, func(), error) {
			return ctx, func() {}, nil
		},
		func(_ context.Context, _ string, mutationErr error) error { return mutationErr },
		func(context.Context) error { return want },
	)
	if result.Kind() != Ambiguous {
		t.Fatalf("kind = %s, want ambiguous", result.Kind())
	}
	if result.Panicked() {
		t.Fatal("ordinary error was classified as a panic")
	}
	if !errors.Is(result.Err(), want) {
		t.Fatalf("error = %v, want cause %v", result.Err(), want)
	}
}
