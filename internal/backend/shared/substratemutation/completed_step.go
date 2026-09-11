package substratemutation

import (
	"context"
	"errors"
)

// CompletedStep is the causal receipt of one fully successful mutation bracket.
// Its zero value, copies after consumption and copies escaping the Guard's
// workflow grant no authority. A receipt is bound to the exact opaque subject
// and operation selected by the construction-bound facade.
type CompletedStep struct{ state *completedStepState }

type completedStepState struct {
	session         *session
	operation       string
	consumed        bool // protected by session.mu
	issueGeneration uint64
}

func (r Runner) StepCompleted(ctx context.Context, operation string, action func(context.Context) error) (CompletedStep, error) {
	if r.session == nil || action == nil {
		return CompletedStep{}, unavailable(operation)
	}
	s := r.session
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return CompletedStep{}, unavailable(operation)
	}
	result := RunStep(ctx, operation, s.authorize, s.complete, action)
	if result.EffectEntered() {
		s.effectEntered = true
	}
	s.addIssue(result.Err())
	if result.Kind() != Attested {
		return CompletedStep{}, result.Err()
	}
	return CompletedStep{state: &completedStepState{session: s, operation: operation, issueGeneration: s.issueGeneration}}, nil
}

// ConsumeCompletedStep consumes the receipt in the active workflow that owns
// it. The subject's comparable constraint excludes caller-built slices/maps,
// and comparison includes its private issuer/session-bound fields.
func ConsumeCompletedStep[Subject comparable](receipt CompletedStep, subject Subject, operation string) error {
	if receipt.state == nil || receipt.state.session == nil {
		return errors.New("completed mutation receipt is unavailable")
	}
	state := receipt.state
	state.session.mu.Lock()
	defer state.session.mu.Unlock()
	if !state.session.active || state.consumed || state.session.issueGeneration != state.issueGeneration || state.session.subject != any(subject) || state.operation != operation {
		return errors.New("completed mutation receipt is stale, consumed or belongs to another effect")
	}
	state.consumed = true
	return nil
}
