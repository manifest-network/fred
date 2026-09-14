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

// StepCompleted returns both the commit capability and the original causal
// bracket result. Keeping panic and dispatch evidence here lets every consumer
// use the same authorization/action/completion bracket without nesting another.
func (r Runner) StepCompleted(ctx context.Context, operation string, action func(context.Context) error) (CompletedStep, StepResult) {
	if r.session == nil || action == nil {
		return CompletedStep{}, StepResult{initialized: true, err: unavailable(operation)}
	}
	s := r.session
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return CompletedStep{}, StepResult{initialized: true, err: unavailable(operation)}
	}
	result := RunStep(ctx, operation, s.authorize, s.complete, action)
	if result.EffectEntered() {
		s.effectEntered = true
	}
	s.addIssue(result.Err())
	if result.Kind() != Attested {
		return CompletedStep{}, result
	}
	return CompletedStep{state: &completedStepState{session: s, operation: operation, issueGeneration: s.issueGeneration}}, result
}

// CommitCompletedStep holds the exact live receipt while committing its durable
// consequence. Only a successful commit consumes it, so a failed transaction
// can be retried within the same workflow without repeating the physical effect.
// commit must not reenter this execution's Runner. A storage implementation must
// independently withdraw its authority if its commit result is ambiguous.
func CommitCompletedStep[Subject comparable](receipt CompletedStep, subject Subject, operation string, commit func() error) error {
	if receipt.state == nil || receipt.state.session == nil || commit == nil {
		return errors.New("completed mutation receipt is unavailable")
	}
	state := receipt.state
	state.session.mu.Lock()
	defer state.session.mu.Unlock()
	if !state.session.active || state.consumed || state.session.issueGeneration != state.issueGeneration || state.session.subject != any(subject) || state.operation != operation {
		return errors.New("completed mutation receipt is stale, consumed or belongs to another effect")
	}
	if err := commit(); err != nil {
		return err
	}
	state.consumed = true
	return nil
}
