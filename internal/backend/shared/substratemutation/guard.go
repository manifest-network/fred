package substratemutation

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrCapabilityUnavailable = errors.New("substrate mutation capability is unavailable")

type Authorize func(context.Context, string) (context.Context, func(), error)
type Complete func(context.Context, string, error) error

// StepResult is the panic-safe causal result of one authorization/action/
// completion/release bracket. It deliberately carries no mutation capability:
// callers must still close RunStep over a construction-bound narrow writer.
//
// Once EffectEntered reports true, an error can no longer prove that the
// substrate saw no effect. Panicked distinguishes broken control flow from an
// ordinary, operation-specific error so idempotent background convergence may
// remain retryable while still fail-stopping on a panic.
type StepResult struct {
	initialized   bool
	effectEntered bool
	panicked      bool
	err           error
}

// EffectEntered reports whether RunStep crossed the physical action boundary.
func (r StepResult) EffectEntered() bool { return r.effectEntered }

// Panicked reports whether any bracket phase recovered a panic.
func (r StepResult) Panicked() bool { return r.panicked }

// Err returns the joined action, completion, and release failures.
func (r StepResult) Err() error { return r.err }

// Kind uses the same causal vocabulary as Guard results: authorization failure
// is Refused, a clean entered step is Attested, and any entered-step issue is
// Ambiguous. It does not perform final domain classification or mint Evidence.
func (r StepResult) Kind() Kind {
	if !r.initialized {
		return Invalid
	}
	if !r.effectEntered {
		return Refused
	}
	if r.err != nil {
		return Ambiguous
	}
	return Attested
}

// Runner is the only primitive from which a backend may build a physical
// mutation capability. A zero Runner is inert. A live Runner is scoped to one
// Execute call and becomes permanently inert when that call returns.
//
// Backends capture Runner in private, narrow facade closures. The facade type
// passed as T then defines, at compile time, the complete set of mutations a
// workflow may perform. Runner never contains or returns a raw writer.
type Runner struct {
	session *session
}

func unavailable(operation string) error {
	return fmt.Errorf("%s: %w", operation, ErrCapabilityUnavailable)
}

// Step brackets one potentially effectful call with admission and completion
// attestation. Calling it outside the Execute invocation which minted the
// Runner cannot invoke action.
func (r Runner) Step(ctx context.Context, operation string, action func(context.Context) error) error {
	if r.session == nil {
		return unavailable(operation)
	}
	return r.session.perform(ctx, operation, action, true)
}

// Prepare brackets an auxiliary action that cannot change tenant substrate,
// such as pulling or inspecting an image. Its errors remain part of the causal
// session and make any later tenant Step ambiguous, but without a later Step
// they produce Refused rather than pretending tenant state may have changed.
func (r Runner) Prepare(ctx context.Context, operation string, action func(context.Context) error) error {
	if r.session == nil {
		return unavailable(operation)
	}
	return r.session.perform(ctx, operation, action, false)
}

// EffectEntered reports whether this exact live execution has crossed at
// least one tenant/substrate Step. It lets a construction-bound workflow make
// cleanup decisions from the protocol's single causal fact instead of keeping
// a second boolean which could drift from the Runner taxonomy.
func (r Runner) EffectEntered() bool {
	if r.session == nil {
		return false
	}
	r.session.mu.Lock()
	defer r.session.mu.Unlock()
	return r.session.active && r.session.effectEntered
}

// Guard is an exact settlement-bound executor for one narrow backend-defined
// capability T. The builder is the only code that sees Runner, and both the
// builder and workflow are fixed when the executor is constructed. Callers can
// therefore neither select a different handler nor inject physical targets at
// Execute time.
type Guard[T any, Subject comparable, Evidence any] struct {
	protocol  *Protocol[Subject]
	executor  *executorLineage
	authorize Authorize
	complete  Complete
	build     func(Runner, Subject) T
	run       func(context.Context, T, Subject) error
	classify  func(context.Context, Subject) (Evidence, error)
}

// NewExecutor atomically binds the live Guard, its workflow, and its recovery
// attestor to the same Protocol executor lineage. All configuration is checked
// before the one-shot binding is consumed. Live workflows and restart recovery
// therefore cannot be wired to independently swappable physical authorities.
func NewExecutor[T any, Subject comparable, Evidence any](
	binding GuardBinding[Subject],
	authorize Authorize,
	complete Complete,
	build func(Runner, Subject) T,
	run func(context.Context, T, Subject) error,
	classify func(context.Context, Subject) (Evidence, error),
) (*Guard[T, Subject, Evidence], *RecoveryAttestor[Subject, Evidence], error) {
	protocol := binding.protocol
	if protocol == nil || protocol.lineage == nil {
		return nil, nil, errors.New("substrate mutation protocol is required")
	}
	if authorize == nil {
		return nil, nil, errors.New("storage mutation authorizer is required")
	}
	if complete == nil {
		return nil, nil, errors.New("storage mutation completion verifier is required")
	}
	if build == nil {
		return nil, nil, errors.New("substrate mutation capability builder is required")
	}
	if run == nil {
		return nil, nil, errors.New("substrate mutation workflow is required")
	}
	if classify == nil {
		return nil, nil, errors.New("strict exhaustive substrate classifier is required")
	}
	executor := &executorLineage{}
	if !protocol.executor.CompareAndSwap(nil, executor) {
		return nil, nil, errors.New("substrate mutation protocol already has a bound executor")
	}
	guard := &Guard[T, Subject, Evidence]{
		protocol: protocol, executor: executor, authorize: authorize,
		complete: complete, build: build, run: run, classify: classify,
	}
	attestor := &RecoveryAttestor[Subject, Evidence]{
		protocol: protocol, executor: executor,
		authorize: authorize, complete: complete, classify: classify,
	}
	return guard, attestor, nil
}

// session aggregates the complete workflow history. A caller cannot launder
// an earlier physical effect by returning the NoEffect result of a later
// refused step: once any action is entered, every error makes the workflow
// Ambiguous.
type session struct {
	subject         any // always the Guard's comparable, opaque subject
	mu              sync.Mutex
	active          bool
	effectEntered   bool
	issues          error
	issueGeneration uint64
	authorize       Authorize
	complete        Complete
}

func (s *session) addIssue(err error) {
	if err != nil {
		s.issues = errors.Join(s.issues, err)
		s.issueGeneration++
	}
}

type bracketPanicError struct {
	operation string
	phase     string
	recovered any
}

func (e *bracketPanicError) Error() string {
	return fmt.Sprintf("%s %s panicked: %v", e.operation, e.phase, e.recovered)
}

func panicError(operation, phase string, recovered any) error {
	return &bracketPanicError{operation: operation, phase: phase, recovered: recovered}
}

func callAuthorize(
	authorize Authorize,
	ctx context.Context,
	operation string,
) (mutationCtx context.Context, done func(), err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = panicError(operation, "authorization", recovered)
		}
	}()
	mutationCtx, done, err = authorize(ctx, operation)
	if err == nil && mutationCtx == nil {
		err = errors.New("storage mutation authorizer returned a nil context")
	}
	return mutationCtx, done, err
}

func callAction(ctx context.Context, operation string, action func(context.Context) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = panicError(operation, "action", recovered)
		}
	}()
	return action(ctx)
}

func callComplete(
	complete Complete,
	ctx context.Context,
	operation string,
	mutationErr error,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = panicError(operation, "completion attestation", recovered)
		}
	}()
	return complete(ctx, operation, mutationErr)
}

func callDone(operation string, done func()) (err error) {
	if done == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = panicError(operation, "authorization release", recovered)
		}
	}()
	done()
	return nil
}

// RunStep executes exactly one potentially effectful call through the same
// panic-safe bracket used by Guard.Runner. Completion and release are attempted
// even when the action or completion panics. A panic in authorization remains
// pre-effect; a panic in action, completion, or release is post-effect and is
// therefore reported as an Ambiguous StepResult.
func RunStep(
	ctx context.Context,
	operation string,
	authorize Authorize,
	complete Complete,
	action func(context.Context) error,
) StepResult {
	if authorize == nil {
		return StepResult{initialized: true, err: unavailable(operation + " authorization")}
	}
	if complete == nil {
		return StepResult{initialized: true, err: unavailable(operation + " completion")}
	}
	if action == nil {
		return StepResult{initialized: true, err: unavailable(operation)}
	}

	mutationCtx, done, authorizeErr := callAuthorize(authorize, ctx, operation)
	if authorizeErr != nil {
		err := errors.Join(authorizeErr, callDone(operation, done))
		var panicErr *bracketPanicError
		return StepResult{initialized: true, panicked: errors.As(err, &panicErr), err: err}
	}

	// Crossing this instruction means the action may have reached the substrate;
	// neither a return value nor a same-turn observation can move the causal
	// boundary back to Refused.
	mutationErr := callAction(mutationCtx, operation, action)
	completionErr := callComplete(complete, mutationCtx, operation, mutationErr)
	releaseErr := callDone(operation, done)
	err := errors.Join(mutationErr, completionErr, releaseErr)
	var panicErr *bracketPanicError
	return StepResult{
		initialized:   true,
		effectEntered: true,
		panicked:      errors.As(err, &panicErr),
		err:           err,
	}
}

func (s *session) perform(
	ctx context.Context,
	operation string,
	action func(context.Context) error,
	tenantEffect bool,
) error {
	if s == nil || action == nil {
		return unavailable(operation)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return unavailable(operation)
	}

	result := RunStep(ctx, operation, s.authorize, s.complete, action)
	if tenantEffect && result.EffectEntered() {
		s.effectEntered = true
	}
	s.addIssue(result.Err())
	return result.Err()
}

func callBuild[T any, Subject comparable](
	build func(Runner, Subject) T,
	runner Runner,
	subject Subject,
) (capability T, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("physical mutation capability builder panicked: %v", recovered)
		}
	}()
	return build(runner, subject), nil
}

func callWorkflow[T any, Subject comparable](
	workflow func(context.Context, T, Subject) error,
	ctx context.Context,
	capability T,
	subject Subject,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("physical mutation workflow panicked: %v", recovered)
		}
	}()
	return workflow(ctx, capability, subject)
}

// Execute is the sole live Result minter. It invokes only the construction-bound
// workflow with the subject-bound narrow capability T and aggregates every Step
// invoked through it. Attested requires an entered mutation, a nil workflow
// result, and a strict final classification. Any failure after entry is
// Ambiguous.
func (g *Guard[T, Subject, Evidence]) Execute(
	execution LiveExecution[Subject],
	ctx context.Context,
) Result[Subject, Evidence] {
	proof := execution.proof
	subject := execution.subject
	if g == nil || g.protocol == nil || execution.live == nil || !proof.valid() ||
		proof.lineage != g.protocol.lineage || proof.executor != g.executor {
		return invalidResult[Subject, Evidence](errors.New("started physical mutation authority belongs to another executor"))
	}
	if err := proof.consume(); err != nil {
		return invalidResult[Subject, Evidence](err)
	}
	s := &session{active: true, authorize: g.authorize, complete: g.complete, subject: subject}
	capability, buildErr := callBuild(g.build, Runner{session: s}, subject)
	var workflowErr error
	if buildErr == nil {
		workflowErr = callWorkflow(g.run, ctx, capability, subject)
	}

	s.mu.Lock()
	s.active = false
	entered := s.effectEntered
	issues := s.issues
	s.mu.Unlock()

	causalErr := errors.Join(buildErr, workflowErr, issues)
	if !entered {
		if causalErr == nil {
			causalErr = errors.New("physical mutation workflow performed no mutation")
		}
		return refusedResult[Subject, Evidence](proof, subject, causalErr)
	}

	evidence, classificationErr := classifyBracket(
		g.authorize, g.complete, g.classify, ctx, subject,
		"live workflow final substrate classification",
	)
	// A same-turn observation cannot prove absence after an uncertain remote
	// call: the delayed call may commit after the read. The classifier still
	// runs to detect identity withdrawal and aid diagnosis, but only a wholly
	// successful workflow can mint Attested evidence. A successful rollback is
	// represented by an Evidence variant and a nil workflow result.
	if err := errors.Join(causalErr, classificationErr); err != nil {
		return ambiguousResult[Subject, Evidence](proof, subject, err)
	}
	return attestedResult(proof, subject, evidence)
}

// ExecuteRecovery runs the construction-bound workflow for an exact subject
// returned by a durable recovery re-read. Unlike Execute, a nil workflow which
// enters no Step is allowed to complete from classifier evidence: cleanup is
// idempotent, so an already-absent target is a successful recovery postcondition,
// not a new synchronous refusal. A workflow error before the first Step remains
// Refused, while any error after a Step remains Ambiguous.
func (g *Guard[T, Subject, Evidence]) ExecuteRecovery(
	execution RecoveryExecution[Subject],
	ctx context.Context,
) Result[Subject, Evidence] {
	proof := execution.proof
	subject := execution.subject
	if g == nil || g.protocol == nil || execution.recovery == nil || !proof.valid() ||
		proof.lineage != g.protocol.lineage || proof.executor != g.executor {
		return invalidResult[Subject, Evidence](errors.New("recovered physical mutation authority belongs to another executor"))
	}
	if err := proof.consume(); err != nil {
		return invalidResult[Subject, Evidence](err)
	}
	s := &session{active: true, authorize: g.authorize, complete: g.complete, subject: subject}
	capability, buildErr := callBuild(g.build, Runner{session: s}, subject)
	var workflowErr error
	if buildErr == nil {
		workflowErr = callWorkflow(g.run, ctx, capability, subject)
	}

	s.mu.Lock()
	s.active = false
	entered := s.effectEntered
	issues := s.issues
	s.mu.Unlock()

	causalErr := errors.Join(buildErr, workflowErr, issues)
	if !entered && causalErr != nil {
		return refusedResult[Subject, Evidence](proof, subject, causalErr)
	}
	evidence, classificationErr := classifyBracket(
		g.authorize, g.complete, g.classify, ctx, subject,
		"recovery workflow final substrate classification",
	)
	if err := errors.Join(causalErr, classificationErr); err != nil {
		return ambiguousResult[Subject, Evidence](proof, subject, err)
	}
	return attestedResult(proof, subject, evidence)
}
