package substratemutation

import (
	"context"
	"errors"
)

// RecoveryAttestor is the sole restart-recovery Result minter. Its exhaustive
// classifier is fixed in the same construction transaction as the live Guard;
// recovery call sites provide only a typed Subject and cannot select a verdict.
type RecoveryAttestor[Subject comparable, Evidence any] struct {
	protocol  *Protocol[Subject]
	executor  *executorLineage
	authorize Authorize
	complete  Complete
	classify  func(context.Context, Subject) (Evidence, error)
}

func (a *RecoveryAttestor[Subject, Evidence]) validateAndConsume(execution RecoveryExecution[Subject]) error {
	proof := execution.proof
	if a == nil || a.protocol == nil || a.executor == nil || execution.recovery == nil ||
		!proof.valid() || proof.lineage != a.protocol.lineage ||
		proof.executor != a.executor || a.protocol.executor.Load() != a.executor {
		return errors.New("started authority belongs to another recovery attestor")
	}
	return proof.consume()
}

func callClassifier[Subject, Evidence any](
	classify func(context.Context, Subject) (Evidence, error),
	ctx context.Context,
	subject Subject,
	operation string,
) (evidence Evidence, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = panicError(operation, "strict substrate classifier", recovered)
		}
	}()
	return classify(ctx, subject)
}

func classifyBracket[Subject, Evidence any](
	authorize Authorize,
	complete Complete,
	classify func(context.Context, Subject) (Evidence, error),
	ctx context.Context,
	subject Subject,
	operation string,
) (Evidence, error) {
	classificationCtx, done, authorizeErr := callAuthorize(authorize, ctx, operation)
	if authorizeErr != nil {
		return *new(Evidence), errors.Join(authorizeErr, callDone(operation, done))
	}
	evidence, classificationErr := callClassifier(classify, classificationCtx, subject, operation)
	completionErr := callComplete(complete, classificationCtx, operation, classificationErr)
	releaseErr := callDone(operation, done)
	if err := errors.Join(classificationErr, completionErr, releaseErr); err != nil {
		return *new(Evidence), err
	}
	return evidence, nil
}

// Inspect classifies one exact typed subject after the original execution's
// process/call-stack boundary. Any classifier or identity-bracket failure is
// Ambiguous; only a complete strict read returns Attested evidence.
func (a *RecoveryAttestor[Subject, Evidence]) Inspect(
	execution RecoveryExecution[Subject],
	ctx context.Context,
) Result[Subject, Evidence] {
	proof := execution.proof
	subject := execution.subject
	if err := a.validateAndConsume(execution); err != nil {
		return invalidResult[Subject, Evidence](err)
	}
	evidence, err := classifyBracket(
		a.authorize, a.complete, a.classify, ctx, subject,
		"strict recovery substrate classification",
	)
	if err != nil {
		return ambiguousResult[Subject, Evidence](proof, subject, err)
	}
	return attestedResult(proof, subject, evidence)
}
