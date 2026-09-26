package imagefetch

import (
	"errors"
	"log/slog"

	"github.com/manifest-network/fred/internal/backend/shared/imagebudget"
)

// admitPreparedBudget is the only transition from measured preparation usage
// to import authority. Refusal and pressure observations use the same owned
// ceiling as admission; no caller-side estimate decides the outcome.
func (l *Loader) admitPreparedBudget(source string, verificationBytes, importBytes int64) (imagebudget.Budget, error) {
	ceiling := 2 * l.budget.Bytes()
	if importBytes > ceiling {
		imagePreparationRefusals.WithLabelValues("import_allocation").Inc()
		slog.Warn("image import allocation exceeds admission limit", "source", source, "import_bytes", importBytes, "limit_bytes", ceiling, "reason", "import_allocation")
		return imagebudget.Budget{}, errors.New("image import allocation exceeds twice the image byte limit")
	}
	verification, err := imagebudget.NewVerificationBudget(verificationBytes)
	if err != nil {
		return imagebudget.Budget{}, err
	}
	budget, err := imagebudget.Verified(verification, importBytes)
	if err != nil {
		return imagebudget.Budget{}, err
	}
	// Division before multiplication keeps the comparison safe at the largest
	// admitted ceiling. The first integer above 80% must warn, including when
	// the ceiling is not divisible by five.
	threshold := ceiling/5*4 + ceiling%5*4/5
	// Saved recovery budgets deliberately fit the exact content. Their 100%
	// utilization says nothing about growth under current new-image policy.
	if l.kind == newImagePreparation && importBytes > threshold {
		imageAllocationPressure.Inc()
		slog.Warn("image import allocation has limited growth headroom", "source", source, "import_bytes", importBytes, "limit_bytes", ceiling)
	}
	return budget, nil
}
