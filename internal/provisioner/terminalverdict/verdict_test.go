package terminalverdict

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

const testLease = "11111111-1111-4111-8111-111111111111"

// TestFromProvisionAllowlist crosses every status with absent, empty, bogus,
// retry and exhausted verdicts and counts 0, 1 and 3. Only Failed + exhausted +
// a count of at least one yields a close proof.
func TestFromProvisionAllowlist(t *testing.T) {
	statuses := []backend.ProvisionStatus{
		backend.ProvisionStatusProvisioning, backend.ProvisionStatusReady, backend.ProvisionStatusFailing,
		backend.ProvisionStatusFailed, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating,
		backend.ProvisionStatusDeprovisioning, backend.ProvisionStatusRetained, backend.ProvisionStatusUnknown,
		"", "bogus",
	}
	verdicts := map[string]*backend.TerminalVerdict{
		"absent": nil, "empty": ptr(backend.TerminalVerdict("")), "bogus": ptr(backend.TerminalVerdict("bogus")),
		"retry": ptr(backend.TerminalVerdictRetry), "exhausted": ptr(backend.TerminalVerdictExhausted),
		"Exhausted": ptr(backend.TerminalVerdict("Exhausted")),
	}
	for _, status := range statuses {
		for name, wire := range verdicts {
			for _, count := range []int{-1, 0, 1, 3} {
				info := backend.ProvisionInfo{LeaseUUID: testLease, Status: status}
				if wire != nil {
					info.TerminalBudget = &backend.TerminalBudgetObservation{Verdict: *wire, ConsecutiveFailures: count}
				}
				verdict := FromProvision(info)
				label := fmt.Sprintf("%s/%s/%d", status, name, count)
				wantExhausted := status == backend.ProvisionStatusFailed && name == "exhausted" && count >= 1
				proof, exhausted := verdict.Exhausted()
				assert.Equal(t, wantExhausted, exhausted, label)
				assert.Equal(t, wantExhausted, proof.Valid(), label)
				switch {
				case wire == nil:
					assert.Equal(t, "absent", verdict.Label(), label)
				case wantExhausted:
					assert.Equal(t, "exhausted", verdict.Label(), label)
					assert.Equal(t, testLease, proof.LeaseUUID(), label)
					assert.Equal(t, count, proof.ConsecutiveFailures(), label)
				case name == "retry" && count >= 0:
					assert.Equal(t, "retry", verdict.Label(), label)
				default:
					assert.Equal(t, "unknown", verdict.Label(), label)
				}
			}
		}
	}
}

func TestFromProvisionRequiresALeaseIdentity(t *testing.T) {
	verdict := FromProvision(backend.ProvisionInfo{
		Status: backend.ProvisionStatusFailed,
		TerminalBudget: &backend.TerminalBudgetObservation{
			Verdict: backend.TerminalVerdictExhausted, ConsecutiveFailures: 3,
		},
	})
	_, exhausted := verdict.Exhausted()
	assert.False(t, exhausted)
	assert.Equal(t, "unknown", verdict.Label())
}

func TestZeroValuesNeverClose(t *testing.T) {
	_, exhausted := Verdict{}.Exhausted()
	assert.False(t, exhausted, "the zero verdict is absent")
	assert.Equal(t, "absent", Verdict{}.Label())
	assert.False(t, Exhaustion{}.Valid(), "the zero proof is invalid")
}

func TestLabelsAreTheClosedSet(t *testing.T) {
	assert.Equal(t, []string{"absent", "unknown", "retry", "exhausted"}, Labels())
	for raw := range 256 {
		label := Verdict{kind: kind(raw)}.Label()
		if kind(raw) >= kindSentinel {
			assert.Equal(t, "absent", label, "kind %d outside the closed set", raw)
		}
		_, exhausted := Verdict{kind: kind(raw), leaseUUID: testLease, consecutiveFailures: 3}.Exhausted()
		assert.Equal(t, kind(raw) == kindExhausted, exhausted, "kind %d", raw)
	}
}

func TestTenantViewOnlyCopiesRecognizedObservations(t *testing.T) {
	failed := func(verdict backend.TerminalVerdict, count int) backend.ProvisionInfo {
		return backend.ProvisionInfo{
			LeaseUUID: testLease, Status: backend.ProvisionStatusFailed,
			TerminalBudget: &backend.TerminalBudgetObservation{Verdict: verdict, ConsecutiveFailures: count},
		}
	}
	assert.Nil(t, TenantView(backend.ProvisionInfo{LeaseUUID: testLease}))
	assert.Nil(t, TenantView(failed("bogus", 3)))
	assert.Nil(t, TenantView(failed(backend.TerminalVerdictRetry, -1)))

	source := failed(backend.TerminalVerdictExhausted, 3)
	view := TenantView(source)
	require.NotNil(t, view)
	assert.Equal(t, *source.TerminalBudget, *view)
	assert.NotSame(t, source.TerminalBudget, view, "the tenant view must not alias inventory")
	assert.Equal(t, &backend.TerminalBudgetObservation{Verdict: backend.TerminalVerdictRetry, ConsecutiveFailures: 2},
		TenantView(failed(backend.TerminalVerdictRetry, 2)))
}

func TestSealedTypesExportNoFields(t *testing.T) {
	for _, value := range []any{Verdict{}, Exhaustion{}} {
		typeOf := reflect.TypeOf(value)
		for index := range typeOf.NumField() {
			assert.Falsef(t, typeOf.Field(index).IsExported(),
				"%s.%s must not be settable outside this package", typeOf, typeOf.Field(index).Name)
		}
	}
}

func ptr[T any](value T) *T { return &value }
