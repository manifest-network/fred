package imagefetch

import (
	"fmt"
	"math"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestImportAllocationAuthorityOwnsPressureAndRefusal(t *testing.T) {
	for _, tc := range []struct {
		limit             int64
		used              int64
		pressure, refused bool
	}{
		{100, 160, false, false}, {100, 161, true, false}, {100, 200, true, false}, {100, 201, false, true},
		{101, 161, false, false}, {101, 162, true, false},
	} {
		t.Run(fmt.Sprint(tc.limit, "/", tc.used), func(t *testing.T) {
			loader, err := NewLoader(&recordingImporter{}, t.TempDir(), tc.limit)
			require.NoError(t, err)
			pressure := testutil.ToFloat64(imageAllocationPressure)
			refused := testutil.ToFloat64(imagePreparationRefusals.WithLabelValues("import_allocation"))
			budget, err := loader.admitPreparedBudget("registry.example/image@sha256:verified", max(tc.limit, (tc.used+1)/2), tc.used)
			if tc.refused {
				require.ErrorContains(t, err, "import allocation")
				require.Zero(t, budget.Allocation().Bytes())
				refused++
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.used, budget.Allocation().Bytes())
			}
			if tc.pressure {
				pressure++
			}
			require.Equal(t, pressure, testutil.ToFloat64(imageAllocationPressure))
			require.Equal(t, refused, testutil.ToFloat64(imagePreparationRefusals.WithLabelValues("import_allocation")))
		})
	}
	loader, err := NewLoader(&recordingImporter{}, t.TempDir(), math.MaxInt64/8)
	require.NoError(t, err)
	_, err = loader.admitPreparedBudget("registry.example/large", math.MaxInt64/8, math.MaxInt64/4-1)
	require.NoError(t, err, "threshold calculation cannot overflow a valid allowance")
	recovery, err := loader.WithBudget(loader.VerificationBudget())
	require.NoError(t, err)
	before := testutil.ToFloat64(imageAllocationPressure)
	_, err = recovery.admitPreparedBudget("registry.example/large", math.MaxInt64/8, math.MaxInt64/4-1)
	require.NoError(t, err)
	require.Equal(t, before, testutil.ToFloat64(imageAllocationPressure), "exact saved recovery budgets do not imply new-image growth pressure")
}

func TestPrepareRecordsExactImportAllocationRefusal(t *testing.T) {
	f := newRegistry(t, encodedTar(t))
	daemon := &recordingImporter{}
	loader, err := NewLoader(daemon, t.TempDir(), 1<<20, WithRegistryTransport(f.server.Client().Transport.(*http.Transport)))
	require.NoError(t, err)
	prepared, err := loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.NoError(t, err)
	measured := prepared.ImportBytes()
	require.NoError(t, prepared.Close())
	// The identical metadata/layer fixture fits staging, but one byte beyond
	// half its measured footprint must refuse before any Docker dispatch.
	loader, err = NewLoader(daemon, t.TempDir(), (measured-1)/2, WithRegistryTransport(f.server.Client().Transport.(*http.Transport)))
	require.NoError(t, err)
	before := testutil.ToFloat64(imagePreparationRefusals.WithLabelValues("import_allocation"))
	prepared, err = loader.Prepare(t.Context(), f.ref(), testPlatform)
	require.ErrorContains(t, err, "import allocation")
	require.Nil(t, prepared)
	require.Equal(t, before+1, testutil.ToFloat64(imagePreparationRefusals.WithLabelValues("import_allocation")))
	require.Zero(t, daemon.loads)
}
