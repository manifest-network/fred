package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceRefusalSeriesExistBeforeFirstRefusal(t *testing.T) {
	// Collect the actual registered vector without WithLabelValues: calling it
	// here would create the series and conceal a missing package initialization.
	require.NoError(t, testutil.CollectAndCompare(MaintenanceAdmissionRefusalsTotal, strings.NewReader(`
# HELP fred_maintenance_admission_refusals_total Fresh maintenance commands refused by global pending budget or its newcomer reservation
# TYPE fred_maintenance_admission_refusals_total counter
fred_maintenance_admission_refusals_total{reason="bytes"} 0
fred_maintenance_admission_refusals_total{reason="count"} 0
fred_maintenance_admission_refusals_total{reason="reserved_bytes"} 0
fred_maintenance_admission_refusals_total{reason="reserved_count"} 0
`)))
}
