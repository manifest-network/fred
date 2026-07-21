package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// deployManifestWithLabel returns a stack payload whose single service carries
// the partition label — the integrator-side declaration.
func deployManifestWithLabel(key, value string) []byte {
	m := map[string]any{"services": map[string]any{"app": map[string]any{
		"image":  "nginx:alpine",
		"labels": map[string]string{key: value},
	}}}
	data, _ := json.Marshal(m)
	return data
}

// twoServiceManifest: two services carrying DIFFERENT values for the key —
// the divergence case. (The harness seeds ProvisionState directly, so the
// provision-time ValidateStackAgainstItems 1:1 check never runs here; only
// close-time extraction reads this manifest.)
func twoServiceManifest(key, v1, v2 string) []byte {
	m := map[string]any{"services": map[string]any{
		"app": map[string]any{"image": "nginx:alpine", "labels": map[string]string{key: v1}},
		"db":  map[string]any{"image": "nginx:alpine", "labels": map[string]string{key: v2}},
	}}
	data, _ := json.Marshal(m)
	return data
}

// TestNew_PopulatesPartitionSource is the production wiring pin. The unit test
// harnesses (newBackendForTest) build the Backend literal directly and never run
// New, so they cannot catch a regression where New forgets to populate
// b.partitionSource — the field would stay zero (PartitionSourceNone), silently
// disabling partitioning in production while every test still passes because each
// test assigns b.partitionSource by hand. This drives the REAL constructor (New
// defers the Docker daemon connection to Start, so it runs headless) and asserts
// the parsed source landed on the field.
func TestNew_PopulatesPartitionSource(t *testing.T) {
	const srcKey = "com.example.customer"
	cfg := DefaultConfig()
	tmp := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(tmp, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(tmp, "diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(tmp, "releases.db")
	cfg.RetentionDBPath = filepath.Join(tmp, "retention.db")
	cfg.VolumeDataPath = "" // noopVolumeManager; no filesystem needed
	// DiskMB 0 keeps VolumeDataPath="" valid (no XFS mount required) and makes
	// largestSKUDiskMB 0 so the budget's disk floor checks are vacuously satisfied.
	cfg.SKUProfiles = map[string]shared.SKUProfile{
		"docker-micro": {CPUCores: 1, MemoryMB: 256, DiskMB: 0},
	}
	cfg.RetainOnClose = true
	cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	cfg.HostAddress = "127.0.0.1"
	cfg.RetentionPartitionSource = "manifest.label:" + srcKey
	cfg.RetentionTenantBudgets = map[string]RetentionTenantBudget{
		"agg": {MaxRetainedLeases: 200, MaxRetainedDiskMB: 50000, MaxPartitions: 64},
	}

	b, err := New(cfg, slog.Default())
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Stop() })

	require.Equal(t, shared.PartitionSourceManifestLabel, b.partitionSource.Kind,
		"New must populate b.partitionSource — an unset field silently disables partitioning in production")
	require.Equal(t, srcKey, b.partitionSource.Key)
}

func TestClosePartitionMatrix(t *testing.T) {
	const srcKey = "com.example.customer"
	aggBudget := RetentionTenantBudget{MaxRetainedLeases: 200, MaxRetainedDiskMB: 500000, MaxPartitions: 64, PerPartitionMaxLeases: 5}

	type wantCounters struct{ stamped, collapsedDivergent, collapsedInvalid, collapsedOverLimit float64 }
	cases := []struct {
		name           string
		allowlisted    bool
		labelValue     string
		secondService  string
		seedPartitions int
		wantPartition  string
		want           wantCounters
	}{
		// (a): budget-first short-circuit — N spam closes are N independent
		// copies of this case (identical MaxPartitions==0 path).
		{name: "(a) non-allowlisted tenant label collapses silently, stores nothing",
			allowlisted: false, labelValue: "cust-1", wantPartition: "", want: wantCounters{}},
		{name: "allowlisted + valid label stamps",
			allowlisted: true, labelValue: "cust-1", wantPartition: "cust-1", want: wantCounters{stamped: 1}},
		{name: "allowlisted + no label = silent default",
			allowlisted: true, labelValue: "", wantPartition: "", want: wantCounters{}},
		{name: "allowlisted + invalid label collapses with counter",
			allowlisted: true, labelValue: "has spaces", wantPartition: "", want: wantCounters{collapsedInvalid: 1}},
		{name: "allowlisted + divergent labels collapse with counter",
			allowlisted: true, labelValue: "cust-1", secondService: "cust-2", wantPartition: "", want: wantCounters{collapsedDivergent: 1}},
		// over max_partitions collapses to default, record retained.
		{name: "(c) over max_partitions collapses to default, record retained",
			allowlisted: true, labelValue: "cust-new", seedPartitions: 64,
			wantPartition: "", want: wantCounters{collapsedOverLimit: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := deployManifestWithLabel(srcKey, tc.labelValue)
			if tc.labelValue == "" {
				payload = deployManifestWithLabel("unrelated", "x")
			}
			if tc.secondService != "" {
				payload = twoServiceManifest(srcKey, tc.labelValue, tc.secondService)
			}

			b, rs, leaseUUID := newCloseHarness(t, "tenant-a", payload)
			b.cfg.RetentionPartitionSource = "manifest.label:" + srcKey
			if tc.allowlisted {
				b.cfg.RetentionTenantBudgets = map[string]RetentionTenantBudget{"tenant-a": aggBudget}
			}
			var perr error
			b.partitionSource, perr = shared.ParsePartitionSource(b.cfg.RetentionPartitionSource)
			require.NoError(t, perr)
			for i := 0; i < tc.seedPartitions; i++ {
				putActivePart(t, rs, fmt.Sprintf("seed-%03d", i), "tenant-a", fmt.Sprintf("pre-%03d", i), time.Now().Add(-time.Hour))
			}

			stampedBefore := testutil.ToFloat64(retentionPartitionStampedTotal)
			divBefore := testutil.ToFloat64(retentionPartitionCollapsedTotal.WithLabelValues(shared.PartitionReasonDivergent))
			invBefore := testutil.ToFloat64(retentionPartitionCollapsedTotal.WithLabelValues(shared.PartitionReasonInvalid))
			overBefore := testutil.ToFloat64(retentionPartitionCollapsedTotal.WithLabelValues(shared.PartitionReasonOverLimit))

			require.NoError(t, b.Deprovision(context.Background(), leaseUUID))

			require.Eventually(t, func() bool {
				rec, err := rs.Get(leaseUUID)
				return err == nil && rec != nil && rec.Status == shared.RetentionStatusActive
			}, 5*time.Second, 50*time.Millisecond, "close must retain in every matrix case (I5)")

			rec, err := rs.Get(leaseUUID)
			require.NoError(t, err)
			require.Equal(t, tc.wantPartition, rec.Partition)
			require.Equal(t, tc.want.stamped, testutil.ToFloat64(retentionPartitionStampedTotal)-stampedBefore)
			require.Equal(t, tc.want.collapsedDivergent, testutil.ToFloat64(retentionPartitionCollapsedTotal.WithLabelValues(shared.PartitionReasonDivergent))-divBefore)
			require.Equal(t, tc.want.collapsedInvalid, testutil.ToFloat64(retentionPartitionCollapsedTotal.WithLabelValues(shared.PartitionReasonInvalid))-invBefore)
			require.Equal(t, tc.want.collapsedOverLimit, testutil.ToFloat64(retentionPartitionCollapsedTotal.WithLabelValues(shared.PartitionReasonOverLimit))-overBefore)
		})
	}
}

// TestClose_LegacyByteIdentical pins worked example (e): nothing configured →
// Partition "" and zero partition-series movement.
func TestClose_LegacyByteIdentical(t *testing.T) {
	b, rs, leaseUUID := newCloseHarness(t, "tenant-a", nil)

	stampedBefore := testutil.ToFloat64(retentionPartitionStampedTotal)
	require.NoError(t, b.Deprovision(context.Background(), leaseUUID))
	require.Eventually(t, func() bool {
		rec, err := rs.Get(leaseUUID)
		return err == nil && rec != nil
	}, 5*time.Second, 50*time.Millisecond)

	rec, err := rs.Get(leaseUUID)
	require.NoError(t, err)
	require.Equal(t, "", rec.Partition)
	require.Equal(t, stampedBefore, testutil.ToFloat64(retentionPartitionStampedTotal))
}

// TestClose_GlobalCapStillRefusesBudgetedTenant: L0 composes above budgets —
// legacy retention_refused_total AND refused_by_scope{global} both fire, and
// another tenant's record is untouched.
func TestClose_GlobalCapStillRefusesBudgetedTenant(t *testing.T) {
	b, rs, leaseUUID := newCloseHarness(t, "tenant-a", deployManifestWithLabel("com.example.customer", "cust-1"))
	b.cfg.MaxRetainedDiskMB = 1536 // one 1024MB footprint + slack, never two
	b.cfg.RetentionPartitionSource = "manifest.label:com.example.customer"
	b.cfg.RetentionTenantBudgets = map[string]RetentionTenantBudget{
		"tenant-a": {MaxRetainedLeases: 200, MaxRetainedDiskMB: 500000, MaxPartitions: 64},
	}
	var perr error
	b.partitionSource, perr = shared.ParsePartitionSource(b.cfg.RetentionPartitionSource)
	require.NoError(t, perr)
	putActivePart(t, rs, "other-tenant-rec", "other", "", time.Now().Add(-time.Hour)) // fills L0

	bareBefore := testutil.ToFloat64(retentionRefusedTotal)
	scopedBefore := testutil.ToFloat64(retentionRefusedByScopeTotal.WithLabelValues(refuseScopeGlobal))

	require.NoError(t, b.Deprovision(context.Background(), leaseUUID))

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(retentionRefusedTotal) == bareBefore+1 &&
			testutil.ToFloat64(retentionRefusedByScopeTotal.WithLabelValues(refuseScopeGlobal)) == scopedBefore+1
	}, 5*time.Second, 50*time.Millisecond)
	require.Equal(t, shared.RetentionStatusActive, statusOf(t, rs, "other-tenant-rec"))
}

// TestClose_ScopedRefusals drives the L1 (per-tenant aggregate) and L2
// (per-partition) disk refusals through the REAL close path, asserting the scoped
// counter fires at the right scope while (a) the bare retentionRefusedTotal keeps
// its deployed L0-global-only meaning and (b) a disk refusal destroys ONLY the
// incoming lease — every already-retained record survives (disk caps never evict).
func TestClose_ScopedRefusals(t *testing.T) {
	const srcKey = "com.example.customer"

	t.Run("tenant scope", func(t *testing.T) {
		// docker-micro at 1024 MB each. Tenant disk cap 4096; 4 held = 4096 (full);
		// incoming 1024 → 4096+1024 > 4096 breaches the tenant scope. CountCap 200
		// (>5) so no eviction fires before the disk gate.
		b, rs, leaseUUID := newCloseHarness(t, "tenant-a", nil)
		b.cfg.RetentionTenantBudgets = map[string]RetentionTenantBudget{
			"tenant-a": {MaxRetainedLeases: 200, MaxRetainedDiskMB: 4096, MaxPartitions: 0}, // elevation-only, tight disk
		}
		for i := 0; i < 4; i++ { // 4×1024 fills the tenant budget exactly
			putActivePart(t, rs, fmt.Sprintf("t-%d", i), "tenant-a", "", time.Now().Add(-time.Hour))
		}
		bareBefore := testutil.ToFloat64(retentionRefusedTotal)
		scopedBefore := testutil.ToFloat64(retentionRefusedByScopeTotal.WithLabelValues(refuseScopeTenant))
		require.NoError(t, b.Deprovision(context.Background(), leaseUUID))
		require.Eventually(t, func() bool {
			return testutil.ToFloat64(retentionRefusedByScopeTotal.WithLabelValues(refuseScopeTenant)) == scopedBefore+1
		}, 5*time.Second, 50*time.Millisecond)
		require.Equal(t, bareBefore, testutil.ToFloat64(retentionRefusedTotal),
			"bare counter keeps its deployed L0-only meaning")
		for i := 0; i < 4; i++ { // refusal destroys only the incoming
			require.Equal(t, shared.RetentionStatusActive, statusOf(t, rs, fmt.Sprintf("t-%d", i)))
		}
	})

	t.Run("partition scope", func(t *testing.T) {
		// cust-a already holds 4×1024 = its full per-partition disk sub-cap (4096);
		// incoming labeled cust-a → 4096+1024 > 4096 breaches L2. PerPartitionMaxLeases
		// 10 (>4) and CountCap 200 so no eviction fires before the disk gate; L1 disk
		// (500000) is slack, so the FIRST breach is at the partition scope.
		b, rs, leaseUUID := newCloseHarness(t, "tenant-a", deployManifestWithLabel(srcKey, "cust-a"))
		b.cfg.RetentionPartitionSource = "manifest.label:" + srcKey
		b.cfg.RetentionTenantBudgets = map[string]RetentionTenantBudget{
			"tenant-a": {MaxRetainedLeases: 200, MaxRetainedDiskMB: 500000,
				MaxPartitions: 64, PerPartitionMaxLeases: 10, PerPartitionMaxDiskMB: 4096},
		}
		var perr error
		b.partitionSource, perr = shared.ParsePartitionSource(b.cfg.RetentionPartitionSource)
		require.NoError(t, perr)
		for i := 0; i < 4; i++ { // cust-a already holds 4×1024 = its full disk sub-cap
			putActivePart(t, rs, fmt.Sprintf("p-%d", i), "tenant-a", "cust-a", time.Now().Add(-time.Hour))
		}
		scopedBefore := testutil.ToFloat64(retentionRefusedByScopeTotal.WithLabelValues(refuseScopePartition))
		require.NoError(t, b.Deprovision(context.Background(), leaseUUID))
		require.Eventually(t, func() bool {
			return testutil.ToFloat64(retentionRefusedByScopeTotal.WithLabelValues(refuseScopePartition)) == scopedBefore+1
		}, 5*time.Second, 50*time.Millisecond)
		for i := 0; i < 4; i++ {
			require.Equal(t, shared.RetentionStatusActive, statusOf(t, rs, fmt.Sprintf("p-%d", i)),
				"every already-retained record in the partition survives (disk caps never evict)")
		}
	})
}
