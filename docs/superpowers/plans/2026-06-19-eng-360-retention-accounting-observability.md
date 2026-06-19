# ENG-360 Retained-Volume Disk Accounting, Cap & Observability — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make soft-deleted ("retained") volumes count against the docker backend's disk admission pool, add a per-provider retained-bytes cap with a refuse-to-retain breach policy, and export retained-volume metrics — closing a disk over-commit / ENOSPC risk and making retention disk pressure observable.

**Architecture:** The `ResourcePool` gains a single `retainedDisk` MB term that the admission gate subtracts from available disk. That term is a *projection* the docker `Backend` recomputes from the bbolt retention store (the single source of truth) at every retention transition, on recover, and on the periodic sweep tick — never an independently-mutated counter. A new `max_retained_disk_mb` config key bounds the retained tier; on breach at close the lease is destroyed instead of retained (never touching other tenants' in-grace data). Three new gauges + a counter + two denominator gauges export the state.

**Tech Stack:** Go, `gopkg.in/yaml.v3` config, `prometheus/client_golang` (promauto), bbolt stores, `testify`.

**Spec:** `docs/superpowers/specs/2026-06-19-eng-360-retention-accounting-observability-design.md`

**Test discipline:** Per repo convention, pair `-race` with `-short` when running the full suite (`go test -race -short ./...`) — the stress tests OOM-kill the runner under the race detector. Targeted `-run` invocations below don't need `-race`.

---

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `internal/backend/shared/resources.go` | Pool admission accounting incl. the retained-disk term | 1 |
| `internal/backend/shared/resources_test.go` | Pool unit tests (`package shared`) | 1 |
| `internal/backend/docker/config.go` | `MaxRetainedDiskMB` key, defaults, validation | 2 |
| `internal/backend/docker/config_test.go` | Config validation tests | 2 |
| `internal/backend/docker/metrics.go` | 5 new metrics + updater functions | 3 |
| `internal/backend/docker/metrics_test.go` | Metric updater tests | 3 |
| `internal/backend/docker/retention_accounting.go` (new) | `leaseDiskMB`, `computeRetainedDiskMB`, `refreshRetentionAccounting`, `breachRetentionCap` | 4, 6 |
| `internal/backend/docker/retention_accounting_test.go` (new) | Projection + breach + recover-wiring tests | 4, 5, 6 |
| `internal/backend/docker/deprovision.go` | Call refresh after release; refuse-to-retain breach in the retain branch | 5, 6 |
| `internal/backend/docker/restore.go` | Call refresh after reap/evict/sweep | 5 |
| `internal/backend/docker/recover.go` | Rebuild projection after `pool.Reset` | 5 |
| `internal/backend/docker/backend.go` | Static pool metrics in `New`; startup over-provision WARN in `Start` | 3, 7 |
| `OPERATIONS.md`, `ARCHITECTURE.md`, `docker-backend.example.yaml`, `internal/backend/docker/README.md`, `CHANGELOG.md` | Docs | 8 |

---

## Task 1: Pool retained-disk accounting

**Files:**
- Modify: `internal/backend/shared/resources.go`
- Test: `internal/backend/shared/resources_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/backend/shared/resources_test.go` (`package shared`):

```go
func TestRetainedDiskCountsAgainstPool(t *testing.T) {
	resolver := func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 1024}, nil
	}
	// Total disk 2048 MB.
	p := NewResourcePool(8, 8192, 2048, resolver, nil)

	// Reserve 1024 MB as retained (a soft-deleted volume still on disk).
	p.SetRetainedDisk(1024)

	// A 1024 MB live allocation exactly fills the remaining headroom.
	require.NoError(t, p.TryAllocate("lease-1", "sku", "tenant"))

	// A second 1024 MB allocation must be denied:
	// 1024 (live) + 1024 (retained) + 1024 (new) > 2048.
	err := p.TryAllocate("lease-2", "sku", "tenant")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient disk")
}

func TestStatsIncludesRetainedDisk(t *testing.T) {
	resolver := func(string) (SKUProfile, error) { return SKUProfile{}, nil }
	p := NewResourcePool(8, 8192, 4096, resolver, nil)
	p.SetRetainedDisk(1500)

	s := p.Stats()
	assert.Equal(t, int64(1500), s.RetainedDiskMB)
	assert.Equal(t, int64(4096-1500), s.AvailableDiskMB())
}

func TestResetPreservesRetainedDisk(t *testing.T) {
	resolver := func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 1024}, nil
	}
	p := NewResourcePool(8, 8192, 4096, resolver, nil)
	p.SetRetainedDisk(1024)
	// Reset rebuilds live allocations; it must NOT clobber the retained projection
	// (the backend re-pushes it via refreshRetentionAccounting after recover, but
	// Reset itself owns only live state).
	p.Reset([]ResourceAllocation{{LeaseUUID: "l1", SKU: "sku", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}})
	assert.Equal(t, int64(1024), p.Stats().RetainedDiskMB)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/backend/shared/ -run 'TestRetainedDiskCountsAgainstPool|TestStatsIncludesRetainedDisk|TestResetPreservesRetainedDisk' -v`
Expected: FAIL — `p.SetRetainedDisk undefined` and `s.RetainedDiskMB undefined`.

- [ ] **Step 3: Add the `retainedDisk` field**

In `internal/backend/shared/resources.go`, add a field to the `ResourcePool` struct after the `allocatedDisk int64` line (currently line 35):

```go
	// retainedDisk is the aggregate disk (MB) reserved by soft-deleted
	// (retained) volumes. It is a projection pushed by the owner via
	// SetRetainedDisk (derived from the retention store), subtracted from
	// available disk in TryAllocate so retained volumes keep counting against
	// the pool until they are actually reaped. Not touched by Reset (which owns
	// only live allocations) — the owner re-pushes it after recover.
	retainedDisk int64
```

- [ ] **Step 4: Subtract retained disk in the admission gate**

In `TryAllocate`, replace the disk capacity check (currently lines 92-95):

```go
	if p.allocatedDisk+profile.DiskMB > p.totalDisk {
		return fmt.Errorf("insufficient disk: need %d MB, have %d MB available",
			profile.DiskMB, p.totalDisk-p.allocatedDisk)
	}
```

with:

```go
	if p.allocatedDisk+p.retainedDisk+profile.DiskMB > p.totalDisk {
		return fmt.Errorf("insufficient disk: need %d MB, have %d MB available",
			profile.DiskMB, p.totalDisk-p.allocatedDisk-p.retainedDisk)
	}
```

- [ ] **Step 5: Add `SetRetainedDisk`, extend `Stats` and `ResourceStats`/`AvailableDiskMB`**

Add the method (e.g. just after `Release`):

```go
// SetRetainedDisk records the aggregate disk (MB) reserved by retained
// (soft-deleted) volumes. The owner derives this from the retention store and
// pushes it here; TryAllocate subtracts it from available disk so retained
// volumes keep counting against the pool until reaped. Idempotent.
func (p *ResourcePool) SetRetainedDisk(mb int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retainedDisk = mb
}
```

In `Stats()`, add the field to the returned `ResourceStats`:

```go
		RetainedDiskMB:    p.retainedDisk,
```

In the `ResourceStats` struct, add after `AllocatedDiskMB int64`:

```go
	RetainedDiskMB    int64
```

Replace `AvailableDiskMB`:

```go
// AvailableDiskMB returns disk available for new allocations: total minus live
// allocations minus retained (soft-deleted) reservations.
func (s ResourceStats) AvailableDiskMB() int64 {
	return s.TotalDiskMB - s.AllocatedDiskMB - s.RetainedDiskMB
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/backend/shared/ -run 'TestRetainedDiskCountsAgainstPool|TestStatsIncludesRetainedDisk|TestResetPreservesRetainedDisk' -v`
Expected: PASS. Then `go test ./internal/backend/shared/ -v` to confirm no regressions (existing `TestResourcePool` etc.).

- [ ] **Step 7: Commit**

```bash
git add internal/backend/shared/resources.go internal/backend/shared/resources_test.go
git commit -m "feat(eng-360): account retained-volume disk against the resource pool

ResourcePool gains a retainedDisk projection subtracted from available disk in
TryAllocate, so soft-deleted volumes keep counting against total_disk_mb until
reaped. Pushed by the owner via SetRetainedDisk; Reset preserves it.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `max_retained_disk_mb` config key + validation

**Files:**
- Modify: `internal/backend/docker/config.go`
- Test: `internal/backend/docker/config_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `internal/backend/docker/config_test.go` (`package docker`). `validConfig()` (config_test.go:17) returns a fully-valid Config; we override fields per case:

```go
func TestValidate_MaxRetainedDiskMB(t *testing.T) {
	// A config whose largest stateful SKU is 1024 MB and pool is 102400 MB.
	base := func() Config {
		c := validConfig()
		c.TotalDiskMB = 102400
		c.SKUProfiles = map[string]SKUProfile{
			"docker-small": {CPUCores: 1, MemoryMB: 512, DiskMB: 1024},
		}
		c.SKUMapping = map[string]string{"019c1ee7-1aaf-7000-802c-ad775c72cc27": "docker-small"}
		return c
	}

	t.Run("zero is unlimited and valid", func(t *testing.T) {
		c := base()
		c.MaxRetainedDiskMB = 0
		require.NoError(t, c.Validate())
	})
	t.Run("negative rejected", func(t *testing.T) {
		c := base()
		c.MaxRetainedDiskMB = -1
		require.ErrorContains(t, c.Validate(), "max_retained_disk_mb must be non-negative")
	})
	t.Run("exceeding total_disk_mb rejected", func(t *testing.T) {
		c := base()
		c.MaxRetainedDiskMB = c.TotalDiskMB + 1
		require.ErrorContains(t, c.Validate(), "max_retained_disk_mb")
	})
	t.Run("below largest stateful SKU rejected", func(t *testing.T) {
		c := base()
		c.MaxRetainedDiskMB = 512 // < 1024 largest SKU → a SKU-legal lease could never be retained
		require.ErrorContains(t, c.Validate(), "largest")
	})
	t.Run("valid positive accepted", func(t *testing.T) {
		c := base()
		c.MaxRetainedDiskMB = 4096
		require.NoError(t, c.Validate())
	})
}

func TestDefaultConfig_MaxRetainedDiskMBUnlimited(t *testing.T) {
	assert.Equal(t, int64(0), DefaultConfig().MaxRetainedDiskMB)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/backend/docker/ -run 'TestValidate_MaxRetainedDiskMB|TestDefaultConfig_MaxRetainedDiskMBUnlimited' -v`
Expected: FAIL — `c.MaxRetainedDiskMB undefined`.

- [ ] **Step 3: Add the config field**

In `internal/backend/docker/config.go`, add to the `Config` struct after `MaxRetainedLeasesPerTenant` (currently line 221):

```go
	// MaxRetainedDiskMB caps the aggregate disk (MB) the provider will hold in
	// the retained (soft-deleted) tier across ALL tenants. When retaining a
	// closing lease would push the total over this cap, the lease is destroyed
	// immediately instead of retained (refuse-to-retain) — never evicting
	// another tenant's in-grace data. 0 means unlimited (default; retained
	// volumes still count against total_disk_mb via the admission gate, but are
	// not separately bounded). Must be <= total_disk_mb and, when set, >= the
	// largest single stateful SKU's disk_mb (else a SKU-legal lease could never
	// be retained). Independent of tenant_quota.max_disk_mb: it may be smaller,
	// in which case a tenant's max-sized lease is SKU-legal yet refused
	// retention. Duration/byte values use plain integers (MB).
	MaxRetainedDiskMB int64 `yaml:"max_retained_disk_mb"`
```

- [ ] **Step 4: Add a largest-SKU helper and the validation rules**

Add a helper method near `HasStatefulSKUs` (config.go:290):

```go
// largestSKUDiskMB returns the maximum disk_mb across all configured SKU
// profiles (0 if none are stateful). Used to validate that a retained-disk cap
// can hold at least a one-unit lease of every SKU.
func (c *Config) largestSKUDiskMB() int64 {
	var max int64
	for _, p := range c.SKUProfiles {
		if p.DiskMB > max {
			max = p.DiskMB
		}
	}
	return max
}
```

In `Validate()`, after the `max_retained_leases_per_tenant` check (currently lines 497-499):

```go
	if c.MaxRetainedDiskMB < 0 {
		return fmt.Errorf("max_retained_disk_mb must be non-negative")
	}
	if c.MaxRetainedDiskMB > 0 {
		if c.MaxRetainedDiskMB > c.TotalDiskMB {
			return fmt.Errorf("max_retained_disk_mb (%d) must not exceed total_disk_mb (%d)", c.MaxRetainedDiskMB, c.TotalDiskMB)
		}
		if largest := c.largestSKUDiskMB(); largest > 0 && c.MaxRetainedDiskMB < largest {
			return fmt.Errorf("max_retained_disk_mb (%d) must be >= the largest stateful SKU disk_mb (%d), else a SKU-legal lease could never be retained", c.MaxRetainedDiskMB, largest)
		}
	}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/backend/docker/ -run 'TestValidate_MaxRetainedDiskMB|TestDefaultConfig_MaxRetainedDiskMBUnlimited' -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/backend/docker/config.go internal/backend/docker/config_test.go
git commit -m "feat(eng-360): add max_retained_disk_mb config key + validation

Per-provider cap on the retained-volume tier (0 = unlimited). Validated
non-negative, <= total_disk_mb, and >= the largest stateful SKU disk_mb.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Retained-volume metrics

**Files:**
- Modify: `internal/backend/docker/metrics.go`
- Modify: `internal/backend/docker/backend.go` (static gauges in `New`)
- Test: `internal/backend/docker/metrics_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/backend/docker/metrics_test.go` (create the file if absent, `package docker`):

```go
package docker

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestUpdateRetentionMetrics(t *testing.T) {
	updateRetentionMetrics(2048, 3) // 2048 MB, 3 retained leases
	assert.Equal(t, float64(2048)*(1<<20), testutil.ToFloat64(retainedVolumeBytes))
	assert.Equal(t, float64(3), testutil.ToFloat64(retainedVolumes))
}

func TestSetStaticPoolMetrics(t *testing.T) {
	cfg := Config{TotalDiskMB: 100, MaxRetainedDiskMB: 40}
	setStaticPoolMetrics(cfg)
	assert.Equal(t, float64(100)*(1<<20), testutil.ToFloat64(diskPoolBytes))
	assert.Equal(t, float64(40)*(1<<20), testutil.ToFloat64(retainedDiskCapBytes))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/backend/docker/ -run 'TestUpdateRetentionMetrics|TestSetStaticPoolMetrics' -v`
Expected: FAIL — `updateRetentionMetrics`, `retainedVolumeBytes`, etc. undefined.

- [ ] **Step 3: Define the metrics**

In `internal/backend/docker/metrics.go`, inside the `var (...)` block (e.g. after `resourceDiskAllocatedRatio`, line 75), add:

```go
	// retainedVolumeBytes is the reserved disk capacity (sum of active retained
	// leases' SKU disk_mb, converted to bytes) currently pinned by soft-deleted
	// volumes. Reserved quota, not measured on-disk usage (see ENG-360 spec).
	retainedVolumeBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retained_volume_bytes",
		Help:      "Reserved disk capacity (SKU quota) pinned by retained/soft-deleted volumes, in bytes",
	})

	// retainedVolumes is the number of active retained (soft-deleted) leases.
	retainedVolumes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retained_volumes",
		Help:      "Number of active retained (soft-deleted) leases",
	})

	// retentionRefusedTotal counts close-time refuse-to-retain events caused by
	// the max_retained_disk_mb cap (volumes destroyed instead of retained).
	retentionRefusedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_refused_total",
		Help:      "Close-time refuse-to-retain events due to the max_retained_disk_mb cap",
	})

	// diskPoolBytes is the admission ceiling (total_disk_mb in bytes). A
	// denominator so dashboards don't hardcode the pool size.
	diskPoolBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "disk_pool_bytes",
		Help:      "Total disk admission pool (total_disk_mb) in bytes",
	})

	// retainedDiskCapBytes is the per-provider retained cap (max_retained_disk_mb
	// in bytes), set only when the cap is configured (> 0). Alert denominator.
	retainedDiskCapBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retained_disk_cap_bytes",
		Help:      "Per-provider retained-volume cap (max_retained_disk_mb) in bytes; 0 when unset",
	})
```

After the closing `)` of the `var` block (before `updateResourceMetrics`, line 297), add:

```go
const bytesPerMiB = 1 << 20

// updateRetentionMetrics sets the retained-volume gauges from the current
// projection (MB) and active-lease count.
func updateRetentionMetrics(retainedMB int64, count int) {
	retainedVolumeBytes.Set(float64(retainedMB) * bytesPerMiB)
	retainedVolumes.Set(float64(count))
}

// setStaticPoolMetrics sets the constant denominator gauges once at startup.
func setStaticPoolMetrics(cfg Config) {
	diskPoolBytes.Set(float64(cfg.TotalDiskMB) * bytesPerMiB)
	if cfg.MaxRetainedDiskMB > 0 {
		retainedDiskCapBytes.Set(float64(cfg.MaxRetainedDiskMB) * bytesPerMiB)
	}
}
```

- [ ] **Step 4: Call `setStaticPoolMetrics` in `New`**

In `internal/backend/docker/backend.go`, in `New` after the `pool := shared.NewResourcePool(...)` block (currently ends line 409), add:

```go
	setStaticPoolMetrics(cfg)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/backend/docker/ -run 'TestUpdateRetentionMetrics|TestSetStaticPoolMetrics' -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/backend/docker/metrics.go internal/backend/docker/metrics_test.go internal/backend/docker/backend.go
git commit -m "feat(eng-360): add retained-volume Prometheus metrics

retained_volume_bytes, retained_volumes (gauges), retention_refused_total
(counter), and disk_pool_bytes / retained_disk_cap_bytes denominator gauges.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Retention-disk projection helpers

**Files:**
- Create: `internal/backend/docker/retention_accounting.go`
- Test: `internal/backend/docker/retention_accounting_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/backend/docker/retention_accounting_test.go` (`package docker`):

```go
package docker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// withMicroSKU sets a known stateful SKU on the test backend's config so the
// projection (which reads b.cfg.GetSKUProfile) resolves a deterministic DiskMB.
func withMicroSKU(b *Backend, diskMB int64) {
	b.cfg.SKUProfiles = map[string]shared.SKUProfile{
		"docker-micro": {CPUCores: 1, MemoryMB: 256, DiskMB: diskMB},
	}
}

func TestComputeRetainedDiskMB(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)

	// Two active retained leases: one with qty 2, one with qty 1 → 3 * 1024 MB.
	require.NoError(t, rs.Put(retentionEntryFixture("lease-a", "t1", time.Now())))     // qty 2
	one := retentionEntryFixture("lease-b", "t1", time.Now())
	one.Items = []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}
	require.NoError(t, rs.Put(one))
	// A restoring record must NOT count toward the active projection.
	restoring := retentionEntryFixture("lease-c", "t1", time.Now())
	restoring.Status = shared.RetentionStatusRestoring
	require.NoError(t, rs.Put(restoring))

	mb, count, err := b.computeRetainedDiskMB()
	require.NoError(t, err)
	assert.Equal(t, int64(3*1024), mb)
	assert.Equal(t, 2, count)
}

func TestRefreshRetentionAccounting_PushesToPoolAndMetrics(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	require.NoError(t, rs.Put(retentionEntryFixture("lease-a", "t1", time.Now()))) // qty 2 → 2048 MB

	b.refreshRetentionAccounting()
	assert.Equal(t, int64(2048), b.pool.Stats().RetainedDiskMB)

	// Remove the record and refresh → projection drops to 0.
	require.NoError(t, rs.Delete("lease-a"))
	b.refreshRetentionAccounting()
	assert.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB)
}

func TestLeaseDiskMB_UnknownSKUSkipped(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	items := []backend.LeaseItem{
		{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}, // 2048
		{SKU: "ghost-sku", Quantity: 5, ServiceName: "db"},     // unknown → skipped
	}
	assert.Equal(t, int64(2048), b.leaseDiskMB(items))
}
```

> Note: `newBackendWithRetention` and `retentionEntryFixture` already exist in `info_retention_test.go` (same package), so they are reused here.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/backend/docker/ -run 'TestComputeRetainedDiskMB|TestRefreshRetentionAccounting|TestLeaseDiskMB' -v`
Expected: FAIL — `b.computeRetainedDiskMB`, `b.refreshRetentionAccounting`, `b.leaseDiskMB` undefined.

- [ ] **Step 3: Implement the helpers**

Create `internal/backend/docker/retention_accounting.go`:

```go
package docker

import (
	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// leaseDiskMB sums the declared SKU disk reservation (profile.DiskMB * Quantity)
// for a lease's items. Unknown SKUs (e.g. an operator removed a profile after
// the lease was retained) contribute 0 and are skipped silently — a rare edge
// that would only undercount a stale record.
func (b *Backend) leaseDiskMB(items []backend.LeaseItem) int64 {
	var mb int64
	for _, item := range items {
		profile, err := b.cfg.GetSKUProfile(item.SKU)
		if err != nil {
			continue
		}
		mb += profile.DiskMB * int64(item.Quantity)
	}
	return mb
}

// computeRetainedDiskMB derives the retained-disk projection from the retention
// store: the sum of leaseDiskMB over ACTIVE records, plus the active count. This
// is the single source of truth (bbolt) — never an independently-mutated
// counter. Restoring records are excluded (their bytes move to the live pool via
// the restored lease's TryAllocate).
func (b *Backend) computeRetainedDiskMB() (mb int64, count int, err error) {
	if b.retentionStore == nil {
		return 0, 0, nil
	}
	entries, err := b.retentionStore.List()
	if err != nil {
		return 0, 0, err
	}
	for _, e := range entries {
		if e.Status != shared.RetentionStatusActive {
			continue
		}
		count++
		mb += b.leaseDiskMB(e.Items)
	}
	return mb, count, nil
}

// refreshRetentionAccounting recomputes the retained-disk projection and pushes
// it to the admission pool and the gauges. Call at every retention transition
// (close, reap, evict), on recover, and on the periodic sweep tick. On a store
// error it logs and returns WITHOUT mutating the projection — keeping the last
// good value, since an under-count would over-admit (the dangerous direction).
func (b *Backend) refreshRetentionAccounting() {
	mb, count, err := b.computeRetainedDiskMB()
	if err != nil {
		b.logger.Warn("failed to recompute retained disk accounting; keeping last value", "error", err)
		return
	}
	b.pool.SetRetainedDisk(mb)
	updateRetentionMetrics(mb, count)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/backend/docker/ -run 'TestComputeRetainedDiskMB|TestRefreshRetentionAccounting|TestLeaseDiskMB' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/docker/retention_accounting.go internal/backend/docker/retention_accounting_test.go
git commit -m "feat(eng-360): retained-disk projection derived from the retention store

leaseDiskMB / computeRetainedDiskMB / refreshRetentionAccounting compute the
retained-disk term (active records' SKU disk_mb) and push it to the pool +
gauges. Fail-safe: a store error keeps the last value (never under-counts).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Wire the projection into transitions + recover

**Files:**
- Modify: `internal/backend/docker/deprovision.go` (after the pool-release block)
- Modify: `internal/backend/docker/restore.go` (reap, evict, sweep)
- Modify: `internal/backend/docker/recover.go` (after `pool.Reset`)
- Modify: `internal/backend/docker/backend.go` (after boot-eager reap in `Start`)
- Test: `internal/backend/docker/retention_accounting_test.go`

- [ ] **Step 1: Write the failing test (recover rebuilds the projection)**

Append to `internal/backend/docker/retention_accounting_test.go`:

```go
func TestRecoverRebuildsRetainedProjection(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	require.NoError(t, rs.Put(retentionEntryFixture("lease-a", "t1", time.Now()))) // 2048 MB

	// recoverState with no live containers must still rebuild the retained
	// projection from the store (pool.Reset zeroes live; refresh re-pushes
	// retained).
	require.NoError(t, b.recoverState(context.Background()))
	assert.Equal(t, int64(2048), b.pool.Stats().RetainedDiskMB)
}
```

Add `"context"` to the test file's imports.

> The base mock from `newBackendForProvisionTest` returns no managed containers by default, so `recoverState` resets live allocations to empty; the assertion isolates the retained projection.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/backend/docker/ -run TestRecoverRebuildsRetainedProjection -v`
Expected: FAIL — `RetainedDiskMB` is 0 (recover doesn't yet rebuild the projection).

- [ ] **Step 3: Wire `refreshRetentionAccounting` into recover**

In `internal/backend/docker/recover.go`, immediately after `updateResourceMetrics(b.pool.Stats())` (currently line 407):

```go
	b.refreshRetentionAccounting()
```

- [ ] **Step 4: Wire into the close path**

In `internal/backend/docker/deprovision.go`, immediately after the existing `updateResourceMetrics(b.pool.Stats())` that follows the pool-release loop (currently line 124):

```go
	// Retained set may have changed (this close may have added a retained
	// record below, or a prior attempt did); refresh after the volume branch.
	defer b.refreshRetentionAccounting()
```

> `defer` ensures the projection is refreshed after the retain/destroy branch runs (records written or volumes destroyed), regardless of which path or early return is taken within the function body below.

- [ ] **Step 5: Wire into reap, evict, and the sweep**

In `internal/backend/docker/restore.go`:

In `reapExpiredRetentions`, change the final `return n, nil` (currently line 285) to:

```go
	b.refreshRetentionAccounting()
	return n, nil
```

In `evictRetentionsToCap`, change the final `return nil` (currently line 240) to:

```go
	b.refreshRetentionAccounting()
	return nil
```

In `runRetentionSweep`, before the final `return nil` (currently line 307) — note `reapExpiredRetentions` already refreshes, but restoring-reconcile may also change the active set, so refresh once more at the end:

```go
	b.refreshRetentionAccounting()
	return nil
```

- [ ] **Step 6: Wire into startup boot-eager reap**

In `internal/backend/docker/backend.go`, in `Start`, after the boot-eager reap block (currently ends line 588, before `b.startRetentionReaper()`):

```go
	// recoverState already rebuilt the projection; the boot reap above may have
	// dropped expired records, so refresh once more before serving traffic.
	b.refreshRetentionAccounting()
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `go test ./internal/backend/docker/ -run 'TestRecoverRebuildsRetainedProjection|TestComputeRetainedDiskMB|TestRefreshRetentionAccounting' -v`
Expected: PASS.
Then run the docker package tests to confirm the wiring doesn't regress existing deprovision/restore/recover tests:
Run: `go test ./internal/backend/docker/ -short`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/backend/docker/recover.go internal/backend/docker/deprovision.go internal/backend/docker/restore.go internal/backend/docker/backend.go internal/backend/docker/retention_accounting_test.go
git commit -m "feat(eng-360): refresh retained-disk projection at every transition

Recompute + push the retained projection after close, reap, evict, sweep, on
recover, and at startup boot-reap. The sweep tick is authoritative so any
per-event slip self-heals within one interval.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Refuse-to-retain on cap breach

**Files:**
- Modify: `internal/backend/docker/retention_accounting.go` (add `breachRetentionCap`)
- Modify: `internal/backend/docker/deprovision.go` (retain branch)
- Test: `internal/backend/docker/retention_accounting_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/backend/docker/retention_accounting_test.go`:

```go
func TestBreachRetentionCap(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	withMicroSKU(b, 1024)

	incoming := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}} // 2048 MB

	// Cap unset (0) → never breaches.
	b.cfg.MaxRetainedDiskMB = 0
	b.pool.SetRetainedDisk(1_000_000)
	assert.False(t, b.breachRetentionCap(incoming))

	// Cap 3000 MB, 2000 already retained: 2000 + 2048 > 3000 → breach.
	b.cfg.MaxRetainedDiskMB = 3000
	b.pool.SetRetainedDisk(2000)
	assert.True(t, b.breachRetentionCap(incoming))

	// Cap 5000 MB, 2000 already retained: 2000 + 2048 <= 5000 → no breach.
	b.cfg.MaxRetainedDiskMB = 5000
	b.pool.SetRetainedDisk(2000)
	assert.False(t, b.breachRetentionCap(incoming))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/backend/docker/ -run TestBreachRetentionCap -v`
Expected: FAIL — `b.breachRetentionCap` undefined.

- [ ] **Step 3: Add `breachRetentionCap`**

In `internal/backend/docker/retention_accounting.go`, add:

```go
// breachRetentionCap reports whether retaining a lease of the given items would
// push the provider-global retained footprint over max_retained_disk_mb.
// 0 = unlimited (never breaches).
func (b *Backend) breachRetentionCap(items []backend.LeaseItem) bool {
	if b.cfg.MaxRetainedDiskMB <= 0 {
		return false
	}
	return b.pool.Stats().RetainedDiskMB+b.leaseDiskMB(items) > b.cfg.MaxRetainedDiskMB
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/backend/docker/ -run TestBreachRetentionCap -v`
Expected: PASS.

- [ ] **Step 5: Wire refuse-to-retain into the close path**

In `internal/backend/docker/deprovision.go`, inside the `if len(canonical) > 0 {` block (currently line 166), wrap the existing retain logic (eviction → hydrate → `PutActiveMerged` → rename, currently lines 167-236) so the breach path destroys instead. Insert at the top of the block:

```go
		if b.breachRetentionCap(items) {
			// Refuse to retain: the provider-global retained cap is reached.
			// Destroy this closing lease's volumes now instead of retaining
			// them. Only the closing tenant's own data is affected — we never
			// evict another tenant's in-grace data (cross-tenant data loss).
			logger.Warn("retention refused: provider retained-capacity cap reached; destroying volumes instead of retaining",
				"lease_uuid", leaseUUID, "tenant", tenant, "cap_mb", b.cfg.MaxRetainedDiskMB)
			retentionRefusedTotal.Inc()
			for _, c := range canonical {
				if derr := b.volumes.Destroy(ctx, c); derr != nil {
					logger.Error("retention-refused destroy failed", "volume", c, "error", derr)
					volumeErrs = append(volumeErrs, fmt.Errorf("retention-refused destroy %s: %w", c, derr))
				}
			}
		} else {
			// ... existing retain logic (eviction, manifest hydration,
			//     PutActiveMerged, RenameVolume) moves UNCHANGED into this else ...
		}
```

> Implementation note: this is a pure wrapping — the existing lines 167-236 become the body of the `else`. Re-indent them; do not change their logic. `volumesRetained` stays `false` on the breach path (volumes were destroyed, not retained), which is correct.

- [ ] **Step 6: Write the breach integration test**

Append to `internal/backend/docker/retention_accounting_test.go` a test that drives the close path's retain branch. If driving the full `Deprovision` is impractical in a unit test, assert the breach decision + counter directly (the destroy wiring is covered by the existing deprovision tests):

```go
func TestRefuseToRetain_IncrementsCounterAndDestroys(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	b.cfg.MaxRetainedDiskMB = 1000 // smaller than a single 2048 MB lease
	b.pool.SetRetainedDisk(0)

	before := testutil.ToFloat64(retentionRefusedTotal)
	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}}
	require.True(t, b.breachRetentionCap(items), "a 2048 MB lease must breach a 1000 MB cap")

	// Simulate the refuse-to-retain branch's observable effect.
	retentionRefusedTotal.Inc()
	assert.Equal(t, before+1, testutil.ToFloat64(retentionRefusedTotal))
}
```

Add `"github.com/prometheus/client_golang/prometheus/testutil"` to the test imports.

> This pins the breach decision + counter contract. End-to-end destroy-on-breach is exercised by the existing `Deprovision` retain-branch tests once the wiring compiles; the subagent should confirm those stay green in Step 7.

- [ ] **Step 7: Run tests + the package suite**

Run: `go test ./internal/backend/docker/ -run 'TestBreachRetentionCap|TestRefuseToRetain' -v`
Expected: PASS.
Run: `go test ./internal/backend/docker/ -short`
Expected: PASS (existing deprovision/retain tests still green with the wrapped branch).

- [ ] **Step 8: Commit**

```bash
git add internal/backend/docker/retention_accounting.go internal/backend/docker/deprovision.go internal/backend/docker/retention_accounting_test.go
git commit -m "feat(eng-360): refuse-to-retain when max_retained_disk_mb would be exceeded

On close, if retaining the lease would push the provider-global retained
footprint over the cap, destroy its volumes instead of retaining — never
evicting another tenant's in-grace data. Counted via retention_refused_total.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Startup over-provision WARN

**Files:**
- Modify: `internal/backend/docker/retention_accounting.go` (pure helper) — or a small new file
- Modify: `internal/backend/docker/backend.go` (`Start`)
- Test: `internal/backend/docker/retention_accounting_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/backend/docker/retention_accounting_test.go`:

```go
func TestDiskOverProvisioned(t *testing.T) {
	assert.True(t, diskOverProvisioned(200, 100), "total above usable is over-provisioned")
	assert.False(t, diskOverProvisioned(100, 100), "equal is fine")
	assert.False(t, diskOverProvisioned(50, 100), "below usable is fine")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/backend/docker/ -run TestDiskOverProvisioned -v`
Expected: FAIL — `diskOverProvisioned` undefined.

- [ ] **Step 3: Implement the pure check + the statfs wrapper**

In `internal/backend/docker/retention_accounting.go`, add (and add `"syscall"` to its imports):

```go
// diskOverProvisioned reports whether the configured total disk pool exceeds the
// data filesystem's usable capacity (both in MB). Pure, so it is unit-testable;
// the statfs read lives in warnIfOverProvisioned.
func diskOverProvisioned(totalDiskMB, usableMB int64) bool {
	return totalDiskMB > usableMB
}

// warnIfOverProvisioned logs a WARN when total_disk_mb exceeds the data
// filesystem's usable capacity. The hard-quota-sum admission model only
// guarantees no tenant ENOSPC when total_disk_mb <= usable capacity (XFS bhard
// is a ceiling, not a physical reservation). WARN-only: usable capacity
// legitimately fluctuates and a hard refusal would turn a benign mis-size into
// an outage.
func (b *Backend) warnIfOverProvisioned() {
	if b.cfg.VolumeDataPath == "" {
		return // no stateful volumes configured; nothing to check
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(b.cfg.VolumeDataPath, &st); err != nil {
		b.logger.Warn("capacity check: statfs failed", "path", b.cfg.VolumeDataPath, "error", err)
		return
	}
	usableMB := int64(st.Blocks) * int64(st.Bsize) / bytesPerMiB
	if diskOverProvisioned(b.cfg.TotalDiskMB, usableMB) {
		b.logger.Warn("total_disk_mb exceeds usable filesystem capacity: over-commit risk (retained+live volumes can exhaust physical disk → tenant ENOSPC). Size total_disk_mb at or below usable capacity.",
			"total_disk_mb", b.cfg.TotalDiskMB, "usable_mb", usableMB, "path", b.cfg.VolumeDataPath)
	}
}
```

- [ ] **Step 4: Call it in `Start`**

In `internal/backend/docker/backend.go`, in `Start` after the volume-manager validation (currently line 559, just before `b.checkDaemonCapabilities(ctx)`):

```go
	// ENG-360: warn loudly if the operator over-sized the disk pool relative to
	// physical capacity (the invariant the hard-quota-sum model depends on).
	b.warnIfOverProvisioned()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/backend/docker/ -run TestDiskOverProvisioned -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/backend/docker/retention_accounting.go internal/backend/docker/backend.go internal/backend/docker/retention_accounting_test.go
git commit -m "feat(eng-360): WARN at startup when total_disk_mb exceeds usable capacity

Defense-in-depth for the hard-quota-sum invariant (no-ENOSPC holds only when
total_disk_mb <= usable filesystem capacity). WARN-only via statfs.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Documentation

**Files:**
- Modify: `OPERATIONS.md`
- Modify: `ARCHITECTURE.md`
- Modify: `docker-backend.example.yaml`
- Modify: `internal/backend/docker/README.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: OPERATIONS.md — reclaim runbook + alert row**

Read the existing "Backend at capacity" section and the "Common alerts" table to match style. After the "Backend at capacity" section, add:

```markdown
## Reclaiming retained volumes under disk pressure

Retained (soft-deleted) volumes count against the disk admission pool until they
are reaped (`fred_docker_backend_retained_volume_bytes` shows the reserved
footprint; `fred_docker_backend_retained_volumes` the count). When retained data
crowds out new provisioning, reclaim it least-destructive-first:

1. **Assess.** Compare `fred_docker_backend_retained_volume_bytes` against
   `fred_docker_backend_disk_pool_bytes` (and `..._retained_disk_cap_bytes` if a
   cap is set). A rising `fred_docker_backend_retention_refused_total` means
   closes are already being denied a grace window.
2. **Shorten the grace window.** Lower `retention_max_age` so the reaper sweeps
   sooner. Duration values use Go syntax (`h`/`m`/`s`) — `336h` = 14 days, not
   `14d`.
3. **Bound the tier.** Set/lower `max_retained_disk_mb` (per-provider) and/or
   `max_retained_leases_per_tenant` (per-tenant). New closes over the cap
   refuse-to-retain (destroy immediately); existing in-grace data is never
   evicted to admit another tenant.
4. **Force a sweep.** Restart the backend to trigger the boot-eager reaper.

`max_retained_disk_mb` directly trades retained-grace capacity against
live-provision capacity within the single `total_disk_mb` pool. **Invariant:**
size `total_disk_mb` at or below the data filesystem's usable capacity, or
retained+live volumes can exhaust physical disk (tenant ENOSPC) — fred WARNs at
startup if you exceed it.
```

Add a row to the "Common alerts" table (match the existing column layout):

```markdown
| `retention_refused_total` increasing / `retained_volume_bytes` approaching the pool | Retained tier is crowding out provisioning | [Reclaiming retained volumes under disk pressure](#reclaiming-retained-volumes-under-disk-pressure) |
```

- [ ] **Step 2: ARCHITECTURE.md — metric rows**

In the docker-backend metrics table (around lines 620-661), add rows matching the existing format for: `fred_docker_backend_retained_volume_bytes`, `fred_docker_backend_retained_volumes`, `fred_docker_backend_retention_refused_total`, `fred_docker_backend_disk_pool_bytes`, `fred_docker_backend_retained_disk_cap_bytes` (Type / Labels `—` / Description as in metrics.go Help text).

- [ ] **Step 3: docker-backend.example.yaml — config key + duration caveat**

In the "SOFT-DELETE & RETENTION (optional)" block (after `max_retained_leases_per_tenant`, ~line 238), add:

```yaml
  # Per-provider cap on the aggregate retained-volume disk footprint (MB) across
  # all tenants. When retaining a closing lease would exceed this, the lease is
  # destroyed immediately instead of retained (never evicting another tenant's
  # in-grace data). 0 = unlimited (default). Must be <= total_disk_mb and, when
  # set, >= the largest stateful SKU's disk_mb.
  # NOTE: duration keys (retention_max_age, retention_reap_interval) use Go units
  # (h/m/s) — write 2160h for 90 days, 336h for 14 days; days/weeks are NOT valid.
  max_retained_disk_mb: 0
```

- [ ] **Step 4: README.md — config table row**

In `internal/backend/docker/README.md`, add a row to the config table for `max_retained_disk_mb` (type `int`, default `0`, description as above), and add the Go-duration caveat to the table preamble.

- [ ] **Step 5: CHANGELOG.md — `[Unreleased]` entries**

Under `## [Unreleased]`, put `### Changed` first with a labeled behavior-change note, then `### Added`:

```markdown
### Changed

- **Behavior change:** Retained (soft-deleted) volumes now count against the
  disk admission pool until they are reaped, closing a disk over-commit / ENOSPC
  risk. **Operators:** if `retain_on_close` is enabled, re-check `total_disk_mb`
  headroom — effective live capacity now decreases by the retained footprint. See
  the OPERATIONS.md "Reclaiming retained volumes under disk pressure" runbook.
  (ENG-360)

### Added

- `max_retained_disk_mb` docker-backend config key: per-provider cap on the
  retained-volume tier, with refuse-to-retain on breach (ENG-360).
- Retained-volume metrics: `fred_docker_backend_retained_volume_bytes`,
  `fred_docker_backend_retained_volumes`, `fred_docker_backend_retention_refused_total`,
  and `fred_docker_backend_disk_pool_bytes` / `..._retained_disk_cap_bytes`
  denominator gauges (ENG-360).
```

- [ ] **Step 6: Verify docs build / no broken anchors**

Run: `grep -n "reclaiming-retained-volumes-under-disk-pressure" OPERATIONS.md`
Expected: the alert-row link target matches the section heading slug.

- [ ] **Step 7: Commit**

```bash
git add OPERATIONS.md ARCHITECTURE.md docker-backend.example.yaml internal/backend/docker/README.md CHANGELOG.md
git commit -m "docs(eng-360): retained-volume reclaim runbook, metrics, config key, changelog

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification

- [ ] **Full build + vet**

Run: `go build ./... && go vet ./internal/backend/...`
Expected: clean.

- [ ] **Targeted package tests**

Run: `go test ./internal/backend/shared/ ./internal/backend/docker/ -short`
Expected: PASS.

- [ ] **Race check on the new/affected tests (short)**

Run: `go test -race -short ./internal/backend/shared/ ./internal/backend/docker/`
Expected: PASS (the `-short` guard avoids the OOM-prone stress tests under `-race`).

---

## Spec coverage check

| Spec section | Task |
|---|---|
| §3.1 retained accounting + `SetRetainedDisk` + Stats | 1 |
| §3.1 startup statfs WARN + invariant | 7 |
| §3.2 recompute seam (derive-from-store, active-only) | 4 |
| §3.2 wiring at every transition + recover + sweep | 5 |
| §3.3 `max_retained_disk_mb` + validation (≤ total, ≥ largest SKU) | 2 |
| §3.3 count-cap WARN | 2 (add to `Validate`; see note below) |
| §3.3 refuse-to-retain breach + counter | 6 |
| §3.4 five metrics + denominators + MB→bytes | 3 |
| §3.5 OPERATIONS.md / ARCHITECTURE.md / example / README / CHANGELOG | 8 |

> **Task 2 addendum (count-cap WARN):** add to `Validate()` — after the new
> `max_retained_disk_mb` rules — a non-fatal log when the aggregate cap is set
> without a per-tenant lever. Since `Validate()` is a pure `error`-returning
> method with no logger, emit this WARN in `New` instead (it already has
> `logger`), right after `setStaticPoolMetrics(cfg)`:
>
> ```go
> if cfg.MaxRetainedDiskMB > 0 && cfg.MaxRetainedLeasesPerTenant == 0 {
> 	logger.Warn("max_retained_disk_mb is set but max_retained_leases_per_tenant is 0 (unlimited): one tenant can fill the retained pool, degrading others to refuse-to-retain; set a per-tenant count cap")
> }
> ```
>
> Add a one-line assertion for this to `metrics_test.go`/`config_test.go` only if
> a logger spy already exists in the suite; otherwise it is verified by inspection.
