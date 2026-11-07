package docker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// assertDemoteRefused runs fn and asserts the demote-refusal counter for the
// given {backend, reason} label pair incremented by exactly 1. It measures a
// before/after delta on the specific child so it stays correct despite other
// tests sharing the package-level restoreDemoteRefusedTotal CounterVec.
func assertDemoteRefused(t *testing.T, backendKind, reason string, fn func()) {
	t.Helper()
	c := restoreDemoteRefusedTotal.WithLabelValues(backendKind, reason)
	before := testutil.ToFloat64(c)
	fn()
	if got := testutil.ToFloat64(c) - before; got != 1 {
		t.Fatalf("restore_demote_refused_total{backend=%q,reason=%q} delta = %v; want 1", backendKind, reason, got)
	}
}

// gateBackend builds a Backend whose volumes mock returns a fixed usage.
func gateBackend(usage int64, usageErr error) *Backend {
	b := newBackendForTest(&mockDockerClient{}, nil) // cfg.SKUProfiles = defaultTestSKUProfiles()
	volumes := &mockVolumeManager{
		UsageFn: func(context.Context, string) (int64, error) { return usage, usageErr },
	}
	useVolumeManager(b, volumes)
	return b
}

func gItem(sku, svc string, qty int) backend.LeaseItem {
	return backend.LeaseItem{SKU: sku, ServiceName: svc, Quantity: qty}
}

// gRetVol: fred-retained-{orig}-{svc}-{idx}
func gRetVol(orig, svc string, idx int) string {
	return retainedName(canonicalVolumeName(orig, svc, idx))
}

// gRec builds a retained record whose sizing is frozen from b's configuration,
// as Start leaves every retained record.
func gRec(b *Backend, orig string, items []backend.LeaseItem, vols []string) *shared.RetentionEntry {
	profiles, err := b.resolveResourceProfiles(items)
	if err != nil {
		panic(err)
	}
	return &shared.RetentionEntry{
		OriginalLeaseUUID: orig, Items: items, ResourceProfiles: profiles, RetainedVolumeNames: vols,
	}
}

// gProfiles resolves the given SKUs into a profiles map via b.cfg.
func gProfiles(b *Backend, skus ...string) map[string]SKUProfile {
	m := map[string]SKUProfile{}
	for _, s := range skus {
		p, err := b.cfg.GetSKUProfile(s)
		if err != nil {
			panic(err)
		}
		m[s] = p
	}
	return m
}

func TestCheckDemoteFit_PromoteSkipsMeasurement(t *testing.T) {
	called := false
	b := newBackendForTest(&mockDockerClient{}, nil)
	useVolumeManager(b, &mockVolumeManager{UsageFn: func(context.Context, string) (int64, error) { called = true; return 0, nil }})

	rec := gRec(b, "orig1", []backend.LeaseItem{gItem("docker-micro", "app", 1)}, []string{gRetVol("orig1", "app", 0)})
	newItems := []backend.LeaseItem{gItem("docker-medium", "app", 1)} // 512→2048: promote
	if err := b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-medium"), b.logger); err != nil {
		t.Fatalf("promote should pass: %v", err)
	}
	if called {
		t.Fatalf("promote must not call Usage")
	}
}

func TestCheckDemoteFit_RetiredOldSKUUsesItsFrozenProfile(t *testing.T) {
	// "docker-retired" is no longer configured. The record's frozen 512 MB
	// still sizes it, so restoring into docker-medium (2048 MB) is a promote
	// and needs no measurement.
	called := false
	b := newBackendForTest(&mockDockerClient{}, nil)
	useVolumeManager(b, &mockVolumeManager{UsageFn: func(context.Context, string) (int64, error) { called = true; return 0, nil }})
	items := []backend.LeaseItem{gItem("docker-retired", "app", 1)}
	rec := &shared.RetentionEntry{
		OriginalLeaseUUID:   "orig1",
		Items:               items,
		ResourceProfiles:    []shared.SKUResourceSnapshot{{SKU: "docker-retired", CPUCores: 0.5, MemoryMB: 512, DiskMB: 512}},
		RetainedVolumeNames: []string{gRetVol("orig1", "app", 0)},
	}
	newItems := []backend.LeaseItem{gItem("docker-medium", "app", 1)}
	if err := b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-medium"), b.logger); err != nil {
		t.Fatalf("a promote from a retired SKU should pass on its frozen profile: %v", err)
	}
	if called {
		t.Fatalf("promote must not call Usage")
	}
}

func TestCheckDemoteFit_UnfrozenRecordRefusesWithoutMeasuring(t *testing.T) {
	// Start freezes every retained record's sizing, so a record without it is
	// invalid authority. The gate refuses it instead of sizing it from config.
	called := false
	b := newBackendForTest(&mockDockerClient{}, nil)
	useVolumeManager(b, &mockVolumeManager{UsageFn: func(context.Context, string) (int64, error) { called = true; return 0, nil }})
	rec := &shared.RetentionEntry{
		OriginalLeaseUUID:   "orig1",
		Items:               []backend.LeaseItem{gItem("docker-micro", "app", 1)},
		RetainedVolumeNames: []string{gRetVol("orig1", "app", 0)},
	}
	newItems := []backend.LeaseItem{gItem("docker-medium", "app", 1)}
	err := b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-medium"), b.logger)
	if !errors.Is(err, backend.ErrInvalidState) || !errors.Is(err, errRetainedSizingUnfrozen) {
		t.Fatalf("err = %v; want ErrInvalidState wrapping errRetainedSizingUnfrozen", err)
	}
	if called {
		t.Fatalf("an unfrozen record must be refused before any measurement")
	}
}

func TestCheckDemoteFit_DemoteFitsAtBoundary(t *testing.T) {
	b := gateBackend(2048*bytesPerMiB, nil) // usage == docker-medium cap exactly
	rec := gRec(b, "orig1", []backend.LeaseItem{gItem("docker-large", "app", 1)}, []string{gRetVol("orig1", "app", 0)})
	newItems := []backend.LeaseItem{gItem("docker-medium", "app", 1)} // 4096→2048: demote
	if err := b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-medium"), b.logger); err != nil {
		t.Fatalf("usage==cap must pass: %v", err)
	}
}

func TestCheckDemoteFit_DemoteExceedsByOneByte(t *testing.T) {
	b := gateBackend(2048*bytesPerMiB+1, nil)
	rec := gRec(b, "orig1", []backend.LeaseItem{gItem("docker-large", "app", 1)}, []string{gRetVol("orig1", "app", 0)})
	newItems := []backend.LeaseItem{gItem("docker-medium", "app", 1)}
	var err error
	assertDemoteRefused(t, "mock", "measured_exceeds", func() {
		err = b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-medium"), b.logger)
	})
	if !errors.Is(err, backend.ErrDemoteDataExceedsTier) {
		t.Fatalf("err = %v; want ErrDemoteDataExceedsTier", err)
	}
}

func TestCheckDemoteFit_UnmeasurableRefuses(t *testing.T) {
	// A non-ErrUnsupported read failure (e.g. the qgroup command errored) →
	// reason label "unmeasurable_read_error".
	b := gateBackend(0, errors.New("qgroup show failed"))
	rec := gRec(b, "orig1", []backend.LeaseItem{gItem("docker-large", "app", 1)}, []string{gRetVol("orig1", "app", 0)})
	newItems := []backend.LeaseItem{gItem("docker-medium", "app", 1)}
	var err error
	assertDemoteRefused(t, "mock", "unmeasurable_read_error", func() {
		err = b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-medium"), b.logger)
	})
	if !errors.Is(err, backend.ErrDemoteDataExceedsTier) {
		t.Fatalf("unmeasurable demote must refuse")
	}
	// The tenant-facing error must NOT leak the underlying Usage error (which
	// can carry host paths / command output); that detail is logged, not returned.
	if strings.Contains(err.Error(), "qgroup show failed") {
		t.Fatalf("tenant-facing error must not leak the underlying Usage error: %v", err)
	}
}

func TestCheckDemoteFit_UnmeasurableBackendRefuses(t *testing.T) {
	// Usage returns a wrapped errors.ErrUnsupported (noop-style backend that
	// cannot measure at all) → reason label "unmeasurable_backend", distinct
	// from a transient read error.
	b := gateBackend(0, fmt.Errorf("cannot measure usage: %w", errors.ErrUnsupported))
	rec := gRec(b, "orig1", []backend.LeaseItem{gItem("docker-large", "app", 1)}, []string{gRetVol("orig1", "app", 0)})
	newItems := []backend.LeaseItem{gItem("docker-medium", "app", 1)}
	var err error
	assertDemoteRefused(t, "mock", "unmeasurable_backend", func() {
		err = b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-medium"), b.logger)
	})
	if !errors.Is(err, backend.ErrDemoteDataExceedsTier) {
		t.Fatalf("unsupported-usage demote must refuse")
	}
	if strings.Contains(err.Error(), "cannot measure usage") {
		t.Fatalf("tenant-facing error must not leak the underlying Usage error: %v", err)
	}
}

func TestCheckDemoteFit_MixedLeaseAtomicRefuse(t *testing.T) {
	// app (first item) demotes and overflows → refuses before web is even examined.
	b := gateBackend(2048*bytesPerMiB+1, nil)
	rec := gRec(b, "orig1",
		[]backend.LeaseItem{gItem("docker-large", "app", 1), gItem("docker-micro", "web", 1)},
		[]string{gRetVol("orig1", "app", 0), gRetVol("orig1", "web", 0)})
	newItems := []backend.LeaseItem{gItem("docker-medium", "app", 1), gItem("docker-medium", "web", 1)}
	if !errors.Is(b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-medium"), b.logger), backend.ErrDemoteDataExceedsTier) {
		t.Fatalf("mixed lease with one overflowing demote must refuse atomically")
	}
}

func TestCheckDemoteFit_EphemeralTierWithDataRefuses(t *testing.T) {
	b := gateBackend(1, nil)
	b.cfg.SKUProfiles["docker-ephemeral"] = SKUProfile{CPUCores: 0.25, MemoryMB: 256, DiskMB: 0}
	rec := gRec(b, "orig1", []backend.LeaseItem{gItem("docker-large", "app", 1)}, []string{gRetVol("orig1", "app", 0)})
	newItems := []backend.LeaseItem{gItem("docker-ephemeral", "app", 1)}
	var err error
	assertDemoteRefused(t, "mock", "ephemeral_tier", func() {
		err = b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-ephemeral"), b.logger)
	})
	if !errors.Is(err, backend.ErrDemoteDataExceedsTier) {
		t.Fatalf("demote to DiskMB=0 with retained data must refuse")
	}
}
