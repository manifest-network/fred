package docker

import (
	"context"
	"errors"
	"testing"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// gateBackend builds a Backend whose volumes mock returns a fixed usage.
func gateBackend(usage int64, usageErr error) *Backend {
	b := newBackendForTest(&mockDockerClient{}, nil) // cfg.SKUProfiles = defaultTestSKUProfiles()
	b.volumes = &mockVolumeManager{
		UsageFn: func(context.Context, string) (int64, error) { return usage, usageErr },
	}
	return b
}

func gItem(sku, svc string, qty int) backend.LeaseItem {
	return backend.LeaseItem{SKU: sku, ServiceName: svc, Quantity: qty}
}

// gRetVol: fred-retained-{orig}-{svc}-{idx}
func gRetVol(orig, svc string, idx int) string {
	return retainedName(canonicalVolumeName(orig, svc, idx))
}

func gRec(orig string, items []backend.LeaseItem, vols []string) *shared.RetentionEntry {
	return &shared.RetentionEntry{OriginalLeaseUUID: orig, Items: items, RetainedVolumeNames: vols}
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
	b.volumes = &mockVolumeManager{UsageFn: func(context.Context, string) (int64, error) { called = true; return 0, nil }}

	rec := gRec("orig1", []backend.LeaseItem{gItem("docker-micro", "app", 1)}, []string{gRetVol("orig1", "app", 0)})
	newItems := []backend.LeaseItem{gItem("docker-medium", "app", 1)} // 512→2048: promote
	if err := b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-medium"), b.logger); err != nil {
		t.Fatalf("promote should pass: %v", err)
	}
	if called {
		t.Fatalf("promote must not call Usage")
	}
}

func TestCheckDemoteFit_DemoteFitsAtBoundary(t *testing.T) {
	b := gateBackend(2048*bytesPerMiB, nil) // usage == docker-medium cap exactly
	rec := gRec("orig1", []backend.LeaseItem{gItem("docker-large", "app", 1)}, []string{gRetVol("orig1", "app", 0)})
	newItems := []backend.LeaseItem{gItem("docker-medium", "app", 1)} // 4096→2048: demote
	if err := b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-medium"), b.logger); err != nil {
		t.Fatalf("usage==cap must pass: %v", err)
	}
}

func TestCheckDemoteFit_DemoteExceedsByOneByte(t *testing.T) {
	b := gateBackend(2048*bytesPerMiB+1, nil)
	rec := gRec("orig1", []backend.LeaseItem{gItem("docker-large", "app", 1)}, []string{gRetVol("orig1", "app", 0)})
	newItems := []backend.LeaseItem{gItem("docker-medium", "app", 1)}
	err := b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-medium"), b.logger)
	if !errors.Is(err, backend.ErrDemoteDataExceedsTier) {
		t.Fatalf("err = %v; want ErrDemoteDataExceedsTier", err)
	}
}

func TestCheckDemoteFit_UnmeasurableRefuses(t *testing.T) {
	b := gateBackend(0, errors.New("qgroup show failed"))
	rec := gRec("orig1", []backend.LeaseItem{gItem("docker-large", "app", 1)}, []string{gRetVol("orig1", "app", 0)})
	newItems := []backend.LeaseItem{gItem("docker-medium", "app", 1)}
	if !errors.Is(b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-medium"), b.logger), backend.ErrDemoteDataExceedsTier) {
		t.Fatalf("unmeasurable demote must refuse")
	}
}

func TestCheckDemoteFit_MixedLeaseAtomicRefuse(t *testing.T) {
	// app demotes (4096→2048) and overflows; web promotes (512→2048, skipped). Whole restore refuses.
	b := gateBackend(2048*bytesPerMiB+1, nil)
	rec := gRec("orig1",
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
	rec := gRec("orig1", []backend.LeaseItem{gItem("docker-large", "app", 1)}, []string{gRetVol("orig1", "app", 0)})
	newItems := []backend.LeaseItem{gItem("docker-ephemeral", "app", 1)}
	if !errors.Is(b.checkDemoteFit(context.Background(), rec, newItems, gProfiles(b, "docker-ephemeral"), b.logger), backend.ErrDemoteDataExceedsTier) {
		t.Fatalf("demote to DiskMB=0 with retained data must refuse")
	}
}
