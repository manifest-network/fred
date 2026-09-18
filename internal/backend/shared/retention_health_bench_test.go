package shared

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// BenchmarkRetentionHealth measures the full validation of an idle,
// identity-bound journal. 871 is the fleet-wide retained count in the ENG-1006
// sc21 archive, deliberately placed on one backend here; it is a local baseline,
// not a model of the deployment's disks, lock contention, or request latency.
func BenchmarkRetentionHealth(b *testing.B) {
	for _, rows := range []int{0, 871, 4096} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			path, storage := initializeBoundRetentionStore(b)
			store, err := OpenIdentityBoundRetentionStore(
				RetentionStoreConfig{DBPath: path}, storage, newTestStorageAuthorityGate(b))
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, store.Close()) })
			stack, err := manifest.ParsePayload([]byte(`{"services":{"web":{"image":"nginx:1"},"db":{"image":"redis:7"}}}`))
			require.NoError(b, err)
			for index := range rows {
				leaseUUID := fmt.Sprintf("00000000-0000-4000-8000-%012x", index+1)
				require.NoError(b, store.putForTest(RetentionEntry{
					OriginalLeaseUUID: leaseUUID, Tenant: "benchmark-tenant", ProviderUUID: "33333333-3333-4333-8333-333333333333",
					Items: []backend.LeaseItem{
						{SKU: "web-sku", Quantity: 1, ServiceName: "web"},
						{SKU: "db-sku", Quantity: 1, ServiceName: "db"},
					},
					ResourceProfiles: []SKUResourceSnapshot{
						{SKU: "db-sku", CPUCores: 1, MemoryMB: 512, DiskMB: 1024},
						{SKU: "web-sku", CPUCores: 1, MemoryMB: 512, DiskMB: 1024},
					},
					StackManifest: stack, CallbackURL: "https://fred.example/callbacks/provision",
					RetainedVolumeNames: []string{"fred-" + leaseUUID + "-db-0"},
					Status:              RetentionStatusActive, CreatedAt: time.Unix(1, 0).UTC(),
				}))
			}
			require.NoError(b, store.Healthy())
			b.ReportAllocs()
			for b.Loop() {
				if err := store.Healthy(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
