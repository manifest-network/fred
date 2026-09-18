package imageexec

import (
	"fmt"
	"testing"
)

// These measurements isolate admission of already-decoded metadata. The shared
// ImageInspect transport separately bounds response allocation before decoding.
func BenchmarkImageMetadataAdmission(b *testing.B) {
	for _, count := range []int{MaxImageVolumes, 100_000} {
		volumes := make(map[string]struct{}, count)
		for i := range count {
			volumes[fmt.Sprintf("/volume-%06d", i)] = struct{}{}
		}
		b.Run(fmt.Sprintf("volumes_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := admitImageMetadata(nil, volumes)
				if (err != nil) != (count > MaxImageVolumes) {
					b.Fatalf("unexpected metadata admission result: %v", err)
				}
			}
		})
	}
	for _, count := range []int{MaxImageLabels, 100_000} {
		labels := make(map[string]string, count)
		for i := range count {
			labels[fmt.Sprintf("tenant.label.%06d", i)] = "value"
		}
		b.Run(fmt.Sprintf("labels_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, err := admitImageMetadata(labels, nil)
				if (err != nil) != (count > MaxImageLabels) {
					b.Fatalf("unexpected metadata admission result: %v", err)
				}
			}
		})
	}
}
