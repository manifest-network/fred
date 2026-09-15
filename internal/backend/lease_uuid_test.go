package backend

import "testing"

func TestIsCanonicalLeaseUUID(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  bool
	}{
		{"canonical", "550e8400-e29b-41d4-a716-446655440000", true},
		{"uppercase", "550E8400-E29B-41D4-A716-446655440000", false},
		{"compact", "550e8400e29b41d4a716446655440000", false},
		{"braced", "{550e8400-e29b-41d4-a716-446655440000}", false},
		{"nil", "00000000-0000-0000-0000-000000000000", false},
		{"arbitrary", "lease-1", false},
		{"empty", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := IsCanonicalLeaseUUID(test.value); got != test.want {
				t.Fatalf("IsCanonicalLeaseUUID(%q) = %t, want %t", test.value, got, test.want)
			}
		})
	}
}
