package docker

import "testing"

func TestVolumeNameHelpers(t *testing.T) {
	if got := canonicalVolumeName("u1", "app", 0); got != "fred-u1-app-0" {
		t.Errorf("canonicalVolumeName: got %q, want %q", got, "fred-u1-app-0")
	}
	if got := retainedName("fred-u1-app-0"); got != "fred-retained-u1-app-0" {
		t.Errorf("retainedName: got %q, want %q", got, "fred-retained-u1-app-0")
	}
	if got := isRetainedVolume("fred-retained-u1-app-0"); !got {
		t.Errorf("isRetainedVolume(retained): got false, want true")
	}
	if got := isRetainedVolume("fred-u1-app-0"); got {
		t.Errorf("isRetainedVolume(canonical): got true, want false")
	}
	if got := leaseVolumePrefix("u1"); got != "fred-u1-" {
		t.Errorf("leaseVolumePrefix: got %q, want %q", got, "fred-u1-")
	}
}
