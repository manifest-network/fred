package manifest

import (
	"path"
	"strings"
	"testing"
)

// FuzzParsePayload drives the tenant-controlled manifest parser with arbitrary
// bytes. ParsePayload delegates decoding to hardened stdlib, so the goal here is
// not crash-hunting but VALIDATION-INVARIANT hunting: any payload ParsePayload
// *accepts* must satisfy every security property StackManifest.Validate is meant
// to enforce. A parse that returns success while violating one of these is a
// validation bypass (the class the reserved-label-prefix guard defends against).
//
// The oracle deliberately re-derives the invariants from the package's own rule
// sources (serviceNameRe, reservedLabelPrefixes, MaxTmpfsMounts,
// blockedTmpfsPaths) so it tracks the real rules rather than a hand-copied set.
func FuzzParsePayload(f *testing.F) {
	seeds := []string{
		// Accepted shapes.
		`{"image":"nginx"}`,
		`{"image":"nginx","labels":{"app":"web"}}`,
		`{"image":"nginx","tmpfs":["/tmp/cache"]}`,
		`{"services":{"web":{"image":"nginx"}}}`,
		`{"services":{"web":{"image":"nginx"},"db":{"image":"postgres"}}}`,
		`{"services":{"web":{"image":"nginx","depends_on":{"db":{"condition":"service_started"}}},"db":{"image":"postgres"}}}`,
		// Shapes that MUST be rejected — they exercise the reject paths and give
		// the mutator adversarial starting points aimed at the invariants.
		`{"image":"nginx","labels":{"traefik.enable":"true"}}`,
		`{"image":"nginx","labels":{"fred.owner":"x"}}`,
		`{"services":{"WEB":{"image":"nginx"}}}`,
		`{"services":{"-bad":{"image":"nginx"}}}`,
		`{"image":"nginx","tmpfs":["/"]}`,
		`{"image":"nginx","tmpfs":["relative/path"]}`,
		`{"image":"nginx","tmpfs":["/a","/b","/c","/d","/e"]}`,
		// Malformed / degenerate.
		``,
		`{}`,
		`{"services":{}}`,
		`not json`,
		`{"services":{"web":null}}`,
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Invariant 0: never panics (implicit).
		stack, err := ParsePayload(data)
		if err != nil {
			return // rejected input carries no guarantees.
		}
		if stack == nil {
			t.Fatal("ParsePayload returned nil stack with nil error")
		}

		// Invariant 1: at least one service.
		if len(stack.Services) == 0 {
			t.Fatal("accepted stack has zero services")
		}

		for name, svc := range stack.Services {
			// Invariant 2: service names are DNS-label-safe and length-bounded.
			if l := len(name); l == 0 || l > 63 {
				t.Fatalf("accepted service name %q has invalid length %d", name, l)
			}
			if !serviceNameRe.MatchString(name) {
				t.Fatalf("accepted service name %q does not match serviceNameRe", name)
			}
			if svc == nil {
				t.Fatalf("accepted service %q has nil manifest", name)
			}

			// Invariant 3: no label uses a reserved (fred/traefik/…) prefix.
			for key := range svc.Labels {
				for _, prefix := range reservedLabelPrefixes {
					if strings.HasPrefix(key, prefix) {
						t.Fatalf("accepted service %q carries reserved-prefix label %q", name, key)
					}
				}
			}

			// Invariant 4: tmpfs mounts are bounded, absolute, non-root, not a
			// backend-managed/blocked path, not under a sensitive kernel path,
			// and free of duplicates — a full mirror of validateTmpfsPaths so a
			// regression in any of those rules is caught. Uses path (not
			// path/filepath) to match the validator's normalization exactly.
			if n := len(svc.Tmpfs); n > MaxTmpfsMounts {
				t.Fatalf("accepted service %q has %d tmpfs mounts, exceeding MaxTmpfsMounts=%d", name, n, MaxTmpfsMounts)
			}
			seenTmpfs := make(map[string]bool, len(svc.Tmpfs))
			for _, p := range svc.Tmpfs {
				cleaned := path.Clean(p)
				if !path.IsAbs(cleaned) {
					t.Fatalf("accepted service %q has non-absolute tmpfs path %q", name, p)
				}
				if cleaned == "/" {
					t.Fatalf("accepted service %q mounts tmpfs at root", name)
				}
				if blockedTmpfsPaths[cleaned] {
					t.Fatalf("accepted service %q mounts tmpfs at blocked path %q", name, cleaned)
				}
				for sensitive := range sensitiveKernelPaths {
					if strings.HasPrefix(cleaned, sensitive+"/") {
						t.Fatalf("accepted service %q mounts tmpfs %q under sensitive kernel path %q", name, cleaned, sensitive)
					}
				}
				if seenTmpfs[cleaned] {
					t.Fatalf("accepted service %q has duplicate tmpfs path %q", name, cleaned)
				}
				seenTmpfs[cleaned] = true
			}
		}
	})
}
