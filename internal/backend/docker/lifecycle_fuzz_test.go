package docker

import (
	"archive/tar"
	"bytes"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildBenignTar returns a well-formed archive: one directory and one regular
// file inside it. Baseline "happy path" seed.
func buildBenignTar(t testing.TB) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "conf/", Typeflag: tar.TypeDir, Mode: 0o755}))
	body := []byte("key=value\n")
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "conf/app.conf", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(body))}))
	_, err := tw.Write(body)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

// buildTraversalTar returns an archive with a single ".."-escaping entry name.
func buildTraversalTar(t testing.TB) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	body := []byte("evil")
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(body))}))
	_, err := tw.Write(body)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

// buildEscapingSymlinkTar returns an archive that first creates a symlink
// pointing at the filesystem root, then tries to write a regular file
// *through* that symlink. os.Root must keep the write confined to destDir.
func buildEscapingSymlinkTar(t testing.TB) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/", Mode: 0o777}))
	body := []byte("pwn")
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "link/pwn", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(body))}))
	_, err := tw.Write(body)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

// buildSizeOverflowTar returns an archive that defeats the byte-budget gate via
// int64 overflow. Entry 1 is an honest small file that advances the running
// total above zero; entry 2 declares a near-MaxInt64 size (so totalBytes+Size
// wraps negative and slips past the "> maxBytes" gate) while streaming only a
// short body. The writer is intentionally left unclosed so entry 2 stays
// truncated — mirroring a tenant image that streams real bytes under a bogus
// huge declared size. This is the ENG-520 reproducer.
func buildSizeOverflowTar(t testing.TB) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "a", Typeflag: tar.TypeReg, Mode: 0o644, Size: 512}))
	_, err := tw.Write(bytes.Repeat([]byte("A"), 512))
	require.NoError(t, err)

	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name:     "b",
		Typeflag: tar.TypeReg,
		Mode:     0o644,
		Size:     math.MaxInt64 - 100,
		Format:   tar.FormatPAX,
	}))
	_, err = tw.Write(bytes.Repeat([]byte("B"), 4096))
	require.NoError(t, err)

	// No tw.Close(): Close would reject the size mismatch and append EOF blocks.
	// We want the malicious entry left truncated.
	return buf.Bytes()
}

// sumRegularFileBytes returns the total on-disk size of regular files under
// dir. It deliberately ignores the return value of sanitizeAndExtractTar and
// measures what actually landed on disk, because the byte-budget can be
// bypassed on the error path where the returned count reads zero.
func sumRegularFileBytes(t testing.TB, dir string) int64 {
	t.Helper()
	var total int64
	// Fail hard on any walk or stat error: this feeds a security invariant, so a
	// silent walk failure that undercounts bytes-on-disk must never let the
	// budget assertion pass by default.
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return ierr
		}
		total += info.Size()
		return nil
	})
	require.NoErrorf(t, err, "walking %s to sum on-disk bytes", dir)
	return total
}

// TestSanitizeAndExtractTar_SizeGateOverflow is the deterministic ENG-520
// regression: an int64 overflow in the size gate must not let extraction write
// more than maxBytes to disk.
func TestSanitizeAndExtractTar_SizeGateOverflow(t *testing.T) {
	const maxBytes int64 = 1024
	raw := buildSizeOverflowTar(t)
	dest := t.TempDir()

	written, _, extractErr := sanitizeAndExtractTar(bytes.NewReader(raw), dest, maxBytes)

	onDisk := sumRegularFileBytes(t, dest)
	require.LessOrEqualf(t, onDisk, maxBytes,
		"extraction wrote %d bytes to disk, exceeding the %d-byte cap via int64 overflow in the size gate (returned written=%d err=%v)",
		onDisk, maxBytes, written, extractErr)
}

// FuzzSanitizeAndExtractTar drives the tenant-controlled tar extractor with
// arbitrary archive bytes and byte budgets, asserting three security
// invariants on every input:
//
//  1. it never panics;
//  2. the byte budget holds on disk (no write exceeds maxBytes), even on the
//     error path — this catches the ENG-520 int64-overflow cap bypass;
//  3. containment: a canary file beside destDir is never touched and no real
//     (non-symlink) path is ever created outside destDir.
func FuzzSanitizeAndExtractTar(f *testing.F) {
	f.Add(buildBenignTar(f), int64(1<<20))
	f.Add(buildTraversalTar(f), int64(1<<20))
	f.Add(buildEscapingSymlinkTar(f), int64(1<<20))
	f.Add(buildSizeOverflowTar(f), int64(1024))

	const canaryContent = "do-not-touch"

	f.Fuzz(func(t *testing.T, tarBytes []byte, maxBytes int64) {
		sandbox := t.TempDir()
		dest := filepath.Join(sandbox, "dest")
		require.NoError(t, os.MkdirAll(dest, 0o700))
		canaryPath := filepath.Join(sandbox, "CANARY")
		require.NoError(t, os.WriteFile(canaryPath, []byte(canaryContent), 0o600))

		// Invariant 1: never panics (a panic fails the test implicitly).
		_, _, _ = sanitizeAndExtractTar(bytes.NewReader(tarBytes), dest, maxBytes)

		// Invariant 2: byte budget holds on disk, even when extraction errored.
		budget := maxBytes
		if budget < 0 {
			budget = 0
		}
		if onDisk := sumRegularFileBytes(t, dest); onDisk > budget {
			t.Fatalf("extraction wrote %d bytes to disk, exceeding maxBytes=%d", onDisk, maxBytes)
		}

		// Invariant 3: containment.
		got, err := os.ReadFile(canaryPath)
		if err != nil || string(got) != canaryContent {
			t.Fatalf("canary mutated or removed: err=%v got=%q", err, got)
		}
		// Fail hard on a walk error rather than risk missing a later path-escape:
		// this is a security invariant, so an aborted walk must not pass silently.
		walkErr := filepath.WalkDir(sandbox, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if p == sandbox || p == canaryPath || p == dest {
				return nil
			}
			if strings.HasPrefix(p, dest+string(os.PathSeparator)) {
				return nil
			}
			// A dangling/escaping symlink *inside* dest is created by design
			// (os.Root confines it); only real paths outside dest are a breach.
			if d.Type()&fs.ModeSymlink != 0 {
				return nil
			}
			t.Fatalf("path escaped destDir: %s", p)
			return nil
		})
		require.NoError(t, walkErr, "walking sandbox for containment check")
	})
}
