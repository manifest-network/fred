package shared

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// Retention-partition extraction. Everything in this file is pure and
// collapse-only: no function here can fail a close — every uncertainty
// degrades to the "" default whole-tenant bucket (the eventual caller is the
// close path, where a hard failure would destroy a closing lease's data).

// PartitionSourceKind enumerates where a retention-partition value is read
// from at lease close. Future kinds (a reserved manifest field, an on-chain
// lease field) are additive switch arms; the store/caps machinery never
// changes with the source.
type PartitionSourceKind int

const (
	PartitionSourceNone PartitionSourceKind = iota
	PartitionSourceManifestLabel
	PartitionSourceManifestEnv
)

// PartitionSource is the parsed retention_partition_source config value.
type PartitionSource struct {
	Kind PartitionSourceKind
	Key  string // label key or env var name; empty iff Kind == PartitionSourceNone
}

// PartitionInputs carries the close-time artifacts a partition can be
// extracted from. A struct (not bare params) so future sources extend it
// without breaking the ExtractPartition signature.
type PartitionInputs struct {
	Manifest *manifest.StackManifest
}

const (
	maxPartitionValueLen     = 64
	maxPartitionSourceKeyLen = 128
)

// Collapse reasons — the CLOSED label set of the partition-collapse metric.
// Declared here (not in the docker package) so extraction and the write-time
// bound share one vocabulary. "" means "did not collapse" (either a valid
// partition or the silent not-declared default).
const (
	PartitionReasonNoInput    = "no_input"    // manifest nil/empty after hydration
	PartitionReasonDivergent  = "divergent"   // carrying services disagree
	PartitionReasonInvalid    = "invalid"     // value fails NormalizePartition
	PartitionReasonOverLimit  = "over_limit"  // produced by the write-time bound (next phase)
	PartitionReasonStoreError = "store_error" // produced by the write-time bound (next phase)
)

// PartitionCollapseReasons is the full closed set, exported for metric
// pre-initialization.
var PartitionCollapseReasons = []string{
	PartitionReasonNoInput, PartitionReasonDivergent, PartitionReasonInvalid,
	PartitionReasonOverLimit, PartitionReasonStoreError,
}

// ParsePartitionSource parses a retention_partition_source config string.
// Grammar: "" | "manifest.label:<key>" | "manifest.env:<key>". Intended to be
// called from config validation, so a malformed or unsatisfiable source is a
// STARTUP failure, never a close-time surprise.
func ParsePartitionSource(s string) (PartitionSource, error) {
	if s == "" {
		return PartitionSource{Kind: PartitionSourceNone}, nil
	}
	kind, key, found := strings.Cut(s, ":")
	if !found || key == "" {
		return PartitionSource{}, fmt.Errorf("%q: want \"<kind>:<key>\" with kind in {manifest.label, manifest.env}", s)
	}
	if len(key) > maxPartitionSourceKeyLen {
		return PartitionSource{}, fmt.Errorf("%q: key too long (%d > %d)", s, len(key), maxPartitionSourceKeyLen)
	}
	switch kind {
	case "manifest.label":
		if manifest.IsReservedLabelKey(key) {
			return PartitionSource{}, fmt.Errorf("label key %q uses a reserved prefix no tenant manifest can declare", key)
		}
		return PartitionSource{Kind: PartitionSourceManifestLabel, Key: key}, nil
	case "manifest.env":
		if manifest.IsBlockedEnvKey(key) {
			return PartitionSource{}, fmt.Errorf("env key %q is blocked/invalid — no tenant manifest can declare it", key)
		}
		return PartitionSource{Kind: PartitionSourceManifestEnv, Key: key}, nil
	default:
		return PartitionSource{}, fmt.Errorf("%q: unknown kind %q (want manifest.label or manifest.env)", s, kind)
	}
}

// NormalizePartition validates a raw declared partition value: trimmed,
// 1..64 chars of [A-Za-z0-9._-]. ok=false means the value is unusable and
// MUST collapse to the default "" bucket. "" is deliberately unrepresentable
// as a valid value — it is exclusively the whole-tenant sentinel, so no
// declared value can collide with it. Case is PRESERVED (no folding):
// fragmentation from case variance is observable, whereas silent merging of
// intentionally-distinct IDs would be invisible.
func NormalizePartition(raw string) (string, bool) {
	v := strings.TrimSpace(raw)
	// len counts BYTES: exact for the ASCII-only charset below, and any
	// multibyte input is rejected by that charset check regardless.
	if v == "" || len(v) > maxPartitionValueLen {
		return "", false
	}
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '_', r == '-':
		default:
			return "", false
		}
	}
	return v, true
}

// TruncatePartitionRaw bounds a tenant-supplied raw value for logging: the
// manifest imposes no per-value length cap (only the 1-2 MiB body caps), so a
// raw value can be ~1 MB. Structured slog quoting neutralizes control
// characters; this bounds size. NO log site may record the untruncated value.
func TruncatePartitionRaw(raw string) string {
	if len(raw) <= maxPartitionValueLen {
		return raw
	}
	// Back off to a UTF-8 rune boundary so the byte-length cut never splits a
	// multi-byte value into a lone lead/continuation byte (invalid UTF-8 in
	// operator logs). A rune is at most 4 bytes, so this drops at most 3.
	end := maxPartitionValueLen
	for end > 0 && !utf8.RuneStart(raw[end]) {
		end--
	}
	return raw[:end] + "…(truncated)"
}

// ExtractPartition derives the candidate partition for a closing lease from
// the configured source. Returns (partition, collapseReason, rawDetail):
//   - ("",  "", "")      — not declared (source none, or no service carries
//     the key): the silent, normal state for every non-participating tenant.
//   - (p,   "", "")      — exactly one distinct declared value, normalized.
//   - ("",  reason, det) — declared but unusable; the caller counts+logs and
//     the record lands in the default bucket. det is the PRE-TRUNCATED
//     offending value (invalid) or the established value and the first
//     divergent one (divergent) for the WARN line — no caller may log
//     anything rawer.
//
// It CANNOT fail: there is no error return, which structurally enforces the
// never-fail-a-close invariant.
func ExtractPartition(src PartitionSource, in PartitionInputs) (partition, reason, rawDetail string) {
	if src.Kind == PartitionSourceNone {
		return "", "", ""
	}
	sm := in.Manifest
	if sm == nil || len(sm.Services) == 0 {
		return "", PartitionReasonNoInput, ""
	}
	// Walk services in sorted name order — determinism of any future logging;
	// the agree-or-collapse decision itself is order-independent.
	names := make([]string, 0, len(sm.Services))
	for name := range sm.Services {
		names = append(names, name)
	}
	sort.Strings(names)
	var (
		found bool
		value string
	)
	for _, name := range names {
		svc := sm.Services[name]
		if svc == nil {
			continue
		}
		var v string
		var ok bool
		switch src.Kind {
		case PartitionSourceManifestLabel:
			v, ok = svc.Labels[src.Key]
		case PartitionSourceManifestEnv:
			v, ok = svc.Env[src.Key]
		}
		if !ok {
			continue // absence is not divergence: label only the main service
		}
		v = strings.TrimSpace(v)
		if found && v != value {
			return "", PartitionReasonDivergent,
				TruncatePartitionRaw(value) + " vs " + TruncatePartitionRaw(v)
		}
		found, value = true, v
	}
	if !found {
		return "", "", "" // zero carriers: silent default (must not spam WARN)
	}
	norm, ok := NormalizePartition(value)
	if !ok {
		return "", PartitionReasonInvalid, TruncatePartitionRaw(value)
	}
	return norm, "", ""
}
