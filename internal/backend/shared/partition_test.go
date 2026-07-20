package shared

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestParsePartitionSource(t *testing.T) {
	cases := []struct {
		in      string
		want    PartitionSource
		wantErr string
	}{
		{in: "", want: PartitionSource{Kind: PartitionSourceNone}},
		{in: "manifest.label:com.example.customer", want: PartitionSource{Kind: PartitionSourceManifestLabel, Key: "com.example.customer"}},
		{in: "manifest.env:APP_CUSTOMER_ID", want: PartitionSource{Kind: PartitionSourceManifestEnv, Key: "APP_CUSTOMER_ID"}},
		{in: "manifest.label:com.example:tier", want: PartitionSource{Kind: PartitionSourceManifestLabel, Key: "com.example:tier"}}, // Cut on FIRST colon keeps colon-bearing keys
		{in: "manifest.labels:x", wantErr: "unknown kind"},
		{in: "chain.lease:partition", wantErr: "unknown kind"}, // future source, not yet
		{in: "manifest.label:", wantErr: "want \"<kind>:<key>\""},
		{in: "manifest.label", wantErr: "want \"<kind>:<key>\""},
		{in: "manifest.label:fred.retention", wantErr: "reserved"},
		{in: "manifest.label:traefik.http.routers.x", wantErr: "reserved"},
		{in: "manifest.env:PATH", wantErr: "blocked"},
		{in: "manifest.env:FRED_PARTITION", wantErr: "blocked"},
		{in: "manifest.env:LD_X", wantErr: "blocked"},
		{in: "manifest.label:" + strings.Repeat("k", 129), wantErr: "key too long"},
	}
	for _, tc := range cases {
		got, err := ParsePartitionSource(tc.in)
		if tc.wantErr != "" {
			require.ErrorContains(t, err, tc.wantErr, "in=%q", tc.in)
			continue
		}
		require.NoError(t, err, "in=%q", tc.in)
		require.Equal(t, tc.want, got, "in=%q", tc.in)
	}
}

func TestNormalizePartition(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"cust-4711", "cust-4711", true},
		{"  cust-4711  ", "cust-4711", true}, // trimmed
		{"A.b_C-9", "A.b_C-9", true},
		{strings.Repeat("x", 64), strings.Repeat("x", 64), true}, // boundary
		{strings.Repeat("x", 65), "", false},                     // over
		{"", "", false},                                          // empty = the sentinel, unrepresentable as a value
		{"   ", "", false},                                       // empty after trim
		{"cust 4711", "", false},                                 // interior whitespace
		{"cust/4711", "", false},                                 // charset
		{"cüst", "", false},                                      // non-ASCII
		{"Cust-A", "Cust-A", true},                               // case PRESERVED (no folding, by design)
	}
	for _, tc := range cases {
		got, ok := NormalizePartition(tc.in)
		require.Equal(t, tc.ok, ok, "in=%q", tc.in)
		require.Equal(t, tc.want, got, "in=%q", tc.in)
	}
}

// stackWith builds a StackManifest whose services carry the given label maps.
func stackWith(labels map[string]map[string]string) *manifest.StackManifest {
	services := map[string]*manifest.Manifest{}
	for name, l := range labels {
		services[name] = &manifest.Manifest{Image: "img", Labels: l}
	}
	return &manifest.StackManifest{Services: services}
}

func TestExtractPartition(t *testing.T) {
	labelSrc := PartitionSource{Kind: PartitionSourceManifestLabel, Key: "com.example.customer"}
	envSrc := PartitionSource{Kind: PartitionSourceManifestEnv, Key: "APP_CUSTOMER_ID"}

	cases := []struct {
		name       string
		src        PartitionSource
		in         PartitionInputs
		wantPart   string
		wantReason string
	}{
		{name: "source none", src: PartitionSource{Kind: PartitionSourceNone},
			in: PartitionInputs{Manifest: stackWith(map[string]map[string]string{"app": {"com.example.customer": "c1"}})}},
		{name: "nil manifest", src: labelSrc, in: PartitionInputs{}, wantReason: PartitionReasonNoInput},
		{name: "no services", src: labelSrc, in: PartitionInputs{Manifest: &manifest.StackManifest{}}, wantReason: PartitionReasonNoInput},
		{name: "zero carriers is silent default", src: labelSrc,
			in: PartitionInputs{Manifest: stackWith(map[string]map[string]string{"app": {"other": "x"}})}},
		{name: "single carrier", src: labelSrc,
			in:       PartitionInputs{Manifest: stackWith(map[string]map[string]string{"app": {"com.example.customer": "cust-1"}})},
			wantPart: "cust-1"},
		{name: "sidecar without the key is not divergence", src: labelSrc,
			in: PartitionInputs{Manifest: stackWith(map[string]map[string]string{
				"app": {"com.example.customer": "cust-1"}, "sidecar": {"unrelated": "y"}})},
			wantPart: "cust-1"},
		{name: "two agreeing carriers", src: labelSrc,
			in: PartitionInputs{Manifest: stackWith(map[string]map[string]string{
				"app": {"com.example.customer": "cust-1"}, "db": {"com.example.customer": "cust-1"}})},
			wantPart: "cust-1"},
		{name: "agreeing after trim", src: labelSrc,
			in: PartitionInputs{Manifest: stackWith(map[string]map[string]string{
				"app": {"com.example.customer": "cust-1"}, "db": {"com.example.customer": "  cust-1 "}})},
			wantPart: "cust-1"},
		{name: "divergent collapses", src: labelSrc,
			in: PartitionInputs{Manifest: stackWith(map[string]map[string]string{
				"app": {"com.example.customer": "cust-1"}, "db": {"com.example.customer": "cust-2"}})},
			wantReason: PartitionReasonDivergent},
		{name: "invalid value collapses", src: labelSrc,
			in:         PartitionInputs{Manifest: stackWith(map[string]map[string]string{"app": {"com.example.customer": "no spaces allowed"}})},
			wantReason: PartitionReasonInvalid},
		{name: "env source", src: envSrc,
			in: PartitionInputs{Manifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				"app": {Image: "img", Env: map[string]string{"APP_CUSTOMER_ID": "cust-9"}}}}},
			wantPart: "cust-9"},
		{name: "nil service entry is skipped not panicked", src: labelSrc,
			in: PartitionInputs{Manifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				"app":   {Image: "img", Labels: map[string]string{"com.example.customer": "cust-1"}},
				"ghost": nil,
			}}},
			wantPart: "cust-1"},
		{name: "divergent detail stays bounded for a huge value", src: labelSrc,
			in: PartitionInputs{Manifest: stackWith(map[string]map[string]string{
				"app": {"com.example.customer": "cust-1"},
				"db":  {"com.example.customer": strings.Repeat("x", 1<<20)}})},
			wantReason: PartitionReasonDivergent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			part, reason, detail := ExtractPartition(tc.src, tc.in)
			require.Equal(t, tc.wantPart, part)
			require.Equal(t, tc.wantReason, reason)
			if reason == PartitionReasonInvalid || reason == PartitionReasonDivergent {
				// Collapse WARNs carry the (truncated) offending value(s); the
				// detail is pre-truncated and bounded.
				require.NotEmpty(t, detail)
				require.LessOrEqual(t, len(detail), 200)
			} else {
				require.Empty(t, detail)
			}
		})
	}
}

func TestTruncatePartitionRaw(t *testing.T) {
	require.Equal(t, "short", TruncatePartitionRaw("short"))
	long := strings.Repeat("x", 100)
	got := TruncatePartitionRaw(long)
	require.True(t, strings.HasPrefix(got, strings.Repeat("x", 64)))
	require.LessOrEqual(t, len(got), 64+len("…(truncated)"))
}

// A multi-byte rune straddling the byte cap must not be sliced into a lone
// lead/continuation byte: the result would be invalid UTF-8 in operator logs.
// Byte 63 begins "€" (U+20AC, 3 bytes), so a naive raw[:64] emits a bare 0xE2.
func TestTruncatePartitionRaw_DoesNotSplitRune(t *testing.T) {
	raw := strings.Repeat("x", 63) + "€" + strings.Repeat("y", 10)
	got := TruncatePartitionRaw(raw)
	require.True(t, utf8.ValidString(got), "truncated value must stay valid UTF-8, got %q", got)
	require.LessOrEqual(t, len(got), maxPartitionValueLen+len("…(truncated)"))
	require.True(t, strings.HasSuffix(got, "…(truncated)"))
	// The "€" straddles the cap, so it must be dropped whole, not partially.
	require.Equal(t, strings.Repeat("x", 63)+"…(truncated)", got)
}
