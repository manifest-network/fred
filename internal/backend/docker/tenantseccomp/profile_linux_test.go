//go:build linux

package tenantseccomp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	mobyseccomp "github.com/moby/profiles/seccomp"
	"github.com/stretchr/testify/require"
)

var updateGolden = flag.Bool("update", false, "regenerate testdata/tenant-seccomp.golden.json from the current build")

const goldenProfilePath = "testdata/tenant-seccomp.golden.json"

func buildTestProfile(t *testing.T) []byte {
	t.Helper()
	compact, err := buildProfile()
	require.NoError(t, err)
	return compact
}

// The golden file is the release record of what the daemon loads. A change
// to the base profile, the encoding, or the dependency shows up here as a
// reviewable diff; regenerate with `go test -run Golden -update`.
func TestTenantProfileMatchesGolden(t *testing.T) {
	compact := buildTestProfile(t)
	if *updateGolden {
		require.NoError(t, os.WriteFile(goldenProfilePath, compact, 0o644))
	}
	golden, err := os.ReadFile(goldenProfilePath)
	require.NoError(t, err)
	require.True(t, bytes.Equal(golden, compact),
		"the emitted profile differs from %s; review the change and regenerate with -update", goldenProfilePath)
	var compacted bytes.Buffer
	require.NoError(t, json.Compact(&compacted, golden))
	require.Equal(t, golden, compacted.Bytes(), "the golden profile must be compact JSON")
}

// A Docker upgrade changes the base profile. Pinning the dependency to the
// version the profile names forces the regeneration to be deliberate.
func TestMobyProfilesVersionMatchesGoMod(t *testing.T) {
	file, err := os.Open(filepath.Join(moduleRoot(t), "go.mod"))
	require.NoError(t, err)
	defer func() { _ = file.Close() }()
	var versions []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(strings.TrimPrefix(strings.TrimSpace(scanner.Text()), "require "))
		if len(fields) >= 2 && fields[0] == "github.com/moby/profiles/seccomp" {
			versions = append(versions, fields[1])
		}
	}
	require.NoError(t, scanner.Err())
	require.Equal(t, []string{MobyProfilesVersion}, versions,
		"go.mod must require github.com/moby/profiles/seccomp at exactly MobyProfilesVersion")
}

// oracleRule is one emitted rule as libseccomp sees it, decoded without the
// production types.
type oracleRule struct {
	names  []string
	action string
	args   []map[string]any
	gated  bool
}

func decodeOracleProfile(t *testing.T, compact []byte) (map[string]any, []oracleRule) {
	t.Helper()
	var document map[string]any
	require.NoError(t, json.Unmarshal(compact, &document))
	rawRules, ok := document["syscalls"].([]any)
	require.True(t, ok)
	rules := make([]oracleRule, 0, len(rawRules))
	for _, raw := range rawRules {
		entry, ok := raw.(map[string]any)
		require.True(t, ok)
		var rule oracleRule
		if names, ok := entry["names"].([]any); ok {
			for _, name := range names {
				rule.names = append(rule.names, name.(string))
			}
		}
		if name, ok := entry["name"].(string); ok && name != "" {
			rule.names = append(rule.names, name)
		}
		rule.action, _ = entry["action"].(string)
		if args, ok := entry["args"].([]any); ok {
			for _, arg := range args {
				rule.args = append(rule.args, arg.(map[string]any))
			}
		}
		_, includes := entry["includes"]
		_, excludes := entry["excludes"]
		rule.gated = includes || excludes
		rules = append(rules, rule)
	}
	return document, rules
}

func jsonUint(t *testing.T, value any) uint64 {
	t.Helper()
	if value == nil {
		return 0 // omitted valueTwo decodes as 0, exactly as the runtime reads it
	}
	number, ok := value.(float64)
	require.True(t, ok, "%v is not a number", value)
	require.Equal(t, float64(uint64(number)), number)
	return uint64(number)
}

// oracleCondition is one MASKED_EQ comparison split into the two 32-bit
// halves libseccomp compares. A half with a zero mask is skipped.
type oracleCondition struct{ maskLo, datumLo, maskHi, datumHi uint32 }

// compileOracle reads the ioctl ALLOW rules with libseccomp's semantics: the
// datum is masked before it is compared, each half whose mask is zero is
// skipped, and a rule left without any comparison is unconditional and allows
// every command.
func compileOracle(t *testing.T, rules []oracleRule) [][]oracleCondition {
	t.Helper()
	var compiled [][]oracleCondition
	for _, rule := range rules {
		if !slices.Contains(rule.names, "ioctl") || rule.action != "SCMP_ACT_ALLOW" {
			continue
		}
		conditions := []oracleCondition{}
		for _, arg := range rule.args {
			require.Equal(t, "SCMP_CMP_MASKED_EQ", arg["op"])
			require.EqualValues(t, 1, jsonUint(t, arg["index"]))
			mask := jsonUint(t, arg["value"])
			datum := jsonUint(t, arg["valueTwo"]) & mask
			conditions = append(conditions, oracleCondition{
				maskLo: uint32(mask), datumLo: uint32(datum),
				maskHi: uint32(mask >> 32), datumHi: uint32(datum >> 32),
			})
		}
		compiled = append(compiled, conditions)
	}
	return compiled
}

func oracleAllows(rules [][]oracleCondition, command uint64) bool {
	lo, hi := uint32(command), uint32(command>>32)
	for _, conditions := range rules {
		matches := true
		for _, c := range conditions {
			if (c.maskLo != 0 && lo&c.maskLo != c.datumLo) || (c.maskHi != 0 && hi&c.maskHi != c.datumHi) {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

// deniedByDesign is the independent statement of the deny set: the
// direction, type and number of FS_IOC_SETFLAGS and FS_IOC_FSSETXATTR,
// whatever the size field and the upper 32 bits hold.
func deniedByDesign(command uint64) bool {
	key := uint32(command) & 0xC000FFFF
	return key == 0x40006602 || key == 0x40005820
}

const (
	fsIocSetflags    = 0x40086602
	fsIoc32Setflags  = 0x40046602
	fsIocFssetxattr  = 0x401C5820
	fsIocGetflags    = 0x80086601
	fsIocFsgetxattr  = 0x801C581F
	ioctlTCGETS      = 0x5401
	ioctlFIONREAD    = 0x541B
	ioctlFICLONE     = 0x40049409
	ioctlTIOCGWINSZ  = 0x5413
	signExtendedHigh = uint64(0xFFFFFFFF) << 32
)

func TestTenantProfileSemantics(t *testing.T) {
	compact := buildTestProfile(t)
	document, rules := decodeOracleProfile(t, compact)

	require.Equal(t, "SCMP_ACT_ERRNO", document["defaultAction"])
	require.EqualValues(t, 1, jsonUint(t, document["defaultErrnoRet"]))

	ioctlRules := 0
	for _, rule := range rules {
		for _, removed := range []string{"file_setattr", "file_getattr"} {
			require.NotContains(t, rule.names, removed)
		}
		if !slices.Contains(rule.names, "ioctl") {
			continue
		}
		ioctlRules++
		require.NotEmpty(t, rule.args, "an entry names ioctl without a condition (gated=%v)", rule.gated)
		require.Equal(t, []string{"ioctl"}, rule.names)
		require.Equal(t, "SCMP_ACT_ALLOW", rule.action)
		require.False(t, rule.gated, "an ioctl rule must not depend on capabilities, kernel or architecture")
		require.Len(t, rule.args, 1)
		mask := jsonUint(t, rule.args[0]["value"])
		datum := jsonUint(t, rule.args[0]["valueTwo"])
		require.NotZero(t, mask)
		require.LessOrEqual(t, mask, uint64(0xFFFFFFFF))
		require.Zero(t, datum&^mask)
	}
	// The kernel caps a filter at BPF_MAXINSNS (4096) instructions; libseccomp
	// emits a few per rule and per architecture. Keep the addition small: the
	// root integration test proves the filter actually loads.
	require.Positive(t, ioctlRules)
	require.LessOrEqual(t, ioctlRules, 40)

	denied := []uint64{fsIocSetflags, fsIoc32Setflags, fsIocFssetxattr}
	for _, size := range []uint64{0, 1, 4, 8, 0x1C, 0x20, 0x3FFF} {
		denied = append(denied, 0x40006602|size<<16, 0x40005820|size<<16)
	}
	for _, command := range slices.Clone(denied) {
		denied = append(denied, signExtendedHigh|command, uint64(0xDEAD0000)<<32|command, uint64(1)<<63|command)
	}
	oracle := compileOracle(t, rules)
	for _, command := range denied {
		require.True(t, deniedByDesign(command))
		require.False(t, oracleAllows(oracle, command), "command %#x must be denied", command)
	}

	allowed := []uint64{
		signExtendedHigh | fsIocFsgetxattr, fsIocFsgetxattr, fsIocGetflags, signExtendedHigh | fsIocGetflags,
		ioctlTCGETS, ioctlFIONREAD, ioctlFICLONE, ioctlTIOCGWINSZ, 0, 0xFFFFFFFF, signExtendedHigh | 0xFFFFFFFF,
	}
	for _, deny := range []uint64{fsIocSetflags, fsIoc32Setflags, fsIocFssetxattr} {
		allowed = append(allowed, deny-1, deny+1)
		for bit := range 32 {
			if 0xC000FFFF&(1<<bit) != 0 {
				allowed = append(allowed, deny^1<<bit)
			}
		}
	}
	for _, command := range allowed {
		require.False(t, deniedByDesign(command), "%#x is not a boundary of the allowed set", command)
		require.True(t, oracleAllows(oracle, command), "command %#x must stay allowed", command)
	}

	random := rand.New(rand.NewPCG(0x5eccc0, 0x1118))
	for range 200_000 {
		command := random.Uint64()
		if oracleAllows(oracle, command) == deniedByDesign(command) {
			t.Fatalf("command %#x: allowed=%v, denied by design=%v", command, oracleAllows(oracle, command), deniedByDesign(command))
		}
	}
}

// The oracle must model the failure modes it guards against, or a passing
// semantics test proves nothing: a zero mask leaves a rule with no comparison,
// which allows every command, and an omitted datum is read as zero.
func TestOracleModelsLibseccompSkips(t *testing.T) {
	compact := buildTestProfile(t)
	_, rules := decodeOracleProfile(t, compact)
	last := rules[len(rules)-1]
	require.Equal(t, []string{"ioctl"}, last.names)

	zeroMask := maps.Clone(last.args[0])
	zeroMask["value"] = float64(0)
	unconditional := append(slices.Clone(rules), oracleRule{names: []string{"ioctl"}, action: "SCMP_ACT_ALLOW", args: []map[string]any{zeroMask}})
	require.True(t, oracleAllows(compileOracle(t, unconditional), fsIocFssetxattr))

	noDatum := map[string]any{"index": float64(1), "value": float64(0x80000000), "op": "SCMP_CMP_MASKED_EQ"}
	withoutDatum := append(slices.Clone(rules), oracleRule{names: []string{"ioctl"}, action: "SCMP_ACT_ALLOW", args: []map[string]any{noDatum}})
	require.True(t, oracleAllows(compileOracle(t, withoutDatum), fsIocSetflags), "an omitted datum compares against zero")
}

// Everything outside the ioctl change is the base profile, byte for byte once
// both sides are put in the same canonical JSON form.
func TestTenantProfileKeepsTheBaseProfileOtherwise(t *testing.T) {
	compact := buildTestProfile(t)
	base, err := json.Marshal(mobyseccomp.DefaultProfile())
	require.NoError(t, err)

	var emitted, original map[string]any
	require.NoError(t, json.Unmarshal(compact, &emitted))
	require.NoError(t, json.Unmarshal(base, &original))
	emittedRules := emitted["syscalls"].([]any)
	originalRules := original["syscalls"].([]any)
	require.Greater(t, len(emittedRules), len(originalRules))
	for _, appended := range emittedRules[len(originalRules):] {
		names := appended.(map[string]any)["names"].([]any)
		require.Equal(t, []any{"ioctl"}, names, "only ioctl rules are appended")
	}
	emitted["syscalls"] = emittedRules[:len(originalRules)]

	restored := false
	for index, raw := range originalRules {
		rule := raw.(map[string]any)
		names := rule["names"].([]any)
		position := slices.Index(names, any("ioctl"))
		if position < 0 {
			continue
		}
		_, conditional := rule["args"]
		require.False(t, conditional)
		require.False(t, restored, "the base profile allows ioctl unconditionally once")
		restored = true
		emittedNames := emittedRules[index].(map[string]any)["names"].([]any)
		require.NotContains(t, emittedNames, any("ioctl"))
		emittedRules[index].(map[string]any)["names"] = slices.Insert(slices.Clone(emittedNames), position, any("ioctl"))
	}
	require.True(t, restored)

	canonicalEmitted, err := json.Marshal(emitted)
	require.NoError(t, err)
	canonicalOriginal, err := json.Marshal(original)
	require.NoError(t, err)
	require.Equal(t, string(canonicalOriginal), string(canonicalEmitted))
}

func TestVerifyEmittedRejectsUnsafeIoctlRules(t *testing.T) {
	deny, err := deniedIoctlCubes()
	require.NoError(t, err)
	compact := buildTestProfile(t)
	require.NoError(t, verifyEmitted(compact, deny))

	mutate := func(t *testing.T, edit func(profile map[string]any, rules []any) []any) []byte {
		t.Helper()
		var profile map[string]any
		require.NoError(t, json.Unmarshal(compact, &profile))
		profile["syscalls"] = edit(profile, profile["syscalls"].([]any))
		encoded, err := json.Marshal(profile)
		require.NoError(t, err)
		return encoded
	}
	lastArg := func(rules []any) map[string]any {
		return rules[len(rules)-1].(map[string]any)["args"].([]any)[0].(map[string]any)
	}
	for name, edit := range map[string]func(map[string]any, []any) []any{
		"argless ioctl": func(_ map[string]any, rules []any) []any {
			return append(rules, map[string]any{"names": []any{"ioctl"}, "action": "SCMP_ACT_ALLOW"})
		},
		"capability-gated argless ioctl": func(_ map[string]any, rules []any) []any {
			return append(rules, map[string]any{"names": []any{"ioctl"}, "action": "SCMP_ACT_ALLOW",
				"includes": map[string]any{"caps": []any{"CAP_SYS_ADMIN"}}})
		},
		"zero mask": func(_ map[string]any, rules []any) []any {
			lastArg(rules)["value"] = 0
			return rules
		},
		"mask above 32 bits": func(_ map[string]any, rules []any) []any {
			lastArg(rules)["value"] = uint64(1) << 32
			return rules
		},
		"datum outside mask": func(_ map[string]any, rules []any) []any {
			arg := lastArg(rules)
			arg["valueTwo"] = uint64(arg["value"].(float64)) + 1
			return rules
		},
		"wrong argument": func(_ map[string]any, rules []any) []any {
			lastArg(rules)["index"] = 0
			return rules
		},
		"wrong operator": func(_ map[string]any, rules []any) []any {
			lastArg(rules)["op"] = "SCMP_CMP_EQ"
			return rules
		},
		"conditional errno": func(_ map[string]any, rules []any) []any {
			rules[len(rules)-1].(map[string]any)["action"] = "SCMP_ACT_ERRNO"
			return rules
		},
		"missing cube": func(_ map[string]any, rules []any) []any {
			return rules[:len(rules)-1]
		},
		"denied command allowed": func(_ map[string]any, rules []any) []any {
			return append(rules, map[string]any{"names": []any{"ioctl"}, "action": "SCMP_ACT_ALLOW",
				"args": []any{map[string]any{"index": 1, "value": 0xC000FFFF, "valueTwo": 0x40006602, "op": "SCMP_CMP_MASKED_EQ"}}})
		},
		"file_setattr": func(_ map[string]any, rules []any) []any {
			return append(rules, map[string]any{"names": []any{"file_setattr"}, "action": "SCMP_ACT_ALLOW"})
		},
		"permissive default": func(profile map[string]any, rules []any) []any {
			profile["defaultAction"] = "SCMP_ACT_ALLOW"
			return rules
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, verifyEmitted(mutate(t, edit), deny))
		})
	}
}

func TestAllowedIoctlCubesAreDisjoint(t *testing.T) {
	deny, err := deniedIoctlCubes()
	require.NoError(t, err)
	allowed := allowedIoctlCubes(deny)
	for i, a := range allowed {
		require.NotZero(t, a.mask)
		require.Zero(t, a.value&^a.mask)
		for _, b := range allowed[i+1:] {
			require.False(t, a.overlaps(b), "%#x/%#x overlaps %#x/%#x", a.mask, a.value, b.mask, b.value)
		}
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "go.mod not found above the test directory")
		dir = parent
	}
}
