//go:build linux

package tenantseccomp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"slices"

	mobyseccomp "github.com/moby/profiles/seccomp"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
)

// MobyProfilesVersion is the github.com/moby/profiles/seccomp release the
// profile is derived from: the default profile of the supported Docker
// Engine. A test fails when go.mod resolves another version, so a Docker
// upgrade regenerates the profile, and its golden copy, on purpose.
const MobyProfilesVersion = "v0.2.3"

// ErrRefused marks every container creation fred refuses because the tenant
// profile cannot be applied: the profile or its sealed file is unavailable, or
// a create request does not carry exactly this profile.
var ErrRefused = errors.New("tenant seccomp profile cannot be applied")

func refusal(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrRefused, fmt.Sprintf(format, args...))
}

// Digest identifies one profile: the SHA-256 of its compact JSON. Only this
// package computes one, and the zero Digest matches nothing.
type Digest struct{ sum [sha256.Size]byte }

// String returns the digest as lowercase hex.
func (d Digest) String() string { return hex.EncodeToString(d.sum[:]) }

func (d Digest) valid() bool { return d != Digest{} }

func digestOf(data []byte) Digest { return Digest{sum: sha256.Sum256(data)} }

// Profile is the verified tenant profile. Source.TenantSeccompProfile is its
// only constructor: it builds the JSON and checks what it emitted before
// returning it. The zero Profile is unusable: it has no JSON, its Digest
// matches nothing, and MemfdPath refuses.
type Profile struct{ r *profileRecord }

type profileRecord struct {
	compact []byte
	digest  Digest
	owner   *processOwner
}

// CompactJSON returns a copy of the profile's compact JSON, or nil for the
// zero Profile.
func (p Profile) CompactJSON() []byte {
	if p.r == nil {
		return nil
	}
	return slices.Clone(p.r.compact)
}

// Digest returns the SHA-256 of the compact JSON, or the zero Digest for the
// zero Profile.
func (p Profile) Digest() Digest {
	if p.r == nil {
		return Digest{}
	}
	return p.r.digest
}

// MemfdPath returns /proc/self/fd/N for the sealed file that holds the compact
// JSON, after checking that N still names that file. A mismatch re-creates the
// file under the owner's lock; the result is a refusal only when that fails.
func (p Profile) MemfdPath() (string, error) {
	if p.r == nil || p.r.owner == nil {
		return "", refusal("the tenant profile is the zero value")
	}
	return p.r.owner.path(p.r)
}

// ioctlDecisionMask selects the bits of an asm-generic ioctl command that the
// profile keys on: direction (bits 30-31), type (8-15) and number (0-7). The
// size field (16-29) is left free, so a denied command stays denied whatever
// argument size it encodes.
const ioctlDecisionMask uint32 = 0xC000FFFF

// ioctlCube is the set of 32-bit commands c with c&mask == value.
type ioctlCube struct{ mask, value uint32 }

func (c ioctlCube) contains(command uint32) bool { return command&c.mask == c.value }

func (c ioctlCube) overlaps(other ioctlCube) bool {
	return (c.value^other.value)&(c.mask&other.mask) == 0
}

// deniedIoctlCubes returns the two commands that change inode attributes,
// keyed on direction, type and number: _IOW('f', 2, *) (FS_IOC_SETFLAGS and
// its 32-bit form FS_IOC32_SETFLAGS) and _IOW('X', 32, *) (FS_IOC_FSSETXATTR).
// Architecture families encode the direction differently, so the write bits
// come from x/sys's generated FS_IOC_SETFLAGS, as in volume_xfs.go, and any
// GOARCH without the asm-generic layout is refused.
func deniedIoctlCubes() ([]ioctlCube, error) {
	switch runtime.GOARCH {
	case "amd64", "arm64":
	default:
		return nil, fmt.Errorf("GOARCH %s is not known to use the asm-generic ioctl encoding", runtime.GOARCH)
	}
	setflags := uint32(unix.FS_IOC_SETFLAGS)
	write := setflags & 0xC0000000
	if write != 1<<30 || (setflags>>8)&0xFF != 'f' || setflags&0xFF != 2 {
		return nil, fmt.Errorf("FS_IOC_SETFLAGS %#x does not have the asm-generic encoding", setflags)
	}
	return []ioctlCube{
		{mask: ioctlDecisionMask, value: write | 'f'<<8 | 2},
		{mask: ioctlDecisionMask, value: write | 'X'<<8 | 32},
	}, nil
}

// allowedIoctlCubes covers exactly the complement of deny over the low 32
// bits with pairwise disjoint cubes, by splitting on the decision bits from
// the most significant down and keeping every branch that no denied cube
// reaches. deny must be keyed on the decision bits alone.
func allowedIoctlCubes(deny []ioctlCube) []ioctlCube {
	var bits []uint32
	for bit := 31; bit >= 0; bit-- {
		if ioctlDecisionMask&(1<<bit) != 0 {
			bits = append(bits, 1<<bit)
		}
	}
	var allowed []ioctlCube
	var split func(region ioctlCube, rest []uint32, reachable []ioctlCube)
	split = func(region ioctlCube, rest []uint32, reachable []ioctlCube) {
		if len(reachable) == 0 {
			allowed = append(allowed, region)
			return
		}
		if len(rest) == 0 {
			return // every decision bit is fixed: the region is a denied cube
		}
		for _, side := range [2]uint32{0, rest[0]} {
			child := ioctlCube{mask: region.mask | rest[0], value: region.value | side}
			var overlapping []ioctlCube
			for _, cube := range reachable {
				if cube.overlaps(child) {
					overlapping = append(overlapping, cube)
				}
			}
			split(child, rest[1:], overlapping)
		}
	}
	split(ioctlCube{}, bits, deny)
	return allowed
}

// The profile cannot deny an ioctl command with a conditional ERRNO rule.
// runc drops a rule whose action equals the default action, and libseccomp
// discards a conditional rule for a syscall that already has an unconditional
// one. buildProfile therefore removes ioctl from the default profile's
// unconditional allow and adds conditional ALLOW rules whose union is exactly
// the complement of the denied commands. A denied command matches none of
// them and falls through to the default action, EPERM.
//
// Every rule is a single SCMP_CMP_MASKED_EQ on argument 1, the command. The
// kernel reads the command as a 32-bit value but seccomp sees the whole
// register, which libc may sign-extend and a raw caller can set freely, so
// every mask lies within the low 32 bits. libseccomp reads "value" as the mask
// and "valueTwo" as the datum, masks the datum, and skips a comparison half
// whose mask is zero; a rule with no remaining comparison would allow every
// command. verifyEmitted rejects any rule that could take one of those paths.
// Keeping one condition per rule also keeps runc from splitting a rule into
// alternatives.
func ioctlAllowRule(cube ioctlCube) *mobyseccomp.Syscall {
	return &mobyseccomp.Syscall{LinuxSyscall: specs.LinuxSyscall{
		Names:  []string{"ioctl"},
		Action: specs.ActAllow,
		Args: []specs.LinuxSeccompArg{{
			Index: 1, Value: uint64(cube.mask), ValueTwo: uint64(cube.value), Op: specs.OpMaskedEqual,
		}},
	}}
}

// removedFromEveryRule are syscalls the tenant profile never allows, whatever
// a future base profile adds. file_setattr changes the same inode attributes
// as the denied commands; file_getattr is its read-only counterpart.
var removedFromEveryRule = []string{"file_setattr", "file_getattr"}

// buildProfile derives the tenant profile from the base profile
// deterministically and returns its compact JSON once verifyEmitted accepts
// it.
func buildProfile() ([]byte, error) {
	deny, err := deniedIoctlCubes()
	if err != nil {
		return nil, err
	}
	base := mobyseccomp.DefaultProfile()
	if base == nil {
		return nil, errors.New("the base profile is empty")
	}
	removedIoctl := 0
	rules := make([]*mobyseccomp.Syscall, 0, len(base.Syscalls)+32)
	for _, rule := range base.Syscalls {
		if rule == nil || rule.Name != "" {
			return nil, errors.New("the base profile has a nil or single-name rule")
		}
		names := make([]string, 0, len(rule.Names))
		for _, name := range rule.Names {
			switch {
			case name == "ioctl" && len(rule.Args) == 0 && rule.Action == specs.ActAllow:
				removedIoctl++
			case slices.Contains(removedFromEveryRule, name):
			default:
				names = append(names, name)
			}
		}
		if len(names) == 0 {
			continue
		}
		copied := *rule
		copied.Names = names
		rules = append(rules, &copied)
	}
	if removedIoctl != 1 {
		return nil, fmt.Errorf("the base profile allows ioctl unconditionally in %d rules, want exactly 1", removedIoctl)
	}
	for _, cube := range allowedIoctlCubes(deny) {
		rules = append(rules, ioctlAllowRule(cube))
	}
	base.Syscalls = rules
	compact, err := json.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("encode the tenant profile: %w", err)
	}
	if err := verifyEmitted(compact, deny); err != nil {
		return nil, fmt.Errorf("verify the tenant profile: %w", err)
	}
	return compact, nil
}

type emittedProfile struct {
	DefaultAction   string        `json:"defaultAction"`
	DefaultErrnoRet *uint         `json:"defaultErrnoRet"`
	Syscalls        []emittedRule `json:"syscalls"`
}

type emittedRule struct {
	Names    []string        `json:"names"`
	Name     string          `json:"name"`
	Action   string          `json:"action"`
	Args     []emittedArg    `json:"args"`
	Includes json.RawMessage `json:"includes"`
	Excludes json.RawMessage `json:"excludes"`
}

type emittedArg struct {
	Index    uint   `json:"index"`
	Value    uint64 `json:"value"`
	ValueTwo uint64 `json:"valueTwo"`
	Op       string `json:"op"`
}

// verifyEmitted checks the JSON the daemon will load, not the generator's
// intermediate values. The default action is EPERM, no rule names the
// removed syscalls, and every rule naming ioctl is one ALLOW with a single
// in-range MASKED_EQ on the command. Every mask lies within the decision bits,
// so walking all 2^18 decision-bit patterns decides the whole 32-bit command
// space: a command is allowed exactly when it is not denied.
func verifyEmitted(compact []byte, deny []ioctlCube) error {
	var profile emittedProfile
	if err := json.Unmarshal(compact, &profile); err != nil {
		return err
	}
	if profile.DefaultAction != string(specs.ActErrno) || profile.DefaultErrnoRet == nil || *profile.DefaultErrnoRet != uint(unix.EPERM) {
		return errors.New("the default action is not ERRNO(EPERM)")
	}
	var allow []ioctlCube
	for _, rule := range profile.Syscalls {
		names := rule.Names
		if rule.Name != "" {
			names = append(slices.Clone(names), rule.Name)
		}
		for _, removed := range removedFromEveryRule {
			if slices.Contains(names, removed) {
				return fmt.Errorf("a rule names %s", removed)
			}
		}
		if !slices.Contains(names, "ioctl") {
			continue
		}
		cube, err := verifyIoctlRule(rule, names)
		if err != nil {
			return err
		}
		allow = append(allow, cube)
	}
	if len(allow) == 0 {
		return errors.New("no rule allows any ioctl command")
	}
	for _, cube := range append(slices.Clone(allow), deny...) {
		if cube.mask&^ioctlDecisionMask != 0 {
			return fmt.Errorf("an ioctl cube keys on bits %#x outside the decision mask", cube.mask&^ioctlDecisionMask)
		}
	}
	decisionBits := make([]uint32, 0, 18)
	for bit := range 32 {
		if ioctlDecisionMask&(1<<bit) != 0 {
			decisionBits = append(decisionBits, 1<<bit)
		}
	}
	for pattern := range uint32(1) << len(decisionBits) {
		var command uint32
		for index, bit := range decisionBits {
			if pattern&(1<<index) != 0 {
				command |= bit
			}
		}
		if anyCubeContains(allow, command) == anyCubeContains(deny, command) {
			return fmt.Errorf("ioctl command %#x is both allowed and denied, or neither", command)
		}
	}
	return nil
}

func verifyIoctlRule(rule emittedRule, names []string) (ioctlCube, error) {
	if len(rule.Args) == 0 {
		return ioctlCube{}, errors.New("a rule allows ioctl without a condition")
	}
	if len(names) != 1 || rule.Action != string(specs.ActAllow) || len(rule.Args) != 1 ||
		len(rule.Includes) != 0 || len(rule.Excludes) != 0 {
		return ioctlCube{}, errors.New("an ioctl rule is not a lone unconditional-scope ALLOW with one condition")
	}
	arg := rule.Args[0]
	if arg.Index != 1 || arg.Op != string(specs.OpMaskedEqual) {
		return ioctlCube{}, errors.New("an ioctl rule does not compare the command with MASKED_EQ")
	}
	if arg.Value == 0 || arg.Value > 0xFFFFFFFF || arg.ValueTwo&^arg.Value != 0 {
		return ioctlCube{}, fmt.Errorf("ioctl mask %#x / datum %#x is out of range", arg.Value, arg.ValueTwo)
	}
	return ioctlCube{mask: uint32(arg.Value), value: uint32(arg.ValueTwo)}, nil
}

func anyCubeContains(cubes []ioctlCube, command uint32) bool {
	for _, cube := range cubes {
		if cube.contains(command) {
			return true
		}
	}
	return false
}
