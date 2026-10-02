//go:build linux

package tenantseccomp

import (
	"bytes"
	"encoding/json"
	"strings"
)

// seccompKey is the security-option key Docker reads as a container's
// seccomp profile. This is the only production file that may spell it; a
// test fails on the literal anywhere else.
const seccompKey = "seccomp"

// optionValue splits one security option exactly as dockerd's
// parseSecurityOpt does: at the first '=' when the option contains one,
// otherwise at the first ':'. ok is false for an option with neither, such as
// "no-new-privileges".
func optionValue(option string) (key, value string, ok bool) {
	separator := ":"
	if strings.Contains(option, "=") {
		separator = "="
	}
	if key, value, ok = strings.Cut(option, separator); !ok {
		return "", "", false
	}
	return key, value, true
}

func isSeccompOption(option string) bool {
	key, _, ok := optionValue(option)
	return ok && key == seccompKey
}

// WithoutSeccomp returns a new slice holding, in order, every option that
// does not name a seccomp profile. Captured container configuration passes
// through it, so a replayed snapshot never carries a profile of its own.
func WithoutSeccomp(options []string) []string {
	kept := make([]string, 0, len(options)+1)
	for _, option := range options {
		if !isSeccompOption(option) {
			kept = append(kept, option)
		}
	}
	return kept
}

// InlineSecurityOpt returns the final security options of a container fred
// creates through the Docker API: options without any seccomp entry, then
// exactly one carrying the profile's compact JSON.
func InlineSecurityOpt(options []string, profile Profile) ([]string, error) {
	if profile.r == nil {
		return nil, refusal("the tenant profile is the zero value")
	}
	return finalSecurityOpt(options, string(profile.r.compact)), nil
}

// FileSecurityOpt returns the final security options of a Compose service:
// options without any seccomp entry, then exactly one naming the profile's
// sealed file. Compose reads that file inside fred's process and sends its
// compact JSON inline, so the daemon receives the same entry either way.
func FileSecurityOpt(options []string, profile Profile) ([]string, error) {
	path, err := profile.MemfdPath()
	if err != nil {
		return nil, err
	}
	return finalSecurityOpt(options, path), nil
}

// finalSecurityOpt is the single producer of the seccomp entry. Whatever the
// caller supplied, the result names exactly one profile, the given one.
func finalSecurityOpt(options []string, value string) []string {
	return append(WithoutSeccomp(options), seccompKey+"="+value)
}

// VerifyCreateRequest judges a container create request body before it is
// sent. The daemon must receive an unprivileged HostConfig whose security
// options name exactly one seccomp profile, and that profile's JSON must
// compact to want. Every refusal wraps ErrRefused.
func VerifyCreateRequest(body []byte, want Digest) error {
	var request struct {
		HostConfig *struct {
			SecurityOpt []string
			Privileged  bool
		}
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return refusal("the create request is not a JSON container configuration")
	}
	if request.HostConfig == nil {
		return refusal("the create request has no host configuration")
	}
	if request.HostConfig.Privileged {
		return refusal("the create request is privileged")
	}
	var profiles []string
	for _, option := range request.HostConfig.SecurityOpt {
		if key, value, ok := optionValue(option); ok && key == seccompKey {
			profiles = append(profiles, value)
		}
	}
	if len(profiles) != 1 {
		return refusal("the create request names %d seccomp profiles, want exactly 1", len(profiles))
	}
	if !profileMatches(profiles[0], want) {
		return refusal("the create request names another seccomp profile")
	}
	return nil
}

// Applied reports whether Docker runs a container with these host settings
// under want: it is not privileged, and its last seccomp option, the one
// dockerd keeps, compacts to want. An absent, unconfined or builtin profile
// is not applied.
func Applied(options []string, privileged bool, want Digest) bool {
	if privileged {
		return false
	}
	effective, found := "", false
	for _, option := range options {
		if key, value, ok := optionValue(option); ok && key == seccompKey {
			effective, found = value, true
		}
	}
	return found && profileMatches(effective, want)
}

func profileMatches(value string, want Digest) bool {
	if !want.valid() {
		return false
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(value)); err != nil {
		return false
	}
	return digestOf(compact.Bytes()) == want
}
