//go:build linux

package tenantseccomp

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func createRequestBody(t *testing.T, securityOpt []string, privileged bool) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"Image":      "sha256:" + strings.Repeat("a", 64),
		"HostConfig": map[string]any{"SecurityOpt": securityOpt, "Privileged": privileged, "CapDrop": []string{"ALL"}},
	})
	require.NoError(t, err)
	return body
}

func TestOptionValueFollowsDockerd(t *testing.T) {
	for option, want := range map[string]struct {
		key, value string
		ok         bool
	}{
		"seccomp=unconfined":        {key: "seccomp", value: "unconfined", ok: true},
		"seccomp:unconfined":        {key: "seccomp", value: "unconfined", ok: true},
		"seccomp=/proc/self/fd/9":   {key: "seccomp", value: "/proc/self/fd/9", ok: true},
		`seccomp:{"a":"b=c"}`:       {key: `seccomp:{"a":"b`, value: `c"}`, ok: true},
		`seccomp={"a":"b:c"}`:       {key: "seccomp", value: `{"a":"b:c"}`, ok: true},
		"no-new-privileges":         {},
		"no-new-privileges:true":    {key: "no-new-privileges", value: "true", ok: true},
		"label=disable":             {key: "label", value: "disable", ok: true},
		"apparmor:docker-default":   {key: "apparmor", value: "docker-default", ok: true},
		"Seccomp=unconfined":        {key: "Seccomp", value: "unconfined", ok: true},
		"seccomp":                   {},
		"systempaths=unconfined":    {key: "systempaths", value: "unconfined", ok: true},
		"seccomp=":                  {key: "seccomp", value: "", ok: true},
		"writable-cgroups:true=yes": {key: "writable-cgroups:true", value: "yes", ok: true},
	} {
		key, value, ok := optionValue(option)
		require.Equal(t, want.ok, ok, option)
		require.Equal(t, want.key, key, option)
		require.Equal(t, want.value, value, option)
	}
}

func TestFinalSecurityOptReplacesEverySeccompEntry(t *testing.T) {
	_, profile := newTestOwner(t)
	callerOptions := []string{"seccomp=unconfined", "no-new-privileges:true", "seccomp:builtin", "label=disable", "seccomp=/elsewhere"}
	original := append([]string(nil), callerOptions...)

	inline, err := InlineSecurityOpt(callerOptions, profile)
	require.NoError(t, err)
	require.Equal(t, []string{"no-new-privileges:true", "label=disable", "seccomp=" + string(profile.CompactJSON())}, inline)
	require.Equal(t, original, callerOptions, "the caller's options must not change")

	path, err := profile.MemfdPath()
	require.NoError(t, err)
	file, err := FileSecurityOpt(callerOptions, profile)
	require.NoError(t, err)
	require.Equal(t, []string{"no-new-privileges:true", "label=disable", "seccomp=" + path}, file)

	// Repeated application never accumulates entries and never aliases.
	again, err := InlineSecurityOpt(inline, profile)
	require.NoError(t, err)
	require.Equal(t, inline, again)
	again[0] = "changed"
	require.Equal(t, "no-new-privileges:true", inline[0])

	require.Equal(t, []string{"no-new-privileges:true", "label=disable"}, WithoutSeccomp(callerOptions))
	require.Empty(t, WithoutSeccomp(nil))
	require.NotNil(t, WithoutSeccomp(nil), "a captured snapshot stores an empty list, never null")
}

func TestVerifyCreateRequest(t *testing.T) {
	_, profile := newTestOwner(t)
	valid := "seccomp=" + string(profile.CompactJSON())
	var pretty bytes.Buffer
	require.NoError(t, json.Indent(&pretty, profile.CompactJSON(), "", "  "))

	require.NoError(t, VerifyCreateRequest(createRequestBody(t, []string{"no-new-privileges:true", valid}, false), profile.Digest()))
	require.NoError(t, VerifyCreateRequest(createRequestBody(t, []string{"seccomp:" + pretty.String()}, false), profile.Digest()),
		"a profile is identified by its compact JSON")

	for name, body := range map[string][]byte{
		"no entry":              createRequestBody(t, []string{"no-new-privileges:true"}, false),
		"no security options":   createRequestBody(t, nil, false),
		"valid then unconfined": createRequestBody(t, []string{valid, "seccomp:unconfined"}, false),
		"unconfined":            createRequestBody(t, []string{"seccomp=unconfined"}, false),
		"builtin":               createRequestBody(t, []string{"seccomp=builtin"}, false),
		"two valid entries":     createRequestBody(t, []string{valid, valid}, false),
		"a file path":           createRequestBody(t, []string{"seccomp=/proc/self/fd/3"}, false),
		"another profile":       createRequestBody(t, []string{`seccomp={"defaultAction":"SCMP_ACT_ALLOW"}`}, false),
		"privileged":            createRequestBody(t, []string{valid}, true),
		"no host configuration": []byte(`{"Image":"busybox"}`),
		"null host":             []byte(`{"HostConfig":null}`),
		"trailing document":     append(createRequestBody(t, []string{valid}, false), []byte(`{"HostConfig":{}}`)...),
		"not JSON":              []byte("not json"),
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, VerifyCreateRequest(body, profile.Digest()), ErrRefused)
		})
	}
}

func TestAppliedJudgesTheEffectiveEntry(t *testing.T) {
	_, profile := newTestOwner(t)
	valid := "seccomp=" + string(profile.CompactJSON())
	require.True(t, Applied([]string{"no-new-privileges:true", valid}, false, profile.Digest()))
	require.True(t, Applied([]string{"seccomp=unconfined", valid}, false, profile.Digest()), "dockerd keeps the last entry")
	for name, options := range map[string][]string{
		"valid then unconfined": {valid, "seccomp:unconfined"},
		"unconfined":            {"seccomp=unconfined"},
		"builtin":               {"seccomp=builtin"},
		"none":                  {"no-new-privileges:true"},
		"empty":                 nil,
	} {
		require.False(t, Applied(options, false, profile.Digest()), name)
	}
	require.False(t, Applied([]string{valid}, true, profile.Digest()), "a privileged container runs without seccomp")
}
