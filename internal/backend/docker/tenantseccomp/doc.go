// Package tenantseccomp owns the seccomp profile fred applies to every
// container it creates for a tenant, and every Docker security option that
// names a seccomp profile.
//
// The profile is derived from the default profile of the supported Docker
// Engine, as published by github.com/moby/profiles/seccomp at
// MobyProfilesVersion. Profile is the only form it takes. Process is the only
// usable Source: it builds the profile once, checks the JSON it emits before
// any caller sees it, and keeps that JSON in one sealed memfd per process, so
// Compose, which reads a profile file inside fred's own process, can reference
// it by an absolute path. A failed build or file is retried on every request,
// and every refusal wraps ErrRefused. The zero Profile and the zero Source are
// unusable.
//
// securityopt.go is the only production file that reads or writes a seccomp
// security option. It parses options exactly as dockerd does, produces the
// one final entry every creation carries, removes such entries from captured
// configuration, and judges create requests and inspected containers. A test
// fails on the option's key anywhere else in fred's production code.
//
// The package is Linux-only.
package tenantseccomp
