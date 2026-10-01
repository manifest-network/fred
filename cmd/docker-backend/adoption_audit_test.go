package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker"
)

func TestParseStartupFlagsAuditStorageIdentityAdoption(t *testing.T) {
	startup, err := parseStartupFlags([]string{"-audit-storage-identity-adoption"}, io.Discard)
	require.NoError(t, err)
	assert.True(t, startup.auditStorageIdentityAdoption)
}

func TestStorageIdentityAdoptionAuditExitCodes(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	finding := docker.AdoptionAuditFinding{Class: "unresolved_close", Remedy: "investigate", Message: "m"}
	for name, test := range map[string]struct {
		audit   docker.StorageIdentityAdoptionAudit
		err     error
		want    int
		written bool
	}{
		"clean": {
			audit: docker.StorageIdentityAdoptionAudit{Verdict: docker.StorageIdentityAdoptionReady},
			want:  0, written: true,
		},
		"findings": {
			audit: docker.StorageIdentityAdoptionAudit{
				Verdict:  docker.StorageIdentityAdoptionBlocked,
				Findings: []docker.AdoptionAuditFinding{finding},
			},
			want: 3, written: true,
		},
		"audit failed": {err: errors.New("unreadable journal"), want: 1},
		"blocked without findings": {
			audit: docker.StorageIdentityAdoptionAudit{Verdict: docker.StorageIdentityAdoptionBlocked},
			want:  1, written: true,
		},
		"ready with findings": {
			audit: docker.StorageIdentityAdoptionAudit{
				Verdict:  docker.StorageIdentityAdoptionReady,
				Findings: []docker.AdoptionAuditFinding{finding},
			},
			want: 1, written: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			assert.Equal(t, test.want, storageIdentityAdoptionAuditExit(&out, logger, test.audit, test.err))
			if !test.written {
				assert.Empty(t, out.String(), "a failed audit prints nothing a caller could mistake for a result")
				return
			}
			var decoded docker.StorageIdentityAdoptionAudit
			decoder := json.NewDecoder(&out)
			decoder.DisallowUnknownFields()
			require.NoError(t, decoder.Decode(&decoded))
			assert.Equal(t, test.audit.Verdict, decoded.Verdict)
		})
	}
}
