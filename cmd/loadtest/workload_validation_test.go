package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
)

func writeWorkloadFixture(t *testing.T, fixture any) string {
	t.Helper()
	data, err := json.Marshal(fixture)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "fixtures.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func TestWorkloadAdmissionRejectsInvalidOperatorInputs(t *testing.T) {
	const leaseID = "6ba7b811-9dad-41d1-80b4-00c04fd430c8"
	valid := workloadConfig{target: "http://localhost:8080", traffic: "authenticated", scenario: "connection",
		keyFile: fixtureKeyFile(t), prefix: "manifest",
		fixtures: writeWorkloadFixture(t, fixtureFile{Leases: []leaseFixture{{LeaseUUID: leaseID}}})}
	for _, tc := range []struct {
		name string
		edit func(*workloadConfig)
	}{
		{"non-HTTP origin", func(c *workloadConfig) { c.target = "file:///etc/passwd" }},
		{"unknown traffic mode", func(c *workloadConfig) { c.traffic = "auto" }},
		{"unknown scenario", func(c *workloadConfig) { c.scenario = "deployment" }},
		{"missing key", func(c *workloadConfig) { c.keyFile = "" }},
		{"unreadable fixture", func(c *workloadConfig) { c.fixtures = filepath.Join(t.TempDir(), "missing") }},
		{"fixture directory", func(c *workloadConfig) { c.fixtures = t.TempDir() }},
		{"unknown fixture field", func(c *workloadConfig) {
			c.fixtures = writeWorkloadFixture(t, map[string]any{"lease_ids": []string{leaseID}})
		}},
		{"too many entries", func(c *workloadConfig) {
			c.fixtures = writeWorkloadFixture(t, fixtureFile{Leases: make([]leaseFixture, 1001)})
		}},
		{"noncanonical lease", func(c *workloadConfig) {
			c.fixtures = writeWorkloadFixture(t, fixtureFile{Leases: []leaseFixture{{LeaseUUID: strings.ToUpper(leaseID)}}})
		}},
		{"duplicate lease", func(c *workloadConfig) {
			c.fixtures = writeWorkloadFixture(t, fixtureFile{Leases: []leaseFixture{{LeaseUUID: leaseID}, {LeaseUUID: leaseID}}})
		}},
		{"invalid manifest", func(c *workloadConfig) {
			c.fixtures = writeWorkloadFixture(t, fixtureFile{Leases: []leaseFixture{{LeaseUUID: leaseID, Payload: []byte(`{"services":{}}`)}}})
		}},
		{"payload scenario without payload", func(c *workloadConfig) { c.scenario = "payload" }},
		{"callback scenario without recording", func(c *workloadConfig) { c.scenario = "callback" }},
		{"mixed scenario without payload", func(c *workloadConfig) { c.scenario = "mixed" }},
		{"callback without secret", func(c *workloadConfig) {
			c.fixtures = writeWorkloadFixture(t, fixtureFile{Callbacks: []callbackFixture{{RequestURI: "/callbacks/provision"}}})
			c.scenario = "callback"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.edit(&cfg)
			work, err := loadWorkload(cfg)
			require.Error(t, err)
			require.Empty(t, work.requests, "invalid inputs must not leave dispatchable partial traffic")
		})
	}
	for _, size := range []int{0, -1, (1 << 20) + 1} {
		_, err := loadWorkload(workloadConfig{target: valid.target, traffic: "rejection", scenario: "payload", payloadSize: size})
		require.ErrorContains(t, err, "payload-size")
	}
}

func TestFixtureAndPrivateKeyFilesAreBoundedAndStrict(t *testing.T) {
	t.Run("oversized fixture", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "large.json")
		file, err := os.Create(path)
		require.NoError(t, err)
		require.NoError(t, file.Truncate(maxFixtureBytes+1))
		require.NoError(t, file.Close())
		_, err = loadWorkload(workloadConfig{target: "http://localhost:8080", traffic: "authenticated", scenario: "connection", fixtures: path})
		require.ErrorContains(t, err, "exceeds")
	})
	for _, tc := range []struct{ name, key, prefix, errorText string }{
		{"invalid hex", strings.Repeat("z", 64), "manifest", "hexadecimal"},
		{"short scalar", "01", "manifest", "64 hexadecimal"},
		{"out of range", strings.Repeat("f", 64), "manifest", "scalar range"},
		{"oversized key file", strings.Repeat("1", 1025), "manifest", "exceeds"},
		{"empty address prefix", strings.Repeat("1", 64), "", "prefix"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tenant.hex")
			require.NoError(t, os.WriteFile(path, []byte(tc.key), 0o600))
			_, err := loadTenantSigner(path, tc.prefix)
			require.ErrorContains(t, err, tc.errorText)
			if len(tc.key) >= 64 {
				require.NotContains(t, err.Error(), tc.key, "invalid key material must not be logged")
			}
		})
	}
	_, err := loadTenantSigner(filepath.Join(t.TempDir(), "missing"), "manifest")
	require.ErrorContains(t, err, "read tenant key")
}

func TestCallbackRecordingsRejectAmbiguousOrAlteredAuthority(t *testing.T) {
	const uri = "/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000"
	const body = `{"lease_uuid":"6ba7b811-9dad-41d1-80b4-00c04fd430c8","backend":"fixture","backend_storage_id":"550e8400-e29b-41d4-a716-446655440000","status":"success"}`
	for _, tc := range []struct{ name, uri, body string }{
		{"network-relative destination", "//attacker.example" + uri, body},
		{"invalid URI encoding", "/callbacks/%zz", body},
		{"fragment changes wire URI", uri + "#fragment", body},
		{"invalid JSON", uri, "{"},
		{"missing storage route", uri, `{"status":"success"}`},
		{"duplicate storage route", uri, strings.Replace(body, `"status"`, `"backend_storage_id":"550e8400-e29b-41d4-a716-446655440000","status"`, 1)},
		{"missing backend", uri, strings.Replace(body, `"backend":"fixture",`, "", 1)},
		{"invented status", uri, strings.Replace(body, "success", "invented", 1)},
		{"case-aliased authority field", uri, strings.Replace(body, `"status"`, `"Status"`, 1)},
		{"unknown callback path", "/elsewhere?operation_id=550e8400-e29b-41d4-a716-446655440000", body},
	} {
		t.Run(tc.name, func(t *testing.T) {
			factory, err := recordedCallbackFactory("http://localhost:8080", strings.Repeat("x", 32), []callbackFixture{{RequestURI: tc.uri, Body: []byte(tc.body)}})
			require.Error(t, err)
			require.Nil(t, factory, "rejected recordings must not become signed traffic")
		})
	}
}

func TestConstructedMixedWorkloadPreservesDeclaredTrafficAndPayloads(t *testing.T) {
	const secret = "loadtest-fixture-secret-at-least-32-bytes"
	leases := []leaseFixture{
		{LeaseUUID: "6ba7b811-9dad-41d1-80b4-00c04fd430c8", Payload: []byte(`{"services":{"first":{"image":"nginx:alpine"}}}`)},
		{LeaseUUID: "550e8400-e29b-41d4-a716-446655440000", Payload: []byte(`{"services":{"second":{"image":"busybox:latest"}}}`)},
	}
	for _, includeCallback := range []bool{false, true} {
		t.Run(map[bool]string{false: "without callback", true: "with callback"}[includeCallback], func(t *testing.T) {
			fixture := fixtureFile{Leases: leases}
			if includeCallback {
				fixture.Callbacks = []callbackFixture{{RequestURI: "/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000",
					Body: []byte(`{"lease_uuid":"6ba7b811-9dad-41d1-80b4-00c04fd430c8","backend":"fixture","backend_storage_id":"550e8400-e29b-41d4-a716-446655440000","status":"success"}`)}}
			}
			work, err := loadWorkload(workloadConfig{target: "http://localhost:8080", traffic: "authenticated", scenario: "mixed",
				fixtures: writeWorkloadFixture(t, fixture), keyFile: fixtureKeyFile(t), prefix: "manifest", callbackSecret: secret})
			require.NoError(t, err)
			synctest.Test(t, func(t *testing.T) {
				counts := make(map[string]int)
				payloads := make(map[string]int)
				for _, factory := range work.requests {
					req, err := factory(t.Context())
					require.NoError(t, err)
					switch {
					case strings.HasSuffix(req.URL.Path, "/data"):
						counts["payload"]++
						body, err := io.ReadAll(req.Body)
						require.NoError(t, err)
						require.NoError(t, req.Body.Close())
						payloads[string(body)]++
					case strings.HasSuffix(req.URL.Path, "/connection"):
						counts["connection"]++
					default:
						counts["callback"]++
						require.NoError(t, hmacauth.VerifyRequest(secret, req, fixture.Callbacks[0].Body, req.Header.Get(hmacauth.SignatureHeader), time.Minute))
					}
				}
				require.Equal(t, 4, counts["payload"])
				require.Equal(t, 2, payloads[string(leases[0].Payload)])
				require.Equal(t, 2, payloads[string(leases[1].Payload)])
				if includeCallback {
					require.Equal(t, 5, counts["connection"])
					require.Equal(t, 1, counts["callback"])
				} else {
					require.Equal(t, 6, counts["connection"])
					require.Zero(t, counts["callback"])
				}
			})
		})
	}
}

func TestRejectionWorkloadCarriesNoValidAuthority(t *testing.T) {
	work, err := loadWorkload(workloadConfig{target: "http://localhost:8080", traffic: "rejection", scenario: "mixed", payloadSize: 64})
	require.NoError(t, err)
	for _, factory := range work.requests {
		req, err := factory(t.Context())
		require.NoError(t, err)
		require.Empty(t, req.Header.Get(hmacauth.SignatureHeader))
		if strings.HasPrefix(req.URL.Path, "/v1/leases/") {
			parts := strings.Split(req.URL.Path, "/")
			require.True(t, backend.IsCanonicalLeaseUUID(parts[3]))
			require.Equal(t, "Bearer intentionally-invalid-token", req.Header.Get("Authorization"))
			if req.Method == http.MethodPost {
				body, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.NoError(t, req.Body.Close())
				require.Len(t, body, 64)
			}
		} else {
			require.Empty(t, req.Header.Get("Authorization"))
		}
	}
}

func TestConnectionFactoryStopsWhileWaitingForUniqueToken(t *testing.T) {
	signer, err := loadTenantSigner(fixtureKeyFile(t), "manifest")
	require.NoError(t, err)
	factory := signer.connectionFactory("http://localhost:8080", []leaseFixture{{LeaseUUID: "6ba7b811-9dad-41d1-80b4-00c04fd430c8"}})
	synctest.Test(t, func(t *testing.T) {
		_, err := factory(t.Context())
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		req, err := factory(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, req)
	})
}
