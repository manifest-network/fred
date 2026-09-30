package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/metrics"
	maintenanceapp "github.com/manifest-network/fred/internal/provisioner/maintenance"
	"github.com/manifest-network/fred/internal/testutil"
)

// legacyKeyFixture serves restart/update with a replay tracker that remembers
// every consumed token, as the production tracker does.
type legacyKeyFixture struct {
	handlers *Handlers
	keyPair  *testutil.TestKeyPair
	commands []maintenanceapp.Command
	uses     int
}

func newLegacyKeyFixture(t *testing.T, legacyTenants ...string) *legacyKeyFixture {
	t.Helper()
	fixture := &legacyKeyFixture{keyPair: testutil.NewTestKeyPair("maintenance-legacy-key")}
	used := map[TokenReplayClaim]struct{}{}
	fixture.handlers = NewHandlers(HandlersConfig{
		TokenTracker: &mockTokenTracker{tryUseFunc: func(claim TokenReplayClaim) error {
			fixture.uses++
			if _, replay := used[claim]; replay {
				return ErrTokenAlreadyUsed
			}
			used[claim] = struct{}{}
			return nil
		}},
		MaintenanceService: maintenanceServiceFunc(func(
			_ context.Context, command maintenanceapp.Command,
		) maintenanceapp.Result {
			fixture.commands = append(fixture.commands, command)
			return maintenanceapp.NewResult(maintenanceapp.OutcomeAccepted, nil)
		}),
		ProviderUUID:                        testutil.ValidUUID2,
		Bech32Prefix:                        "manifest",
		MaintenanceLegacyIdempotencyTenants: legacyTenants,
	})
	return fixture
}

func (fixture *legacyKeyFixture) token(at time.Time) string {
	return testutil.CreateTestToken(fixture.keyPair, testutil.ValidUUID1, at)
}

func (fixture *legacyKeyFixture) send(endpoint, token string, keys ...string) int {
	body := ""
	if endpoint == "update" {
		body = `{"payload":"dGVzdA=="}`
	}
	request := httptest.NewRequest(http.MethodPost,
		"/v1/leases/"+testutil.ValidUUID1+"/"+endpoint, strings.NewReader(body))
	request.SetPathValue("lease_uuid", testutil.ValidUUID1)
	request.Header.Set("Authorization", "Bearer "+token)
	for _, key := range keys {
		request.Header.Add(idempotencyKeyHeader, key)
	}
	response := httptest.NewRecorder()
	if endpoint == "restart" {
		fixture.handlers.RestartLease(response, request)
	} else {
		fixture.handlers.UpdateLease(response, request)
	}
	return response.Code
}

func TestKeylessMaintenanceIsRefusedBeforeAuthenticationWhenNoTenantIsListed(t *testing.T) {
	fixture := newLegacyKeyFixture(t)
	assert.Equal(t, http.StatusBadRequest, fixture.send("restart", fixture.token(time.Now())))
	assert.Zero(t, fixture.uses, "the flag off must not consume the token")
	assert.Empty(t, fixture.commands)
}

func TestListedTenantKeylessMaintenanceIsKeyedByItsSingleUseToken(t *testing.T) {
	for _, endpoint := range []string{"restart", "update"} {
		t.Run(endpoint, func(t *testing.T) {
			fixture := newLegacyKeyFixture(t, testutil.NewTestKeyPair("maintenance-legacy-key").Address)
			before := promtestutil.ToFloat64(metrics.APIMaintenanceLegacyKeyTotal)
			now := time.Now()
			first := fixture.token(now)

			require.Equal(t, http.StatusAccepted, fixture.send(endpoint, first))
			require.Len(t, fixture.commands, 1)
			id := fixture.commands[0].ID
			parsed, err := maintenanceid.Parse(id.String())
			require.NoError(t, err, "the derived key must be a canonical UUIDv4")
			assert.Equal(t, id, parsed)
			assert.InDelta(t, before+1, promtestutil.ToFloat64(metrics.APIMaintenanceLegacyKeyTotal), 0)

			assert.Equal(t, http.StatusUnauthorized, fixture.send(endpoint, first),
				"a replayed token is refused before it can name a command")
			require.Len(t, fixture.commands, 1)

			require.Equal(t, http.StatusAccepted, fixture.send(endpoint, fixture.token(now.Add(time.Second))))
			require.Len(t, fixture.commands, 2)
			assert.NotEqual(t, id, fixture.commands[1].ID, "each token names its own command")
		})
	}
}

func TestUnlistedTenantKeylessMaintenanceIsRefused(t *testing.T) {
	fixture := newLegacyKeyFixture(t, testutil.NewTestKeyPair("some-other-tenant").Address)
	assert.Equal(t, http.StatusBadRequest, fixture.send("restart", fixture.token(time.Now())))
	assert.Empty(t, fixture.commands)
}

func TestMalformedKeysStayRefusedBeforeAuthenticationWithLegacyTenants(t *testing.T) {
	fixture := newLegacyKeyFixture(t, testutil.NewTestKeyPair("maintenance-legacy-key").Address)
	for name, keys := range map[string][]string{
		"empty":     {""},
		"malformed": {"not-a-canonical-uuidv4"},
		"repeated":  {"550e8400-e29b-41d4-a716-446655440000", "6ba7b811-9dad-41d1-80b4-00c04fd430c8"},
	} {
		assert.Equal(t, http.StatusBadRequest, fixture.send("restart", fixture.token(time.Now()), keys...), name)
	}
	assert.Zero(t, fixture.uses, "a malformed key must not consume the token")
	assert.Empty(t, fixture.commands)
}

func TestLegacyMaintenanceIDBindsEveryInput(t *testing.T) {
	base := legacyMaintenanceID(testutil.ValidUUID2, testutil.ValidUUID1, maintenanceapp.KindRestart, "sig")
	assert.Equal(t, base, legacyMaintenanceID(testutil.ValidUUID2, testutil.ValidUUID1, maintenanceapp.KindRestart, "sig"))
	for name, other := range map[string]maintenanceid.ID{
		"provider":  legacyMaintenanceID(testutil.ValidUUID3, testutil.ValidUUID1, maintenanceapp.KindRestart, "sig"),
		"lease":     legacyMaintenanceID(testutil.ValidUUID2, testutil.ValidUUID3, maintenanceapp.KindRestart, "sig"),
		"kind":      legacyMaintenanceID(testutil.ValidUUID2, testutil.ValidUUID1, maintenanceapp.KindUpdate, "sig"),
		"signature": legacyMaintenanceID(testutil.ValidUUID2, testutil.ValidUUID1, maintenanceapp.KindRestart, "sig2"),
	} {
		assert.NotEqual(t, base, other, name)
		assert.True(t, other.Valid(), name)
	}
}
