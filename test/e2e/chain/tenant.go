//go:build integration

package chain

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/testutil"
	"github.com/manifest-network/fred/test/e2e"
)

// Lease states as the chain's JSON output spells them.
const (
	LeasePending  = "LEASE_STATE_PENDING"
	LeaseActive   = "LEASE_STATE_ACTIVE"
	LeaseClosed   = "LEASE_STATE_CLOSED"
	LeaseRejected = "LEASE_STATE_REJECTED"
	LeaseExpired  = "LEASE_STATE_EXPIRED"
)

// Tenant is a funded account that leases from a Provider, the way a tenant
// does: transactions on the chain, and the manifest to the provider's API.
type Tenant struct {
	provider *Provider
	key      Key
	signer   *testutil.TestKeyPair

	// minted is the second in which the last token for each lease was
	// minted. Two tokens for one lease minted in the same second are the
	// same token, and the provider accepts a token once.
	mu     sync.Mutex
	minted map[string]int64
}

// Answer is what the provider's API answered one request with.
type Answer struct {
	Status int
	// Error and Reason are the text and the machine-readable reason of a
	// refusal, and RetryAfter its Retry-After header.
	Error      string
	Reason     string
	RetryAfter string
	// Body is the answer as it was sent.
	Body string
}

// NewTenant creates an account, funds it from the validator's genesis
// balance, and funds its credit account, which leases are billed against.
func (p *Provider) NewTenant(t *testing.T) *Tenant {
	t.Helper()
	p.tenants++
	key := p.Chain.AddKey(t, fmt.Sprintf("tenant-%d", p.tenants))
	p.Chain.Tx(t, validatorKey, "bank", "send", validatorKey, key.Address, "100000000"+billingDenom)
	p.Chain.Tx(t, key.Name, "billing", "fund-credit", key.Address, "50000000"+billingDenom)

	// The same key as the keyring's, derived the way manifestd derives it, to
	// sign what a tenant sends to the provider's API.
	derived, err := hd.Secp256k1.Derive()(key.Mnemonic, "", hd.CreateHDPath(118, 0, 0).String())
	require.NoError(t, err)
	signer := &testutil.TestKeyPair{PrivKey: &secp256k1.PrivKey{Key: derived}, Address: key.Address}
	signer.PubKey = signer.PrivKey.PubKey().(*secp256k1.PubKey)
	signer.PubKeyB64 = base64.StdEncoding.EncodeToString(signer.PubKey.Bytes())
	derivedAddress, err := sdktypes.Bech32ifyAddressBytes("manifest", signer.PubKey.Address().Bytes())
	require.NoError(t, err)
	require.Equal(t, key.Address, derivedAddress, "the derived key is not the keyring's key")
	return &Tenant{provider: p, key: key, signer: signer, minted: map[string]int64{}}
}

// Address is the tenant's account on the chain.
func (tn *Tenant) Address() string { return tn.key.Address }

// Deploy creates a lease for one unit of a SKU and uploads its manifest. The
// chain records the manifest's hash with the lease, and the provider deploys
// only a manifest with that hash. It returns the lease's UUID.
func (tn *Tenant) Deploy(t *testing.T, sku string, manifest map[string]any) string {
	t.Helper()
	return tn.DeployItems(t, manifest, sku+":1")
}

// CreateLease creates a lease that records a manifest's hash, and uploads
// nothing: the lease stays pending until something is done about it. Items
// are as for DeployItems.
func (tn *Tenant) CreateLease(t *testing.T, manifest map[string]any, items ...string) string {
	t.Helper()
	_, hashHex := encodeManifest(t, manifest)
	return tn.createLease(t, hashHex, items...)
}

// Upload sends a lease's manifest to the provider once and returns the
// answer, for a test about an upload the provider refuses.
func (tn *Tenant) Upload(t *testing.T, leaseUUID string, manifest map[string]any) Answer {
	t.Helper()
	payload, hashHex := encodeManifest(t, manifest)
	token := testutil.CreateTestPayloadToken(tn.signer, leaseUUID, hashHex, time.Now())
	answer, err := tn.exchange(t.Context(), http.MethodPost, "/v1/leases/"+leaseUUID+"/data", token,
		map[string]string{"Content-Type": "application/octet-stream"}, payload)
	require.NoError(t, err, "upload the manifest of lease %s", leaseUUID)
	return answer
}

// Ask sends one request about a lease to the provider as its tenant, with a
// fresh token, and returns the answer whatever it is. action is the last
// element of the path: "restart", "update", "restore", or a resource to read.
func (tn *Tenant) Ask(t *testing.T, method, leaseUUID, action string, headers map[string]string, body []byte) Answer {
	t.Helper()
	token, err := tn.accessToken(t.Context(), leaseUUID)
	require.NoError(t, err)
	return tn.AskWith(t, token, method, leaseUUID, action, headers, body)
}

// AskWith is Ask with a token the test already holds.
func (tn *Tenant) AskWith(t *testing.T, token, method, leaseUUID, action string, headers map[string]string, body []byte) Answer {
	t.Helper()
	answer, err := tn.exchange(t.Context(), method, "/v1/leases/"+leaseUUID+"/"+action, token, headers, body)
	require.NoError(t, err, "%s %s of lease %s", method, action, leaseUUID)
	return answer
}

// Token mints a token for a lease, for a test that sends it itself.
func (tn *Tenant) Token(t *testing.T, leaseUUID string) string {
	t.Helper()
	token, err := tn.accessToken(t.Context(), leaseUUID)
	require.NoError(t, err)
	return token
}

// TokenAt is Token for a token that says it was minted at the given moment.
// The provider takes a token dated up to ten seconds ahead, and such a token
// lasts that much longer.
func (tn *Tenant) TokenAt(t *testing.T, leaseUUID string, at time.Time) string {
	t.Helper()
	token, err := tn.mint(t.Context(), leaseUUID, "-timestamp", strconv.FormatInt(at.Unix(), 10))
	require.NoError(t, err)
	return token
}

// RestoreFrom is the body of a request to restore a closed lease's data.
func RestoreFrom(t *testing.T, fromLeaseUUID string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]string{"from_lease_uuid": fromLeaseUUID})
	require.NoError(t, err)
	return body
}

// UpdateTo is the body of a request to run a lease on a new manifest.
func UpdateTo(t *testing.T, manifest map[string]any) []byte {
	t.Helper()
	payload, _ := encodeManifest(t, manifest)
	body, err := json.Marshal(map[string][]byte{"payload": payload})
	require.NoError(t, err)
	return body
}

// DeployItems is Deploy for a lease of anything but one unit of one SKU. An
// item is "sku:quantity", or "sku:quantity:service" for a service of a stack.
func (tn *Tenant) DeployItems(t *testing.T, manifest map[string]any, items ...string) string {
	t.Helper()
	payload, hashHex := encodeManifest(t, manifest)
	leaseUUID := tn.createLease(t, hashHex, items...)

	// providerd serves its API before it is ready to take payloads, so the
	// upload is repeated until it is accepted. A token is good for one
	// request.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		token := testutil.CreateTestPayloadToken(tn.signer, leaseUUID, hashHex, time.Now())
		status, body, err := tn.request(t.Context(), http.MethodPost, "/v1/leases/"+leaseUUID+"/data", token,
			map[string]string{"Content-Type": "application/octet-stream"}, payload)
		if assert.NoError(collect, err) {
			assert.Equal(collect, http.StatusAccepted, status, "upload answered: %s", body)
		}
	}, time.Minute, time.Second)
	return leaseUUID
}

// Close closes a lease on the chain. The provider learns of it from the chain.
func (tn *Tenant) Close(t *testing.T, leaseUUID string) {
	t.Helper()
	tn.provider.Chain.Tx(t, tn.key.Name, "billing", "close-lease", leaseUUID)
}

// Cancel withdraws a lease the provider has not acknowledged yet. The chain
// records it as rejected.
func (tn *Tenant) Cancel(t *testing.T, leaseUUID string) {
	t.Helper()
	tn.provider.Chain.Tx(t, tn.key.Name, "billing", "cancel-lease", leaseUUID)
}

// Restart asks the provider to replace an active lease's containers, keeping
// its manifest.
func (tn *Tenant) Restart(t *testing.T, leaseUUID string) {
	t.Helper()
	tn.maintain(t, leaseUUID, "restart", nil)
}

// Update asks the provider to run an active lease on a new manifest.
func (tn *Tenant) Update(t *testing.T, leaseUUID string, manifest map[string]any) {
	t.Helper()
	tn.maintain(t, leaseUUID, "update", UpdateTo(t, manifest))
}

// Restore creates a lease for one unit of a SKU and asks the provider to give
// it the data a closed lease left behind. manifest is the source's manifest:
// the new lease records its hash, which keeps the provider from deploying it
// empty before the restore arrives. It returns the new lease's UUID.
func (tn *Tenant) Restore(t *testing.T, sku, fromLeaseUUID string, manifest map[string]any) string {
	t.Helper()
	return tn.restore(t, sku, fromLeaseUUID, manifest, func(collect *assert.CollectT, status int, answer string) {
		assert.Equal(collect, http.StatusAccepted, status, "restore answered: %s", answer)
	})
}

// RestoreUnanswered is Restore for a restore the test keeps from the backend:
// lost is the link losing the provider's restore requests. The provider then
// cannot tell the tenant that the restore is under way, and answers with an
// error. The request is repeated until the link has lost one, because the
// provider can also turn a restore away for a moment before it sends anything,
// as while it takes stock of its backends.
func (tn *Tenant) RestoreUnanswered(t *testing.T, sku, fromLeaseUUID string, manifest map[string]any, lost *e2e.LostRequests) string {
	t.Helper()
	return tn.restore(t, sku, fromLeaseUUID, manifest, func(collect *assert.CollectT, status int, answer string) {
		assert.True(collect, lost.Lost(), "the provider sent no restore; it answered %d: %s", status, answer)
	})
}

// restore creates the lease to restore into and sends the restore until
// done accepts the provider's answer. The source can still be owned by its
// close, which the provider answers with a conflict, before it sends anything
// to a backend, and expects to be retried.
func (tn *Tenant) restore(t *testing.T, sku, fromLeaseUUID string, manifest map[string]any, done func(collect *assert.CollectT, status int, answer string)) string {
	t.Helper()
	leaseUUID := tn.CreateLease(t, manifest, sku+":1")
	body := RestoreFrom(t, fromLeaseUUID)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		token, err := tn.accessToken(t.Context(), leaseUUID)
		if !assert.NoError(collect, err) {
			return
		}
		status, answer, err := tn.request(t.Context(), http.MethodPost, "/v1/leases/"+leaseUUID+"/restore", token, nil, body)
		if assert.NoError(collect, err) {
			done(collect, status, answer)
		}
	}, time.Minute, 2*time.Second)
	return leaseUUID
}

// maintain sends one restart or update. The idempotency key names the
// command, so repeating the request while the lease is busy cannot run it
// twice.
func (tn *Tenant) maintain(t *testing.T, leaseUUID, kind string, body []byte) {
	t.Helper()
	headers := map[string]string{"Idempotency-Key": uuid.NewString()}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		token, err := tn.accessToken(t.Context(), leaseUUID)
		if !assert.NoError(collect, err) {
			return
		}
		status, answer, err := tn.request(t.Context(), http.MethodPost, "/v1/leases/"+leaseUUID+"/"+kind, token, headers, body)
		if assert.NoError(collect, err) {
			assert.Equal(collect, http.StatusAccepted, status, "%s answered: %s", kind, answer)
		}
	}, time.Minute, 2*time.Second)
}

// Read reads one of a lease's resources from the provider's API as its
// tenant: "status", "provision", "connection", "logs" or "releases". It
// returns the status the provider answered with, and decodes the body of a
// 200 into out.
func (tn *Tenant) Read(t *testing.T, leaseUUID, resource string, out any) int {
	t.Helper()
	token, err := tn.accessToken(t.Context(), leaseUUID)
	require.NoError(t, err)
	status, answer, err := tn.request(t.Context(), http.MethodGet, "/v1/leases/"+leaseUUID+"/"+resource, token, nil, nil)
	require.NoError(t, err, "read %s of lease %s", resource, leaseUUID)
	if status == http.StatusOK && out != nil {
		require.NoError(t, json.Unmarshal([]byte(answer), out), "%s of lease %s: %s", resource, leaseUUID, answer)
	}
	return status
}

// createLease creates a lease for the given items, recording a manifest
// hash, and returns its UUID.
func (tn *Tenant) createLease(t *testing.T, hashHex string, items ...string) string {
	t.Helper()
	// The transaction's own event names the lease it created, so no search
	// of the tenant's leases can pick another one.
	args := append([]string{"billing", "create-lease"}, items...)
	created := tn.provider.Chain.Tx(t, tn.key.Name, append(args, "--meta-hash", hashHex)...).Event(t, "lease_created")
	require.Equal(t, tn.key.Address, created["tenant"])
	require.Equal(t, tn.provider.UUID, created["provider_uuid"])
	require.Equal(t, hashHex, created["meta_hash"])
	require.NotEmpty(t, created["lease_uuid"])
	return created["lease_uuid"]
}

// accessToken mints the bearer token for one request on a lease, with the
// lease-token binary a tenant would use. The provider accepts a token for a
// few seconds only, and for a request that changes something only once.
func (tn *Tenant) accessToken(ctx context.Context, leaseUUID string) (string, error) {
	// A token carries the second it was minted in and nothing finer, so the
	// next one for the same lease waits for the next second.
	tn.mu.Lock()
	defer tn.mu.Unlock()
	for time.Now().Unix() <= tn.minted[leaseUUID] {
		time.Sleep(50 * time.Millisecond)
	}
	tn.minted[leaseUUID] = time.Now().Unix()
	return tn.mint(ctx, leaseUUID)
}

// mint runs the lease-token binary for a lease.
func (tn *Tenant) mint(ctx context.Context, leaseUUID string, args ...string) (string, error) {
	args = append([]string{"-tenant", tn.key.Address, "-lease-uuid", leaseUUID}, args...)
	cmd := exec.CommandContext(ctx, filepath.Join(tn.provider.binDir, "lease-token"), args...) //nolint:gosec // G204: a binary under test and fixture-built arguments
	// The mnemonic never goes on a command line.
	cmd.Env = []string{"FRED_MNEMONIC=" + tn.key.Mnemonic}
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("lease-token: %w: %s", err, stderrOf(err))
	}
	return strings.TrimSpace(string(out)), nil
}

// request sends one request to the provider's API with a tenant token.
func (tn *Tenant) request(ctx context.Context, method, path, token string, headers map[string]string, body []byte) (int, string, error) {
	answer, err := tn.exchange(ctx, method, path, token, headers, body)
	return answer.Status, answer.Body, err
}

// exchange sends one request to the provider's API with a tenant token and
// returns the whole answer.
func (tn *Tenant) exchange(ctx context.Context, method, path, token string, headers map[string]string, body []byte) (Answer, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, tn.provider.URL+path, bytes.NewReader(body))
	if err != nil {
		return Answer{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Answer{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	answer := Answer{Status: resp.StatusCode, RetryAfter: resp.Header.Get("Retry-After"), Body: string(raw)}
	// A refusal is a JSON object; anything else leaves the fields empty.
	var refusal struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
	}
	if json.Unmarshal(raw, &refusal) == nil {
		answer.Error, answer.Reason = refusal.Error, refusal.Reason
	}
	return answer, err
}

// encodeManifest returns a manifest as the bytes a tenant uploads, and the
// hash of those bytes as a lease records it.
func encodeManifest(t *testing.T, manifest map[string]any) (payload []byte, hashHex string) {
	t.Helper()
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)
	hash := sha256.Sum256(payload)
	return payload, hex.EncodeToString(hash[:])
}

// Lease is what the chain records about a lease.
type Lease struct {
	State           string `json:"state"`
	RejectionReason string `json:"rejection_reason"`
	ClosureReason   string `json:"closure_reason"`
}

// Lease reads a lease from the chain.
func (c *Chain) Lease(t *testing.T, leaseUUID string) Lease {
	t.Helper()
	var answer struct {
		Lease Lease `json:"lease"`
	}
	c.Query(t, &answer, "billing", "lease", leaseUUID)
	return answer.Lease
}
