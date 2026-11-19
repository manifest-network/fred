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
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/testutil"
)

// Lease states as the chain's JSON output spells them.
const (
	LeasePending = "LEASE_STATE_PENDING"
	LeaseActive  = "LEASE_STATE_ACTIVE"
	LeaseClosed  = "LEASE_STATE_CLOSED"
)

// Tenant is a funded account that leases from a Provider, the way a tenant
// does: transactions on the chain, and the manifest to the provider's API.
type Tenant struct {
	provider *Provider
	key      Key
	signer   *testutil.TestKeyPair
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
	return &Tenant{provider: p, key: key, signer: signer}
}

// Deploy creates a lease for one unit of a SKU and uploads its manifest. The
// chain records the manifest's hash with the lease, and the provider deploys
// only a manifest with that hash. It returns the lease's UUID.
func (tn *Tenant) Deploy(t *testing.T, sku string, manifest map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)
	hash := sha256.Sum256(payload)
	hashHex := hex.EncodeToString(hash[:])

	before := tn.leases(t)
	tn.provider.Chain.Tx(t, tn.key.Name, "billing", "create-lease", sku+":1", "--meta-hash", hashHex)
	var leaseUUID string
	for uuid := range tn.leases(t) {
		if _, existed := before[uuid]; !existed {
			require.Empty(t, leaseUUID, "one transaction created more than one lease")
			leaseUUID = uuid
		}
	}
	require.NotEmpty(t, leaseUUID, "create-lease was committed but the tenant has no new lease")

	// providerd serves its API before it is ready to take payloads, so the
	// upload is repeated until it is accepted. A token is good for one
	// request.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		token := testutil.CreateTestPayloadToken(tn.signer, leaseUUID, hashHex, time.Now())
		status, body, err := tn.post(t.Context(), "/v1/leases/"+leaseUUID+"/data", token, payload)
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

// leases maps the UUID of each of the tenant's leases to its state.
func (tn *Tenant) leases(t *testing.T) map[string]string {
	t.Helper()
	var answer struct {
		Leases []struct {
			UUID  string `json:"uuid"`
			State string `json:"state"`
		} `json:"leases"`
	}
	tn.provider.Chain.Query(t, &answer, "billing", "leases-by-tenant", tn.key.Address)
	leases := make(map[string]string, len(answer.Leases))
	for _, lease := range answer.Leases {
		leases[lease.UUID] = lease.State
	}
	return leases
}

func (tn *Tenant) post(ctx context.Context, path, token string, body []byte) (int, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tn.provider.URL+path, bytes.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	answer, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, string(answer), err
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
