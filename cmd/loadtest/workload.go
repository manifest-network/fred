package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sdksecp "github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/google/uuid"

	"github.com/manifest-network/fred/internal/adr036"
	"github.com/manifest-network/fred/internal/api"
	"github.com/manifest-network/fred/internal/auth"
	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/httpurl"
	"github.com/manifest-network/fred/internal/provisioner/callbackwire"
	"github.com/manifest-network/fred/internal/strictjson"
)

const maxFixtureBytes = 16 << 20

type requestFactory func(context.Context) (*http.Request, error)

// workload contains only factories whose credentials and fixture prerequisites
// were validated together. Workers cannot select an unconfigured operation.
type workload struct{ requests []requestFactory }

type workloadConfig struct {
	target, scenario, traffic, fixtures, keyFile, prefix, callbackSecret string
	payloadSize                                                          int
}

type leaseFixture struct {
	LeaseUUID string `json:"lease_uuid"`
	Payload   []byte `json:"payload,omitempty"`
}

type callbackFixture struct {
	RequestURI string `json:"request_uri"`
	Body       []byte `json:"body"`
}

type fixtureFile struct {
	Leases    []leaseFixture    `json:"leases,omitempty"`
	Callbacks []callbackFixture `json:"callbacks,omitempty"`
}

func readBoundedFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path) // #nosec G304 -- explicit operator fixture/key path
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("input file exceeds %d bytes", limit)
	}
	return data, nil
}

func loadWorkload(cfg workloadConfig) (workload, error) {
	origin, err := httpurl.NormalizeOrigin(cfg.target)
	if err != nil {
		return workload{}, fmt.Errorf("target: %w", err)
	}
	if cfg.traffic == "rejection" {
		if cfg.fixtures != "" || cfg.keyFile != "" || cfg.callbackSecret != "" {
			return workload{}, errors.New("rejection traffic must not receive fixtures or credentials")
		}
		if cfg.payloadSize <= 0 || cfg.payloadSize > 1<<20 {
			return workload{}, errors.New("rejection payload-size must be 1..1048576 bytes")
		}
		return rejectionWorkload(origin, cfg.scenario, cfg.payloadSize)
	}
	if cfg.traffic != "authenticated" {
		return workload{}, errors.New("traffic must be authenticated or rejection")
	}
	if cfg.fixtures == "" {
		return workload{}, errors.New("authenticated traffic requires -fixtures; use -traffic=rejection for invalid requests")
	}
	data, err := readBoundedFile(cfg.fixtures, maxFixtureBytes)
	if err != nil {
		return workload{}, fmt.Errorf("read fixtures: %w", err)
	}
	var fixtures fixtureFile
	if err := strictjson.DecodeObject(data, maxFixtureBytes, &fixtures); err != nil {
		return workload{}, fmt.Errorf("decode fixtures: %w", err)
	}
	if len(fixtures.Leases)+len(fixtures.Callbacks) > 1000 {
		return workload{}, errors.New("at most 1000 fixture entries are allowed")
	}
	var payloads []leaseFixture
	seen := make(map[string]bool)
	for _, lease := range fixtures.Leases {
		if !backend.IsCanonicalLeaseUUID(lease.LeaseUUID) || seen[lease.LeaseUUID] {
			return workload{}, errors.New("fixture lease UUIDs must be canonical and unique")
		}
		seen[lease.LeaseUUID] = true
		if len(lease.Payload) != 0 {
			if _, err := manifest.ParsePayload(lease.Payload); err != nil {
				return workload{}, fmt.Errorf("fixture payload: %w", err)
			}
			payloads = append(payloads, lease)
		}
	}
	var payload, connection, callback requestFactory
	if len(fixtures.Leases) != 0 {
		signer, err := loadTenantSigner(cfg.keyFile, cfg.prefix)
		if err != nil {
			return workload{}, err
		}
		connection = signer.connectionFactory(origin, fixtures.Leases)
		if len(payloads) != 0 {
			payload = signer.payloadFactory(origin, payloads)
		}
	}
	if len(fixtures.Callbacks) != 0 {
		callback, err = recordedCallbackFactory(origin, cfg.callbackSecret, fixtures.Callbacks)
		if err != nil {
			return workload{}, err
		}
	}
	return selectWorkload(cfg.scenario, payload, connection, callback)
}

func selectWorkload(scenario string, payload, connection, callback requestFactory) (workload, error) {
	var selected []requestFactory
	switch scenario {
	case "payload":
		selected = []requestFactory{payload}
	case "connection":
		selected = []requestFactory{connection}
	case "callback":
		selected = []requestFactory{callback}
	case "mixed":
		selected = []requestFactory{payload, payload, payload, payload,
			connection, connection, connection, connection, connection}
		if callback != nil {
			selected = append(selected, callback)
		} else {
			selected = append(selected, connection)
		}
	default:
		return workload{}, errors.New("scenario must be payload, connection, callback or mixed")
	}
	for _, factory := range selected {
		if factory == nil {
			return workload{}, errors.New("scenario lacks required lease, payload or callback fixtures")
		}
	}
	return workload{requests: selected}, nil
}

type tenantSigner struct {
	key     *sdksecp.PrivKey
	tenant  string
	pubKey  string
	mu      sync.Mutex
	lastUse map[string]int64
}

func loadTenantSigner(path, prefix string) (*tenantSigner, error) {
	if path == "" {
		return nil, errors.New("lease fixtures require -tenant-key-file")
	}
	encoded, err := readBoundedFile(path, 1024)
	if err != nil {
		return nil, fmt.Errorf("read tenant key: %w", err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(string(encoded)))
	if err != nil || len(raw) != sdksecp.PrivKeySize {
		return nil, errors.New("tenant key must contain exactly 64 hexadecimal digits")
	}
	var scalar secp256k1.ModNScalar
	if scalar.SetByteSlice(raw) || scalar.IsZero() {
		return nil, errors.New("tenant key is outside the secp256k1 scalar range")
	}
	key := &sdksecp.PrivKey{Key: raw}
	tenant, err := sdk.Bech32ifyAddressBytes(prefix, key.PubKey().Address())
	if err != nil {
		return nil, fmt.Errorf("tenant address prefix: %w", err)
	}
	return &tenantSigner{key: key, tenant: tenant,
		pubKey: base64.StdEncoding.EncodeToString(key.PubKey().Bytes()), lastUse: make(map[string]int64)}, nil
}

func (s *tenantSigner) token(leaseUUID string, payload []byte, timestamp int64) (string, error) {
	var token any
	if payload != nil {
		digest := sha256.Sum256(payload)
		hash := hex.EncodeToString(digest[:])
		signature, err := s.key.Sign(adr036.CreateSignBytes(auth.FormatPayloadSignData(leaseUUID, hash, timestamp), s.tenant))
		if err != nil {
			return "", err
		}
		token = api.PayloadAuthToken{Tenant: s.tenant, LeaseUUID: leaseUUID, MetaHash: hash,
			Timestamp: timestamp, PubKey: s.pubKey, Signature: base64.StdEncoding.EncodeToString(signature)}
	} else {
		signature, err := s.key.Sign(adr036.CreateSignBytes(auth.FormatSignData(s.tenant, leaseUUID, timestamp), s.tenant))
		if err != nil {
			return "", err
		}
		token = api.AuthToken{Tenant: s.tenant, LeaseUUID: leaseUUID, Timestamp: timestamp,
			PubKey: s.pubKey, Signature: base64.StdEncoding.EncodeToString(signature)}
	}
	encoded, err := json.Marshal(token)
	return base64.StdEncoding.EncodeToString(encoded), err
}

// Connection tokens are single-use and carry second-resolution timestamps.
// Pace each lease instead of sending duplicates or inventing future timestamps.
func (s *tenantSigner) nextConnectionTimestamp(ctx context.Context, lease string) (int64, error) {
	for {
		s.mu.Lock()
		now := time.Now()
		last := s.lastUse[lease]
		if now.Unix() > last {
			s.lastUse[lease] = now.Unix()
			s.mu.Unlock()
			return now.Unix(), nil
		}
		s.mu.Unlock()
		if err := waitFor(ctx, time.Until(time.Unix(last+1, 0))); err != nil {
			return 0, err
		}
	}
}

func (s *tenantSigner) connectionFactory(origin string, leases []leaseFixture) requestFactory {
	var index atomic.Uint64
	return func(ctx context.Context) (*http.Request, error) {
		lease := leases[(index.Add(1)-1)%uint64(len(leases))]
		timestamp, err := s.nextConnectionTimestamp(ctx, lease.LeaseUUID)
		if err != nil {
			return nil, err
		}
		token, err := s.token(lease.LeaseUUID, nil, timestamp)
		if err != nil {
			return nil, err
		}
		return tenantRequest(ctx, origin, lease, token, false)
	}
}

func (s *tenantSigner) payloadFactory(origin string, leases []leaseFixture) requestFactory {
	var index atomic.Uint64
	return func(ctx context.Context) (*http.Request, error) {
		lease := leases[(index.Add(1)-1)%uint64(len(leases))]
		token, err := s.token(lease.LeaseUUID, lease.Payload, time.Now().Unix())
		if err != nil {
			return nil, err
		}
		return tenantRequest(ctx, origin, lease, token, true)
	}
}

func tenantRequest(ctx context.Context, origin string, lease leaseFixture, token string, payload bool) (*http.Request, error) {
	method, suffix := http.MethodGet, "/connection"
	var body []byte
	if payload {
		method, suffix, body = http.MethodPost, "/data", lease.Payload
	}
	req, err := http.NewRequestWithContext(ctx, method, origin+"/v1/leases/"+lease.LeaseUUID+suffix, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if payload {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	return req, nil
}

func recordedCallbackFactory(origin, secret string, callbacks []callbackFixture) (requestFactory, error) {
	if len(secret) < hmacauth.MinSecretLength {
		return nil, errors.New("recorded callbacks require a callback-secret of at least 32 bytes")
	}
	proofVerifier, _ := hmacauth.NewCallbackProofBoundary()
	for _, fixture := range callbacks {
		parsed, err := url.ParseRequestURI(fixture.RequestURI)
		if err != nil || parsed.IsAbs() || parsed.Host != "" || !strings.HasPrefix(fixture.RequestURI, "/") || strings.HasPrefix(fixture.RequestURI, "//") {
			return nil, errors.New("callback request_uri must be an origin-relative request URI")
		}
		storageID, err := callbackwire.SelectUntrustedStorageRoute(fixture.Body)
		if err != nil {
			return nil, fmt.Errorf("callback fixture: %w", err)
		}
		verifier, err := api.NewCallbackKeyringAuthenticator(map[backendidentity.ID]string{storageID: secret}, proofVerifier)
		if err != nil {
			return nil, err
		}
		req, err := callbackRequest(context.Background(), origin, secret, fixture)
		if err != nil {
			return nil, err
		}
		proof, err := verifier.VerifyCallbackEvidence(req)
		if err != nil {
			return nil, fmt.Errorf("callback fixture authentication: %w", err)
		}
		observation, err := callbackwire.DecodeVerified(proof)
		if err != nil {
			return nil, fmt.Errorf("callback fixture: %w", err)
		}
		if observation.Selector() == callbackwire.SelectorLegacy || observation.BackendName() == "" {
			return nil, errors.New("callback fixtures must carry an exact operation/lifecycle route and backend identity")
		}
	}
	var index atomic.Uint64
	return func(ctx context.Context) (*http.Request, error) {
		fixture := callbacks[(index.Add(1)-1)%uint64(len(callbacks))]
		return callbackRequest(ctx, origin, secret, fixture)
	}, nil
}

func callbackRequest(ctx context.Context, origin, secret string, fixture callbackFixture) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, origin+fixture.RequestURI, bytes.NewReader(fixture.Body))
	if err != nil {
		return nil, err
	}
	if req.URL.RequestURI() != fixture.RequestURI {
		return nil, errors.New("callback request URI changed during parsing")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(hmacauth.SignatureHeader, hmacauth.SignRequest(secret, req, fixture.Body))
	return req, nil
}

func rejectionWorkload(origin, scenario string, payloadSize int) (workload, error) {
	payload := func(ctx context.Context) (*http.Request, error) {
		body := make([]byte, payloadSize)
		rand.Read(body)
		return tenantRequest(ctx, origin, leaseFixture{LeaseUUID: uuid.NewString(), Payload: body}, "intentionally-invalid-token", true)
	}
	connection := func(ctx context.Context) (*http.Request, error) {
		return tenantRequest(ctx, origin, leaseFixture{LeaseUUID: uuid.NewString()}, "intentionally-invalid-token", false)
	}
	callback := func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodPost, origin+"/callbacks/provision", strings.NewReader(`{"status":"success"}`))
	}
	return selectWorkload(scenario, payload, connection, callback)
}
