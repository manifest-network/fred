package api

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/manifest-network/fred/internal/testutil"
)

func TestParsePayloadAuthToken_Valid(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-auth-test-1")
	leaseUUID := testutil.ValidUUID1
	metaHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	tokenStr := testutil.CreateTestPayloadToken(kp, leaseUUID, metaHash, time.Now())

	token, err := ParsePayloadAuthToken(tokenStr)
	if err != nil {
		t.Fatalf("ParsePayloadAuthToken() error = %v", err)
	}

	if token.Tenant != kp.Address {
		t.Errorf("Tenant = %q, want %q", token.Tenant, kp.Address)
	}
	if token.LeaseUUID != leaseUUID {
		t.Errorf("LeaseUUID = %q, want %q", token.LeaseUUID, leaseUUID)
	}
	if token.MetaHash != metaHash {
		t.Errorf("MetaHash = %q, want %q", token.MetaHash, metaHash)
	}
	if token.PubKey != kp.PubKeyB64 {
		t.Errorf("PubKey = %q, want %q", token.PubKey, kp.PubKeyB64)
	}
}

func TestParsePayloadAuthToken_InvalidBase64(t *testing.T) {
	invalidTokens := []string{
		"not-base64!!!",
		"====",
		"a]b[c",
	}

	for _, tokenStr := range invalidTokens {
		_, err := ParsePayloadAuthToken(tokenStr)
		if err == nil {
			t.Errorf("ParsePayloadAuthToken(%q) = nil error, want error", tokenStr)
		}
	}
}

func TestPayloadAuthToken_Validate_ExpiredToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-auth-test-2")
	leaseUUID := testutil.ValidUUID1
	metaHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	// Create token from 10 minutes ago (expired)
	tokenStr := testutil.CreateExpiredPayloadToken(kp, leaseUUID, metaHash)

	token, err := ParsePayloadAuthToken(tokenStr)
	if err != nil {
		t.Fatalf("ParsePayloadAuthToken() error = %v", err)
	}

	err = token.Validate("manifest")
	if err == nil {
		t.Error("Validate() = nil, want error for expired token")
	}
}

func TestPayloadAuthToken_Validate_FutureToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-auth-test-3")
	leaseUUID := testutil.ValidUUID1
	metaHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	// Create token from 10 minutes in the future
	tokenStr := testutil.CreateFuturePayloadToken(kp, leaseUUID, metaHash)

	token, err := ParsePayloadAuthToken(tokenStr)
	if err != nil {
		t.Fatalf("ParsePayloadAuthToken() error = %v", err)
	}

	err = token.Validate("manifest")
	if err == nil {
		t.Error("Validate() = nil, want error for future token")
	}
}

func TestPayloadAuthToken_Validate_MissingMetaHash(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-auth-test-4")
	leaseUUID := testutil.ValidUUID1

	// Create token with empty meta_hash
	tokenStr := testutil.CreateTestPayloadToken(kp, leaseUUID, "", time.Now())

	token, err := ParsePayloadAuthToken(tokenStr)
	if err != nil {
		t.Fatalf("ParsePayloadAuthToken() error = %v", err)
	}

	err = token.Validate("manifest")
	if err == nil {
		t.Error("Validate() = nil, want error for missing meta_hash")
	}
}

func TestPayloadAuthToken_Validate_MissingLeaseUUID(t *testing.T) {
	// Create a token with empty lease_uuid
	tokenData := PayloadAuthToken{
		Tenant:    "manifest1test",
		LeaseUUID: "", // Empty
		MetaHash:  "abcdef0123456789",
		Timestamp: time.Now().Unix(),
		PubKey:    "dummy",
		Signature: "dummy",
	}

	err := tokenData.Validate("manifest")
	if err == nil {
		t.Error("Validate() = nil, want error for missing lease_uuid")
	}
}

func TestPayloadAuthToken_Validate_MissingTenant(t *testing.T) {
	// Create a token with empty tenant
	tokenData := PayloadAuthToken{
		Tenant:    "", // Empty
		LeaseUUID: testutil.ValidUUID1,
		MetaHash:  "abcdef0123456789",
		Timestamp: time.Now().Unix(),
		PubKey:    "dummy",
		Signature: "dummy",
	}

	err := tokenData.Validate("manifest")
	if err == nil {
		t.Error("Validate() = nil, want error for missing tenant")
	}
}

func TestPayloadAuthToken_Validate_InvalidPubKeyLength(t *testing.T) {
	// Create a token with invalid public key length
	tokenData := PayloadAuthToken{
		Tenant:    "manifest1test",
		LeaseUUID: testutil.ValidUUID1,
		MetaHash:  "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		Timestamp: time.Now().Unix(),
		PubKey:    base64.StdEncoding.EncodeToString([]byte("short")),
		Signature: base64.StdEncoding.EncodeToString([]byte("fake-sig")),
	}

	err := tokenData.Validate("manifest")
	if err == nil {
		t.Error("Validate() = nil, want error for invalid pub key length")
	}
}

func TestPayloadAuthToken_Validate_ValidToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-auth-test-5")
	leaseUUID := testutil.ValidUUID1
	metaHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	tokenStr := testutil.CreateTestPayloadToken(kp, leaseUUID, metaHash, time.Now())

	token, err := ParsePayloadAuthToken(tokenStr)
	if err != nil {
		t.Fatalf("ParsePayloadAuthToken() error = %v", err)
	}

	err = token.Validate("manifest")
	if err != nil {
		t.Errorf("Validate() error = %v, want nil", err)
	}
}

func TestPayloadAuthToken_Validate_WrongBech32Prefix(t *testing.T) {
	kp := testutil.NewTestKeyPair("payload-auth-test-6")
	leaseUUID := testutil.ValidUUID1
	metaHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	// Token is created with "manifest" prefix address
	tokenStr := testutil.CreateTestPayloadToken(kp, leaseUUID, metaHash, time.Now())

	token, err := ParsePayloadAuthToken(tokenStr)
	if err != nil {
		t.Fatalf("ParsePayloadAuthToken() error = %v", err)
	}

	// Validate with different prefix - should fail address verification
	err = token.Validate("cosmos")
	if err == nil {
		t.Error("Validate() = nil, want error for wrong bech32 prefix")
	}
}

func TestPayloadAuthToken_CreateSignData(t *testing.T) {
	token := &PayloadAuthToken{
		Tenant:    "manifest1abc",
		LeaseUUID: "01234567-89ab-cdef-0123-456789abcdef",
		MetaHash:  "abcdef0123456789",
		Timestamp: 1234567890,
	}

	signData := token.createSignData()
	expected := "manifest lease data 01234567-89ab-cdef-0123-456789abcdef abcdef0123456789 1234567890"

	if string(signData) != expected {
		t.Errorf("createSignData() = %q, want %q", string(signData), expected)
	}
}
