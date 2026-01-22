package api

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/manifest-network/fred/internal/testutil"
)

func TestParseAuthToken_Valid(t *testing.T) {
	kp := testutil.NewTestKeyPair("auth-test-1")
	leaseUUID := testutil.ValidUUID1

	tokenStr := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	token, err := ParseAuthToken(tokenStr)
	if err != nil {
		t.Fatalf("ParseAuthToken() error = %v", err)
	}

	if token.Tenant != kp.Address {
		t.Errorf("Tenant = %q, want %q", token.Tenant, kp.Address)
	}
	if token.LeaseUUID != leaseUUID {
		t.Errorf("LeaseUUID = %q, want %q", token.LeaseUUID, leaseUUID)
	}
	if token.PubKey != kp.PubKeyB64 {
		t.Errorf("PubKey = %q, want %q", token.PubKey, kp.PubKeyB64)
	}
}

func TestParseAuthToken_InvalidBase64(t *testing.T) {
	invalidTokens := []string{
		"not-base64!!!",
		"====",
		"a]b[c",
	}

	for _, tokenStr := range invalidTokens {
		_, err := ParseAuthToken(tokenStr)
		if err == nil {
			t.Errorf("ParseAuthToken(%q) = nil error, want error", tokenStr)
		}
	}
}

func TestParseAuthToken_InvalidJSON(t *testing.T) {
	invalidJSONs := []string{
		"",
		"not json",
		`{"tenant":`,
		`[1,2,3]`,
	}

	for _, jsonStr := range invalidJSONs {
		tokenStr := base64.StdEncoding.EncodeToString([]byte(jsonStr))
		_, err := ParseAuthToken(tokenStr)
		if err == nil {
			t.Errorf("ParseAuthToken(base64(%q)) = nil error, want error", jsonStr)
		}
	}
}

func TestValidate_ExpiredToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("auth-test-2")
	leaseUUID := testutil.ValidUUID1

	// Create token from 10 minutes ago (expired)
	tokenStr := testutil.CreateExpiredToken(kp, leaseUUID)

	token, err := ParseAuthToken(tokenStr)
	if err != nil {
		t.Fatalf("ParseAuthToken() error = %v", err)
	}

	err = token.Validate("manifest")
	if err == nil {
		t.Error("Validate() = nil, want error for expired token")
	}
}

func TestValidate_FutureToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("auth-test-3")
	leaseUUID := testutil.ValidUUID1

	// Create token from 10 minutes in the future
	tokenStr := testutil.CreateFutureToken(kp, leaseUUID)

	token, err := ParseAuthToken(tokenStr)
	if err != nil {
		t.Fatalf("ParseAuthToken() error = %v", err)
	}

	err = token.Validate("manifest")
	if err == nil {
		t.Error("Validate() = nil, want error for future token")
	}
}

func TestValidate_InvalidPubKeyLength(t *testing.T) {
	// Create a token with invalid public key length
	tokenData := AuthToken{
		Tenant:    "manifest1test",
		LeaseUUID: testutil.ValidUUID1,
		Timestamp: time.Now().Unix(),
		PubKey:    base64.StdEncoding.EncodeToString([]byte("short")),
		Signature: base64.StdEncoding.EncodeToString([]byte("fake-sig")),
	}

	err := tokenData.Validate("manifest")
	if err == nil {
		t.Error("Validate() = nil, want error for invalid pub key length")
	}
}

func TestValidate_ValidToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("auth-test-4")
	leaseUUID := testutil.ValidUUID1

	tokenStr := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	token, err := ParseAuthToken(tokenStr)
	if err != nil {
		t.Fatalf("ParseAuthToken() error = %v", err)
	}

	err = token.Validate("manifest")
	if err != nil {
		t.Errorf("Validate() error = %v, want nil", err)
	}
}

func TestValidate_WrongBech32Prefix(t *testing.T) {
	kp := testutil.NewTestKeyPair("auth-test-5")
	leaseUUID := testutil.ValidUUID1

	// Token is created with "manifest" prefix address
	tokenStr := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	token, err := ParseAuthToken(tokenStr)
	if err != nil {
		t.Fatalf("ParseAuthToken() error = %v", err)
	}

	// Validate with different prefix - should fail address verification
	err = token.Validate("cosmos")
	if err == nil {
		t.Error("Validate() = nil, want error for wrong bech32 prefix")
	}
}

func TestVerifyAddress_Valid(t *testing.T) {
	kp := testutil.NewTestKeyPair("address-test-1")

	token := &AuthToken{
		Tenant: kp.Address,
	}

	err := token.verifyAddress(kp.PubKey.Bytes(), "manifest")
	if err != nil {
		t.Errorf("verifyAddress() error = %v, want nil", err)
	}
}

func TestVerifyAddress_Mismatch(t *testing.T) {
	kp1 := testutil.NewTestKeyPair("address-test-2")
	kp2 := testutil.NewTestKeyPair("address-test-3")

	// Token with kp1's address but kp2's public key
	token := &AuthToken{
		Tenant: kp1.Address,
	}

	err := token.verifyAddress(kp2.PubKey.Bytes(), "manifest")
	if err == nil {
		t.Error("verifyAddress() = nil, want error for mismatched address")
	}
}

func TestVerifyAddress_InvalidPubKeyLength(t *testing.T) {
	token := &AuthToken{
		Tenant: "manifest1test",
	}

	err := token.verifyAddress([]byte("short"), "manifest")
	if err == nil {
		t.Error("verifyAddress() = nil, want error for invalid pub key length")
	}
}

func TestCreateSignData(t *testing.T) {
	token := &AuthToken{
		Tenant:    "manifest1abc",
		LeaseUUID: "01234567-89ab-cdef-0123-456789abcdef",
		Timestamp: 1234567890,
	}

	signData := token.createSignData()
	expected := "manifest1abc:01234567-89ab-cdef-0123-456789abcdef:1234567890"

	if string(signData) != expected {
		t.Errorf("createSignData() = %q, want %q", string(signData), expected)
	}
}

func TestAuthToken_RoundTrip(t *testing.T) {
	kp := testutil.NewTestKeyPair("roundtrip-test")
	leaseUUID := testutil.ValidUUID1
	now := time.Now()

	// Create token
	tokenStr := testutil.CreateTestToken(kp, leaseUUID, now)

	// Parse it back
	token, err := ParseAuthToken(tokenStr)
	if err != nil {
		t.Fatalf("ParseAuthToken() error = %v", err)
	}

	// Validate
	if err := token.Validate("manifest"); err != nil {
		t.Errorf("Validate() error = %v", err)
	}

	// Re-encode and compare
	jsonBytes, err := json.Marshal(token)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	reencoded := base64.StdEncoding.EncodeToString(jsonBytes)

	if reencoded != tokenStr {
		t.Errorf("Token re-encoding mismatch")
	}
}
