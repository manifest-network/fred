package api

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/testutil"
)

func TestParseAuthToken_Valid(t *testing.T) {
	kp := testutil.NewTestKeyPair("auth-test-1")
	leaseUUID := testutil.ValidUUID1

	tokenStr := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	token, err := ParseAuthToken(tokenStr)
	require.NoError(t, err)

	assert.Equal(t, kp.Address, token.Tenant)
	assert.Equal(t, leaseUUID, token.LeaseUUID)
	assert.Equal(t, kp.PubKeyB64, token.PubKey)
}

func TestParseAuthToken_InvalidBase64(t *testing.T) {
	invalidTokens := []string{
		"not-base64!!!",
		"====",
		"a]b[c",
	}

	for _, tokenStr := range invalidTokens {
		_, err := ParseAuthToken(tokenStr)
		assert.Error(t, err)
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
		assert.Error(t, err)
	}
}

func TestValidate_ExpiredToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("auth-test-2")
	leaseUUID := testutil.ValidUUID1

	// Create token from 10 minutes ago (expired)
	tokenStr := testutil.CreateExpiredToken(kp, leaseUUID)

	token, err := ParseAuthToken(tokenStr)
	require.NoError(t, err)

	err = token.Validate("manifest")
	assert.Error(t, err)
}

func TestValidate_FutureToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("auth-test-3")
	leaseUUID := testutil.ValidUUID1

	// Create token from 10 minutes in the future
	tokenStr := testutil.CreateFutureToken(kp, leaseUUID)

	token, err := ParseAuthToken(tokenStr)
	require.NoError(t, err)

	err = token.Validate("manifest")
	assert.Error(t, err)
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
	assert.Error(t, err)
}

func TestValidate_MissingLeaseUUID(t *testing.T) {
	// Create a token with empty lease_uuid
	tokenData := AuthToken{
		Tenant:    "manifest1test",
		LeaseUUID: "", // Empty
		Timestamp: time.Now().Unix(),
		PubKey:    "dummy",
		Signature: "dummy",
	}

	err := tokenData.Validate("manifest")
	assert.Error(t, err)
}

func TestValidate_MissingTenant(t *testing.T) {
	// Create a token with empty tenant
	tokenData := AuthToken{
		Tenant:    "", // Empty
		LeaseUUID: testutil.ValidUUID1,
		Timestamp: time.Now().Unix(),
		PubKey:    "dummy",
		Signature: "dummy",
	}

	err := tokenData.Validate("manifest")
	assert.Error(t, err)
}

func TestValidate_ValidToken(t *testing.T) {
	kp := testutil.NewTestKeyPair("auth-test-4")
	leaseUUID := testutil.ValidUUID1

	tokenStr := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	token, err := ParseAuthToken(tokenStr)
	require.NoError(t, err)

	err = token.Validate("manifest")
	assert.NoError(t, err)
}

func TestValidate_WrongBech32Prefix(t *testing.T) {
	kp := testutil.NewTestKeyPair("auth-test-5")
	leaseUUID := testutil.ValidUUID1

	// Token is created with "manifest" prefix address
	tokenStr := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	token, err := ParseAuthToken(tokenStr)
	require.NoError(t, err)

	// Validate with different prefix - should fail address verification
	err = token.Validate("cosmos")
	assert.Error(t, err)
}

func TestVerifyAddress_Valid(t *testing.T) {
	kp := testutil.NewTestKeyPair("address-test-1")

	v := &tokenValidator{
		tenant: kp.Address,
	}

	err := v.verifyAddress(kp.PubKey.Bytes(), "manifest")
	assert.NoError(t, err)
}

func TestVerifyAddress_Mismatch(t *testing.T) {
	kp1 := testutil.NewTestKeyPair("address-test-2")
	kp2 := testutil.NewTestKeyPair("address-test-3")

	// Validator with kp1's address but kp2's public key
	v := &tokenValidator{
		tenant: kp1.Address,
	}

	err := v.verifyAddress(kp2.PubKey.Bytes(), "manifest")
	assert.Error(t, err)
}

func TestVerifyAddress_InvalidPubKeyLength(t *testing.T) {
	v := &tokenValidator{
		tenant: "manifest1test",
	}

	err := v.verifyAddress([]byte("short"), "manifest")
	assert.Error(t, err)
}

func TestCreateSignData(t *testing.T) {
	token := &AuthToken{
		Tenant:    "manifest1abc",
		LeaseUUID: "01234567-89ab-cdef-0123-456789abcdef",
		Timestamp: 1234567890,
	}

	signData := token.createSignData()
	expected := "manifest1abc:01234567-89ab-cdef-0123-456789abcdef:1234567890"

	assert.Equal(t, expected, string(signData))
}

func TestAuthToken_RoundTrip(t *testing.T) {
	kp := testutil.NewTestKeyPair("roundtrip-test")
	leaseUUID := testutil.ValidUUID1
	now := time.Now()

	// Create token
	tokenStr := testutil.CreateTestToken(kp, leaseUUID, now)

	// Parse it back
	token, err := ParseAuthToken(tokenStr)
	require.NoError(t, err)

	// Validate
	err = token.Validate("manifest")
	assert.NoError(t, err)

	// Re-encode and compare
	jsonBytes, err := json.Marshal(token)
	require.NoError(t, err)
	reencoded := base64.StdEncoding.EncodeToString(jsonBytes)

	assert.Equal(t, tokenStr, reencoded)
}
