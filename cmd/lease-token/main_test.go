package main

import (
	"os"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/api"
)

// testMnemonic is the canonical publicly-known BIP39 test vector ("abandon"×11 +
// "about"). It is NOT a secret — it is the standard zero-entropy mnemonic used
// across the ecosystem — so its derived address is deterministic for assertions.
const testMnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"

// deriveTenant derives the manifest1 bech32 address for the given mnemonic at the
// same HD path mintToken uses (m/44'/118'/0'/0/0). We derive in-test rather than
// hardcoding the string so the test stays valid if the bech32 prefix constant
// changes, while still asserting determinism for a fixed mnemonic.
func deriveTenant(t *testing.T, mnemonic string) string {
	t.Helper()
	hdPath := hd.CreateHDPath(cosmosCoinType, 0, 0).String()
	derived, err := hd.Secp256k1.Derive()(mnemonic, "", hdPath)
	require.NoError(t, err)
	privKey := hd.Secp256k1.Generate()(derived).(*secp256k1.PrivKey)
	pubKey := privKey.PubKey().(*secp256k1.PubKey)
	addr, err := sdktypes.Bech32ifyAddressBytes(bech32Prefix, pubKey.Address().Bytes())
	require.NoError(t, err)
	return addr
}

// TestMintToken_RoundTrip mints a token with the real signer and feeds it through
// the REAL server-side verifier (api.ParseAuthToken + AuthToken.Validate). The
// token must validate and round-trip the tenant + lease UUID, proving the CLI and
// the server agree on the sign-data format, ADR-036 encoding, and JSON envelope.
func TestMintToken_RoundTrip(t *testing.T) {
	tenant := deriveTenant(t, testMnemonic)
	const leaseUUID = "0192abcd-1234-7000-8000-aabbccddeeff"
	ts := time.Now().Unix()

	encoded, err := mintToken(testMnemonic, tenant, leaseUUID, ts)
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	tok, err := api.ParseAuthToken(encoded)
	require.NoError(t, err, "the minted token must be a parseable base64 JSON envelope")
	require.NotNil(t, tok)

	// The REAL verifier: timestamp window + ADR-036 signature + pub_key->address.
	require.NoError(t, tok.Validate(bech32Prefix), "the minted token must pass the server verifier")

	assert.Equal(t, tenant, tok.Tenant, "verifier must round-trip the tenant")
	assert.Equal(t, leaseUUID, tok.LeaseUUID, "verifier must round-trip the lease UUID")
	assert.Equal(t, ts, tok.Timestamp, "the signed timestamp must round-trip")
}

// TestMintToken_WrongTenant: minting with a tenant address that does not match the
// mnemonic-derived address is rejected before signing (the same address cross-check
// the server enforces), with the descriptive "does not match" error.
func TestMintToken_WrongTenant(t *testing.T) {
	const wrongTenant = "manifest1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqg3rl3vp" // not derived from testMnemonic

	encoded, err := mintToken(testMnemonic, wrongTenant, "lease-1", time.Now().Unix())
	require.Error(t, err)
	assert.Empty(t, encoded, "no token must be produced on an address mismatch")
	assert.Contains(t, err.Error(), "does not match",
		"the error must name the derived/expected address mismatch")
}

// TestReadMnemonic covers the three input sources: $FRED_MNEMONIC (highest
// priority), stdin pipe fallback, and the both-empty error.
func TestReadMnemonic(t *testing.T) {
	t.Run("env wins", func(t *testing.T) {
		t.Setenv("FRED_MNEMONIC", testMnemonic)
		got, err := readMnemonic()
		require.NoError(t, err)
		assert.Equal(t, testMnemonic, got)
	})

	t.Run("stdin fallback when env empty", func(t *testing.T) {
		t.Setenv("FRED_MNEMONIC", "")

		r, w, err := os.Pipe()
		require.NoError(t, err)
		orig := os.Stdin
		os.Stdin = r
		t.Cleanup(func() { os.Stdin = orig })

		// Write the piped mnemonic (with surrounding whitespace to confirm trimming),
		// then close so io.ReadAll returns.
		go func() {
			_, _ = w.WriteString("  " + testMnemonic + "\n")
			_ = w.Close()
		}()

		got, err := readMnemonic()
		require.NoError(t, err)
		assert.Equal(t, testMnemonic, got, "stdin value must be trimmed")
	})

	t.Run("both empty is an error", func(t *testing.T) {
		t.Setenv("FRED_MNEMONIC", "")

		r, w, err := os.Pipe()
		require.NoError(t, err)
		orig := os.Stdin
		os.Stdin = r
		t.Cleanup(func() { os.Stdin = orig })

		// Empty stdin: close immediately so io.ReadAll returns "".
		_ = w.Close()

		got, err := readMnemonic()
		require.Error(t, err)
		assert.Empty(t, got)
		assert.Contains(t, err.Error(), "no mnemonic provided")
	})
}
