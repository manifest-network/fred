// Command lease-token mints a Fred tenant bearer token (ADR-036 signed) from a
// real BIP39 mnemonic, for authenticating to lease endpoints such as
// POST /v1/leases/{lease_uuid}/restore.
//
// Usage:
//
//	lease-token -mnemonic "word1 word2 ... word24" \
//	            -tenant manifest1... \
//	            -lease-uuid <NEW lease uuid in the request path>
//
// The token is bound to the lease UUID in the request path (the restore handler
// enforces token.lease_uuid == path lease_uuid), so -lease-uuid must be the NEW
// lease UUID, not the source lease being restored from. Tokens are valid for a
// short window (server MaxTokenAge is 30s), so mint immediately before use.
//
// It prints the base64 bearer token to stdout (followed by a newline for
// readability). To use it:
//
//	TOKEN=$(lease-token -mnemonic "$MNEMONIC" -tenant "$TENANT" -lease-uuid "$UUID")
//	curl -H "Authorization: Bearer $TOKEN" ...
package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	sdktypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/manifest-network/fred/internal/adr036"
	"github.com/manifest-network/fred/internal/auth"
)

const (
	// cosmosCoinType is the BIP44 coin type for Cosmos accounts (118 = ATOM).
	// Combined with account 0 / index 0 this yields the standard derivation
	// path m/44'/118'/0'/0/0.
	cosmosCoinType = 118

	// bech32Prefix is the address prefix for the Manifest chain. It must match
	// the prefix the server uses to verify the tenant address (the value passed
	// to AuthToken.Validate at the call site).
	bech32Prefix = "manifest"
)

// authToken mirrors the JSON envelope of api.AuthToken (internal/api/auth.go).
// Field names and types must stay in lockstep with that struct, since the
// server unmarshals this exact shape.
type authToken struct {
	Tenant    string `json:"tenant"`
	LeaseUUID string `json:"lease_uuid"`
	Timestamp int64  `json:"timestamp"`  // unix seconds
	PubKey    string `json:"pub_key"`    // base64 of 33-byte compressed secp256k1 pubkey
	Signature string `json:"signature"`  // base64 of 64-byte ADR-036 signature
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "lease-token:", err)
		os.Exit(1)
	}
}

func run() error {
	mnemonic := flag.String("mnemonic", "", "BIP39 mnemonic for the tenant account (required)")
	tenant := flag.String("tenant", "", "expected bech32 tenant address (manifest1...) (required)")
	leaseUUID := flag.String("lease-uuid", "", "lease UUID to bind the token to (the request-path UUID) (required)")
	timestamp := flag.Int64("timestamp", 0, "unix timestamp to sign (default: now); the server allows ~30s past / ~10s future")
	flag.Parse()

	if *mnemonic == "" || *tenant == "" || *leaseUUID == "" {
		flag.Usage()
		return fmt.Errorf("-mnemonic, -tenant and -lease-uuid are all required")
	}

	// Derive the secp256k1 private key from the mnemonic at m/44'/118'/0'/0/0.
	// hd.Secp256k1.Derive() returns a DeriveFn; go-bip39 validates the mnemonic
	// inside it, so an invalid mnemonic surfaces as an error here.
	hdPath := hd.CreateHDPath(cosmosCoinType, 0, 0).String()
	derived, err := hd.Secp256k1.Derive()(*mnemonic, "", hdPath)
	if err != nil {
		return fmt.Errorf("derive key from mnemonic: %w", err)
	}
	privKey := hd.Secp256k1.Generate()(derived).(*secp256k1.PrivKey)
	pubKey := privKey.PubKey().(*secp256k1.PubKey)

	// Verify the derived key matches the supplied tenant address. This is the
	// same check the server performs in tokenValidator.verifyAddress, and it
	// catches a wrong mnemonic / HD path before we bother signing.
	derivedAddr, err := sdktypes.Bech32ifyAddressBytes(bech32Prefix, pubKey.Address().Bytes())
	if err != nil {
		return fmt.Errorf("encode bech32 address: %w", err)
	}
	if derivedAddr != *tenant {
		return fmt.Errorf("derived address %s does not match -tenant %s (wrong mnemonic or HD path)", derivedAddr, *tenant)
	}

	ts := *timestamp
	if ts == 0 {
		ts = time.Now().Unix()
	}

	// Build the sign data ("{tenant}:{lease_uuid}:{timestamp}") using the same
	// helper the server uses to recompute it, so the formats cannot drift.
	signData := auth.FormatSignData(*tenant, *leaseUUID, ts)

	// ADR-036 sign: sign the canonical StdSignDoc bytes with the tenant address
	// as the signer. The server normalizes to low-S on verify, so we don't.
	signBytes := adr036.CreateSignBytes(signData, *tenant)
	sig, err := privKey.Sign(signBytes)
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}

	tok := authToken{
		Tenant:    *tenant,
		LeaseUUID: *leaseUUID,
		Timestamp: ts,
		PubKey:    base64.StdEncoding.EncodeToString(pubKey.Bytes()),
		Signature: base64.StdEncoding.EncodeToString(sig),
	}
	jsonBytes, err := json.Marshal(tok)
	if err != nil {
		return fmt.Errorf("marshal token: %w", err)
	}

	fmt.Println(base64.StdEncoding.EncodeToString(jsonBytes))
	return nil
}
