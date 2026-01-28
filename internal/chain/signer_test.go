package chain

import (
	"context"
	"testing"

	"cosmossdk.io/math"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptocodec "github.com/cosmos/cosmos-sdk/crypto/codec"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// newTestMsg creates a MsgAcknowledgeLease for the given signer address.
func newTestMsg(sender string) *billingtypes.MsgAcknowledgeLease {
	return &billingtypes.MsgAcknowledgeLease{
		Sender:     sender,
		LeaseUuids: []string{"test-uuid"},
	}
}

// newTestSigner creates a Signer backed by the in-memory "test" keyring.
func newTestSigner(t *testing.T) *Signer {
	t.Helper()

	interfaceRegistry := codectypes.NewInterfaceRegistry()
	cryptocodec.RegisterInterfaces(interfaceRegistry)
	authtypes.RegisterInterfaces(interfaceRegistry)
	billingtypes.RegisterInterfaces(interfaceRegistry)
	cdc := codec.NewProtoCodec(interfaceRegistry)

	kr := keyring.NewInMemory(cdc)

	supported, _ := kr.SupportedAlgorithms()
	_, _, err := kr.NewMnemonic("testkey", keyring.English, sdk.FullFundraiserPath, keyring.DefaultBIP39Passphrase, supported[0])
	if err != nil {
		t.Fatalf("failed to create test key: %v", err)
	}

	keyInfo, err := kr.Key("testkey")
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}
	addr, err := keyInfo.GetAddress()
	if err != nil {
		t.Fatalf("failed to get address: %v", err)
	}

	txConfig := authtx.NewTxConfig(cdc, authtx.DefaultSignModes)

	return &Signer{
		keyring:  kr,
		keyName:  "testkey",
		address:  addr.String(),
		chainID:  "test-1",
		txConfig: txConfig,
		cdc:      cdc,
		gasLimit: 200000,
		gasPrice: 100,
		feeDenom: "umfx",
	}
}

// newTestAccountAny packs a BaseAccount into *codectypes.Any for SignTx.
func newTestAccountAny(t *testing.T, addr sdk.AccAddress, accNum, seq uint64) *codectypes.Any {
	t.Helper()

	account := authtypes.NewBaseAccount(addr, nil, accNum, seq)
	accountAny, err := codectypes.NewAnyWithValue(account)
	if err != nil {
		t.Fatalf("failed to create account Any: %v", err)
	}
	return accountAny
}

func TestNewSigner_Success(t *testing.T) {
	s := newTestSigner(t)

	if s.address == "" {
		t.Error("address is empty, want non-empty bech32 address")
	}
}

func TestNewSigner_KeyNotFound(t *testing.T) {
	_, err := NewSigner(SignerConfig{
		KeyringBackend: "test",
		KeyringDir:     t.TempDir(),
		KeyName:        "nonexistent",
		ChainID:        "test-1",
	})
	if err == nil {
		t.Error("NewSigner() with missing key should return error")
	}
}

func TestNewSigner_InvalidBackend(t *testing.T) {
	_, err := NewSigner(SignerConfig{
		KeyringBackend: "invalid-backend",
		KeyringDir:     t.TempDir(),
		KeyName:        "testkey",
		ChainID:        "test-1",
	})
	if err == nil {
		t.Error("NewSigner() with invalid backend should return error")
	}
}

func TestSigner_Address(t *testing.T) {
	s := newTestSigner(t)

	addr := s.Address()
	if addr == "" {
		t.Fatal("Address() returned empty string")
	}

	// The address should start with the default cosmos prefix or manifest prefix
	// depending on SDK config. Just verify it's non-empty and parseable.
	_, err := sdk.AccAddressFromBech32(addr)
	if err != nil {
		t.Errorf("Address() returned invalid bech32 address %q: %v", addr, err)
	}
}

func TestSigner_SignTx(t *testing.T) {
	s := newTestSigner(t)

	addr, err := sdk.AccAddressFromBech32(s.address)
	if err != nil {
		t.Fatalf("invalid signer address: %v", err)
	}

	accountAny := newTestAccountAny(t, addr, 42, 7)

	txBytes, err := s.SignTx(context.Background(), newTestMsg(s.address), accountAny)
	if err != nil {
		t.Fatalf("SignTx() error = %v", err)
	}

	if len(txBytes) == 0 {
		t.Fatal("SignTx() returned empty bytes")
	}

	// Decode and verify the transaction
	tx, err := s.txConfig.TxDecoder()(txBytes)
	if err != nil {
		t.Fatalf("failed to decode tx: %v", err)
	}

	feeTx, ok := tx.(sdk.FeeTx)
	if !ok {
		t.Fatal("decoded tx does not implement sdk.FeeTx")
	}

	// gasLimit=200000, gasPrice=100 -> 200000*100/1_000_000 = 20
	expectedFee := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(20)))
	if !feeTx.GetFee().Equal(expectedFee) {
		t.Errorf("fee = %v, want %v", feeTx.GetFee(), expectedFee)
	}
}

func TestSigner_SignTx_MinimumFee(t *testing.T) {
	s := newTestSigner(t)
	// Set gasPrice so that gasLimit*gasPrice/1e6 < 1
	s.gasLimit = 1
	s.gasPrice = 1

	addr, err := sdk.AccAddressFromBech32(s.address)
	if err != nil {
		t.Fatalf("invalid signer address: %v", err)
	}

	accountAny := newTestAccountAny(t, addr, 0, 0)

	txBytes, err := s.SignTx(context.Background(), newTestMsg(s.address), accountAny)
	if err != nil {
		t.Fatalf("SignTx() error = %v", err)
	}

	tx, err := s.txConfig.TxDecoder()(txBytes)
	if err != nil {
		t.Fatalf("failed to decode tx: %v", err)
	}

	feeTx, ok := tx.(sdk.FeeTx)
	if !ok {
		t.Fatal("decoded tx does not implement sdk.FeeTx")
	}

	// Fee should be clamped to minimum of 1
	expectedFee := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(1)))
	if !feeTx.GetFee().Equal(expectedFee) {
		t.Errorf("fee = %v, want minimum fee %v", feeTx.GetFee(), expectedFee)
	}
}

func TestSigner_SignTx_InvalidAccount(t *testing.T) {
	s := newTestSigner(t)

	// Create an Any with a wrong type URL
	wrongAny := &codectypes.Any{
		TypeUrl: "/cosmos.bank.v1beta1.MsgSend",
		Value:   []byte("garbage"),
	}

	_, err := s.SignTx(context.Background(), newTestMsg(s.address), wrongAny)
	if err == nil {
		t.Error("SignTx() with invalid account Any should return error")
	}
}
