package chain

import (
	"testing"

	"cosmossdk.io/math"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptocodec "github.com/cosmos/cosmos-sdk/crypto/codec"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestPassphraseReader_RepeatedReads(t *testing.T) {
	r := newPassphraseReader("mypassword")
	expected := []byte("mypassword\n")

	for i := range 5 {
		buf := make([]byte, 64)
		n, err := r.Read(buf)
		assert.NoError(t, err, "read %d should not error", i)
		assert.Equal(t, expected, buf[:n], "read %d should return full passphrase", i)
	}
}

func TestPassphraseReader_PartialBuffer(t *testing.T) {
	r := newPassphraseReader("longpassphrase")
	// Buffer smaller than passphrase+newline (15 bytes)
	buf := make([]byte, 5)
	n, err := r.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("longp"), buf)
}

func TestNewSignerPool_FileBackendRequiresPassphrase(t *testing.T) {
	_, err := NewSignerPool(SignerPoolConfig{
		SignerConfig: SignerConfig{
			KeyringBackend: "file",
			KeyringDir:     t.TempDir(),
			KeyName:        "testkey",
			ChainID:        "test-1",
			Passphrase:     "",
		},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "passphrase is required")
}

func TestNewSignerPool_PassphraseIgnoredForTestBackend(t *testing.T) {
	_, err := NewSignerPool(SignerPoolConfig{
		SignerConfig: SignerConfig{
			KeyringBackend: "test",
			KeyringDir:     t.TempDir(),
			KeyName:        "nonexistent",
			ChainID:        "test-1",
			Passphrase:     "somepassphrase",
		},
	})
	// Should fail on key lookup, not on keyring creation
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create primary signer")
}

func TestNewSignerPool_Success(t *testing.T) {
	s := newTestSigner(t)
	assert.NotEmpty(t, s.address)
}

func TestNewSignerPool_KeyNotFound(t *testing.T) {
	_, err := NewSignerPool(SignerPoolConfig{
		SignerConfig: SignerConfig{
			KeyringBackend: "test",
			KeyringDir:     t.TempDir(),
			KeyName:        "nonexistent",
			ChainID:        "test-1",
		},
	})
	assert.Error(t, err)
}

func TestNewSignerPool_InvalidBackend(t *testing.T) {
	_, err := NewSignerPool(SignerPoolConfig{
		SignerConfig: SignerConfig{
			KeyringBackend: "invalid-backend",
			KeyringDir:     t.TempDir(),
			KeyName:        "testkey",
			ChainID:        "test-1",
		},
	})
	assert.Error(t, err)
}

func TestSigner_Address(t *testing.T) {
	s := newTestSigner(t)

	addr := s.Address()
	require.NotEmpty(t, addr)

	// The address should start with the default cosmos prefix or manifest prefix
	// depending on SDK config. Just verify it's non-empty and parseable.
	_, err := sdk.AccAddressFromBech32(addr)
	assert.NoError(t, err)
}

func TestSigner_SignTx(t *testing.T) {
	s := newTestSigner(t)

	addr, err := sdk.AccAddressFromBech32(s.address)
	require.NoError(t, err)

	accountAny := newTestAccountAny(t, addr, 42, 7)

	txBytes, err := s.SignTx(t.Context(), newTestMsg(s.address), accountAny)
	require.NoError(t, err)
	require.NotEmpty(t, txBytes)

	// Decode and verify the transaction
	tx, err := s.txConfig.TxDecoder()(txBytes)
	require.NoError(t, err)

	feeTx, ok := tx.(sdk.FeeTx)
	require.True(t, ok)

	// gasLimit=200000, gasPrice=100 -> 200000*100/1_000_000 = 20
	expectedFee := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(20)))
	assert.True(t, feeTx.GetFee().Equal(expectedFee))
}

func TestSigner_SignTx_MinimumFee(t *testing.T) {
	s := newTestSigner(t)
	// Set gasPrice so that gasLimit*gasPrice/1e6 < 1
	s.gasLimit = 1
	s.gasPrice = 1

	addr, err := sdk.AccAddressFromBech32(s.address)
	require.NoError(t, err)

	accountAny := newTestAccountAny(t, addr, 0, 0)

	txBytes, err := s.SignTx(t.Context(), newTestMsg(s.address), accountAny)
	require.NoError(t, err)

	tx, err := s.txConfig.TxDecoder()(txBytes)
	require.NoError(t, err)

	feeTx, ok := tx.(sdk.FeeTx)
	require.True(t, ok)

	// Fee should be clamped to minimum of 1
	expectedFee := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(1)))
	assert.True(t, feeTx.GetFee().Equal(expectedFee))
}

func TestSigner_SignTxMulti_MultipleMessages(t *testing.T) {
	s := newTestSigner(t)

	addr, err := sdk.AccAddressFromBech32(s.address)
	require.NoError(t, err)

	accountAny := newTestAccountAny(t, addr, 1, 0)

	msgs := []sdk.Msg{
		newTestMsg(s.address),
		newTestMsg(s.address),
		newTestMsg(s.address),
	}

	txBytes, err := s.SignTxMulti(t.Context(), msgs, accountAny)
	require.NoError(t, err)
	require.NotEmpty(t, txBytes)

	// Decode and verify the transaction contains all 3 messages
	tx, err := s.txConfig.TxDecoder()(txBytes)
	require.NoError(t, err)

	txWithMsgs, ok := tx.(sdk.HasMsgs)
	require.True(t, ok)
	assert.Len(t, txWithMsgs.GetMsgs(), 3)
}

func TestSigner_SignTx_InvalidAccount(t *testing.T) {
	s := newTestSigner(t)

	// Create an Any with a wrong type URL
	wrongAny := &codectypes.Any{
		TypeUrl: "/cosmos.bank.v1beta1.MsgSend",
		Value:   []byte("garbage"),
	}

	_, err := s.SignTx(t.Context(), newTestMsg(s.address), wrongAny)
	assert.Error(t, err)
}
