package chain

import (
	"testing"

	"cosmossdk.io/math"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptocodec "github.com/cosmos/cosmos-sdk/crypto/codec"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authsigning "github.com/cosmos/cosmos-sdk/x/auth/signing"
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
	dir := t.TempDir()
	newTestKeyringWithPrimary(t, dir)

	pool, err := NewSignerPool(SignerPoolConfig{
		SignerConfig: SignerConfig{
			KeyringBackend: "test",
			KeyringDir:     dir,
			KeyName:        "testkey",
			ChainID:        "test-1",
			GasLimit:       200000,
			GasPrice:       100,
			FeeDenom:       "umfx",
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, pool.ProviderAddress())
	assert.Equal(t, 1, pool.Size())
	assert.Equal(t, 1, pool.LaneCount())
	assert.False(t, pool.HasSubSigners())
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

func TestSigner_SignTx_LargeGasFeeNoOverflow(t *testing.T) {
	// Verify that large gasLimit * gasPrice products don't overflow int64.
	// 5e18 * 2 = 1e19 which exceeds MaxInt64 (~9.2e18) but must still
	// produce a correct fee via math.Int arithmetic.
	s := newTestSigner(t)
	s.gasLimit = 5_000_000_000_000_000_000 // 5e18
	s.gasPrice = 2

	addr, err := sdk.AccAddressFromBech32(s.address)
	require.NoError(t, err)

	accountAny := newTestAccountAny(t, addr, 0, 0)

	txBytes, err := s.SignTx(t.Context(), newTestMsg(s.address), accountAny)
	require.NoError(t, err)

	tx, err := s.txConfig.TxDecoder()(txBytes)
	require.NoError(t, err)

	feeTx, ok := tx.(sdk.FeeTx)
	require.True(t, ok)

	// 5e18 * 2 / 1e6 = 10_000_000_000_000 (10e12) — exact multiple, ceiling == floor.
	expectedFee := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(10_000_000_000_000)))
	assert.True(t, feeTx.GetFee().Equal(expectedFee),
		"fee must be correct even when gasLimit*gasPrice overflows int64; got %s, want %s",
		feeTx.GetFee(), expectedFee)
}

// signTxAndDecodeFee signs a tx with the signer's current config and returns
// the computed fee coin amount for the signer's fee denom.
func signTxAndDecodeFee(t *testing.T, s *Signer) sdk.Coins {
	t.Helper()
	addr, err := sdk.AccAddressFromBech32(s.address)
	require.NoError(t, err)
	accountAny := newTestAccountAny(t, addr, 0, 0)
	txBytes, err := s.SignTx(t.Context(), newTestMsg(s.address), accountAny)
	require.NoError(t, err)
	tx, err := s.txConfig.TxDecoder()(txBytes)
	require.NoError(t, err)
	feeTx, ok := tx.(sdk.FeeTx)
	require.True(t, ok)
	return feeTx.GetFee()
}

func TestSigner_SignTx_CeilingFee_NonDivisible(t *testing.T) {
	// Reproduces the prod failure: gasLimit=337479, gasPrice=25
	// → 337479 * 25 = 8_436_975, / 1e6 = 8.436975
	// Floor (old behaviour) = 8; chain requires ceiling = 9.
	s := newTestSigner(t)
	s.gasLimit = 337479
	s.gasPrice = 25

	fee := signTxAndDecodeFee(t, s)
	expected := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(9)))
	assert.True(t, fee.Equal(expected),
		"ceiling fee must be 9 for non-divisible product; got %s", fee)
}

func TestSigner_SignTx_CeilingFee_ExactMultiple(t *testing.T) {
	// gasLimit=200000, gasPrice=100 → 20_000_000 / 1e6 = 20 exactly.
	// Ceiling must equal floor on exact multiples.
	s := newTestSigner(t)
	s.gasLimit = 200000
	s.gasPrice = 100

	fee := signTxAndDecodeFee(t, s)
	expected := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(20)))
	assert.True(t, fee.Equal(expected),
		"ceiling must equal floor on exact multiple; got %s", fee)
}

func TestSigner_SignTx_CeilingFee_OneUnitOver(t *testing.T) {
	// gasLimit=1000001, gasPrice=1 → 1_000_001 / 1e6 = 1.000001
	// Ceiling bumps to 2.
	s := newTestSigner(t)
	s.gasLimit = 1000001
	s.gasPrice = 1

	fee := signTxAndDecodeFee(t, s)
	expected := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(2)))
	assert.True(t, fee.Equal(expected),
		"ceiling must bump to 2 when product exceeds divisor by 1; got %s", fee)
}

func TestSigner_SignTx_CeilingFee_UnderDivisorHitsMin(t *testing.T) {
	// gasLimit=10, gasPrice=1 → 10 / 1e6 = 0.00001
	// Ceiling = 1; minFeeAmount clamp is also 1. Confirms the two paths compose.
	s := newTestSigner(t)
	s.gasLimit = 10
	s.gasPrice = 1

	fee := signTxAndDecodeFee(t, s)
	expected := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(1)))
	assert.True(t, fee.Equal(expected),
		"fee below minFeeAmount must clamp to 1; got %s", fee)
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

func TestSigner_SignTxInternal_SequenceOverride(t *testing.T) {
	s := newTestSigner(t)

	addr, err := sdk.AccAddressFromBech32(s.address)
	require.NoError(t, err)

	// Account on-chain has sequence 5; subtests verify nil, non-nil, and zero overrides.
	accountAny := newTestAccountAny(t, addr, 1, 5)

	t.Run("nil override uses account sequence", func(t *testing.T) {
		txBytes, err := s.signTxInternal(t.Context(), []sdk.Msg{newTestMsg(s.address)}, accountAny, nil, nil)
		require.NoError(t, err)

		seq := extractTxSequence(t, s, txBytes)
		assert.Equal(t, uint64(5), seq)
	})

	t.Run("non-nil override replaces account sequence", func(t *testing.T) {
		override := uint64(42)
		txBytes, err := s.signTxInternal(t.Context(), []sdk.Msg{newTestMsg(s.address)}, accountAny, &override, nil)
		require.NoError(t, err)

		seq := extractTxSequence(t, s, txBytes)
		assert.Equal(t, uint64(42), seq)
	})

	t.Run("override with sequence 0", func(t *testing.T) {
		override := uint64(0)
		txBytes, err := s.signTxInternal(t.Context(), []sdk.Msg{newTestMsg(s.address)}, accountAny, &override, nil)
		require.NoError(t, err)

		seq := extractTxSequence(t, s, txBytes)
		assert.Equal(t, uint64(0), seq, "sequence 0 is valid and should not be treated as 'no override'")
	})
}

func TestSigner_SignTxInternal_GasLimitOverride(t *testing.T) {
	s := newTestSigner(t)

	addr, err := sdk.AccAddressFromBech32(s.address)
	require.NoError(t, err)

	accountAny := newTestAccountAny(t, addr, 1, 0)

	t.Run("nil override uses default gas limit", func(t *testing.T) {
		txBytes, err := s.signTxInternal(t.Context(), []sdk.Msg{newTestMsg(s.address)}, accountAny, nil, nil)
		require.NoError(t, err)

		decodedTx, err := s.txConfig.TxDecoder()(txBytes)
		require.NoError(t, err)
		feeTx, ok := decodedTx.(sdk.FeeTx)
		require.True(t, ok)
		assert.Equal(t, s.gasLimit, feeTx.GetGas())
	})

	t.Run("non-nil override replaces gas limit", func(t *testing.T) {
		override := uint64(500000)
		txBytes, err := s.signTxInternal(t.Context(), []sdk.Msg{newTestMsg(s.address)}, accountAny, nil, &override)
		require.NoError(t, err)

		decodedTx, err := s.txConfig.TxDecoder()(txBytes)
		require.NoError(t, err)
		feeTx, ok := decodedTx.(sdk.FeeTx)
		require.True(t, ok)
		assert.Equal(t, uint64(500000), feeTx.GetGas())

		// Fee must be recalculated: 500000 * 100 / 1_000_000 = 50
		expectedFee := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(50)))
		assert.True(t, feeTx.GetFee().Equal(expectedFee), "fee must be recalculated from overridden gas")
	})
}

func TestSigner_SignTx_GasAdjustment_Applied(t *testing.T) {
	// GasAdjustment=1.2 multiplies gasLimit at sign time. Both tx gas and fee
	// scale proportionally: 200000 * 1.2 = 240000 gas; 240000 * 100 / 1e6 = 24 umfx.
	s := newTestSigner(t)
	s.gasAdjustment = math.LegacyMustNewDecFromStr("1.2")

	addr, err := sdk.AccAddressFromBech32(s.address)
	require.NoError(t, err)
	accountAny := newTestAccountAny(t, addr, 0, 0)

	txBytes, err := s.SignTx(t.Context(), newTestMsg(s.address), accountAny)
	require.NoError(t, err)
	tx, err := s.txConfig.TxDecoder()(txBytes)
	require.NoError(t, err)
	feeTx, ok := tx.(sdk.FeeTx)
	require.True(t, ok)

	assert.Equal(t, uint64(240000), feeTx.GetGas(), "gas must be multiplied by adjustment")
	expectedFee := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(24)))
	assert.True(t, feeTx.GetFee().Equal(expectedFee),
		"fee must scale with adjusted gas; got %s want %s", feeTx.GetFee(), expectedFee)
}

func TestSigner_SignTx_GasAdjustment_OverrideBypassesMultiplier(t *testing.T) {
	// CRITICAL OOG-path correctness: when gasLimitOverride is set (by the
	// OOG retry loop in client.go), the adjustment MUST NOT compound with
	// the override's own 1.5× bump. Override wins verbatim.
	s := newTestSigner(t)
	s.gasAdjustment = math.LegacyMustNewDecFromStr("1.5")

	addr, err := sdk.AccAddressFromBech32(s.address)
	require.NoError(t, err)
	accountAny := newTestAccountAny(t, addr, 0, 0)

	override := uint64(300000)
	txBytes, err := s.signTxInternal(t.Context(), []sdk.Msg{newTestMsg(s.address)}, accountAny, nil, &override)
	require.NoError(t, err)
	tx, err := s.txConfig.TxDecoder()(txBytes)
	require.NoError(t, err)
	feeTx, ok := tx.(sdk.FeeTx)
	require.True(t, ok)

	assert.Equal(t, uint64(300000), feeTx.GetGas(),
		"override must NOT be multiplied by gasAdjustment")
}

func TestSigner_SignTx_GasAdjustment_RespectsMaxCap(t *testing.T) {
	// With gasAdjustment raising gas past maxGasLimit, the adjusted value
	// must be capped. Picking gasLimit=200000, adjustment=2.5 → 500000 raw,
	// capped to maxGasLimit=300000.
	s := newTestSigner(t)
	s.gasAdjustment = math.LegacyMustNewDecFromStr("2.5")
	s.maxGasLimit = 300000

	addr, err := sdk.AccAddressFromBech32(s.address)
	require.NoError(t, err)
	accountAny := newTestAccountAny(t, addr, 0, 0)

	txBytes, err := s.SignTx(t.Context(), newTestMsg(s.address), accountAny)
	require.NoError(t, err)
	tx, err := s.txConfig.TxDecoder()(txBytes)
	require.NoError(t, err)
	feeTx, ok := tx.(sdk.FeeTx)
	require.True(t, ok)

	assert.Equal(t, uint64(300000), feeTx.GetGas(),
		"adjusted gas must be capped at maxGasLimit")
}

func TestSigner_SignTx_GasAdjustment_RespectsMinFee(t *testing.T) {
	// Even after adjustment the per-tx fee may round below 1; minFeeAmount
	// clamp still applies.
	s := newTestSigner(t)
	s.gasLimit = 1
	s.gasPrice = 1
	s.gasAdjustment = math.LegacyMustNewDecFromStr("1.2") // 1 * 1.2 → 1 (truncated to uint64), fee still hits clamp

	fee := signTxAndDecodeFee(t, s)
	expected := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(1)))
	assert.True(t, fee.Equal(expected), "fee must still clamp to minFeeAmount after adjustment; got %s", fee)
}

func TestSigner_SignTx_GasAdjustment_OverflowReturnsError(t *testing.T) {
	// Defensive: the big-int product of gasLimit * adjustment may exceed
	// uint64. Config validation (1.0 <= adjustment <= 3.0, gasLimit <= MaxInt64)
	// should prevent this, but the signer must still error cleanly when called
	// with out-of-range values rather than truncating silently.
	s := newTestSigner(t)
	// gasLimit * 2.0 where gasLimit is near the uint64 ceiling forces the
	// big-int product beyond uint64 range so IsUint64() returns false.
	s.gasLimit = 1 << 63 // 9.22e18
	s.gasAdjustment = math.LegacyMustNewDecFromStr("3.0")

	addr, err := sdk.AccAddressFromBech32(s.address)
	require.NoError(t, err)
	accountAny := newTestAccountAny(t, addr, 0, 0)

	_, err = s.SignTx(t.Context(), newTestMsg(s.address), accountAny)
	require.Error(t, err, "overflow must be detected and surface as an error")
	assert.Contains(t, err.Error(), "gas adjustment overflow",
		"error message must identify the overflow root cause")
}

func TestSigner_SignTx_GasAdjustment_ExactlyOneIsNoOp(t *testing.T) {
	// Boundary: gasAdjustment == 1.0 must skip the adjustment branch entirely
	// (the guard is `> 1.0`, not `>= 1.0`). Refactoring that guard to `>=`
	// would silently change fee arithmetic for every deployment using 1.0.
	s := newTestSigner(t)
	s.gasLimit = 200000
	s.gasPrice = 100
	s.gasAdjustment = math.LegacyOneDec()

	addr, err := sdk.AccAddressFromBech32(s.address)
	require.NoError(t, err)
	accountAny := newTestAccountAny(t, addr, 0, 0)

	txBytes, err := s.SignTx(t.Context(), newTestMsg(s.address), accountAny)
	require.NoError(t, err)
	tx, err := s.txConfig.TxDecoder()(txBytes)
	require.NoError(t, err)
	feeTx, ok := tx.(sdk.FeeTx)
	require.True(t, ok)

	assert.Equal(t, uint64(200000), feeTx.GetGas(),
		"gasAdjustment=1.0 must leave gasLimit unchanged")
	expectedFee := sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(20)))
	assert.True(t, feeTx.GetFee().Equal(expectedFee),
		"gasAdjustment=1.0 must produce the same fee as no adjustment")
}

// extractTxSequence decodes tx bytes, asserts exactly one signature, and returns its sequence.
func extractTxSequence(t *testing.T, s *Signer, txBytes []byte) uint64 {
	t.Helper()
	decodedTx, err := s.txConfig.TxDecoder()(txBytes)
	require.NoError(t, err)

	sigTx, ok := decodedTx.(authsigning.SigVerifiableTx)
	require.True(t, ok, "decoded tx does not implement authsigning.SigVerifiableTx")

	sigs, err := sigTx.GetSignaturesV2()
	require.NoError(t, err)
	require.Len(t, sigs, 1)
	return sigs[0].Sequence
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
