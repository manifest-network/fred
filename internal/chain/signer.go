package chain

import (
	"context"
	"fmt"
	"io"
	stdmath "math"
	"strconv"

	"cosmossdk.io/math"
	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptocodec "github.com/cosmos/cosmos-sdk/crypto/codec"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/tx/signing"
	authsigning "github.com/cosmos/cosmos-sdk/x/auth/signing"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	"github.com/cosmos/cosmos-sdk/x/authz"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

const (
	// gasPriceDivisor converts gas price from micro-units to base units.
	gasPriceDivisor = 1_000_000
	// minFeeAmount is the minimum fee amount in base units.
	minFeeAmount = 1
)

// Signer handles transaction signing using a Cosmos keyring.
type Signer struct {
	keyring       keyring.Keyring
	keyName       string
	address       string
	chainID       string
	txConfig      client.TxConfig
	cdc           codec.Codec
	gasLimit      uint64
	maxGasLimit   uint64         // 0 = no cap; if set, caps the gas limit during out-of-gas retries
	gasAdjustment math.LegacyDec // multiplier applied to gasLimit at sign time; zero/nil or <=1 disables
	gasPrice      int64
	feeDenom      string
}

// SignerConfig holds configuration for the transaction signer.
type SignerConfig struct {
	KeyringBackend string
	KeyringDir     string
	KeyName        string
	ChainID        string
	GasLimit       uint64
	MaxGasLimit    uint64  // 0 = no cap; if set, caps the gas limit during out-of-gas retries
	GasAdjustment  float64 // multiplier applied to GasLimit at sign time (Cosmos CLI convention); 0 or 1.0 = no adjustment
	GasPrice       int64
	FeeDenom       string
	Passphrase     string // Keyring passphrase for "file" backend (read from FRED_KEYRING_PASSPHRASE env)
}

// passphraseReader supplies the same passphrase on every Read call.
// The Cosmos SDK file keyring calls input.GetPassword via a bufio.NewReader
// wrapping this reader on each password prompt. On first boot, the password
// is read twice (enter + re-enter). Read must never return io.EOF — doing so
// would cause the second GetPassword to fail and exhaust prompt retries.
type passphraseReader struct {
	data []byte
	pos  int
}

func newPassphraseReader(passphrase string) io.Reader {
	return &passphraseReader{data: []byte(passphrase + "\n")}
}

func (r *passphraseReader) Read(p []byte) (int, error) {
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		r.pos = 0 // reset for the next prompt (first-boot reads twice)
	}
	return n, nil
}

// newCodecAndTxConfig creates the shared codec and tx config used by all signers.
func newCodecAndTxConfig() (codec.Codec, client.TxConfig) {
	interfaceRegistry := codectypes.NewInterfaceRegistry()
	cryptocodec.RegisterInterfaces(interfaceRegistry)
	authtypes.RegisterInterfaces(interfaceRegistry)
	billingtypes.RegisterInterfaces(interfaceRegistry)
	authz.RegisterInterfaces(interfaceRegistry)
	banktypes.RegisterInterfaces(interfaceRegistry)
	cdc := codec.NewProtoCodec(interfaceRegistry)
	txConfig := authtx.NewTxConfig(cdc, authtx.DefaultSignModes)
	return cdc, txConfig
}

// newSignerFromKeyring creates a Signer from a pre-opened keyring.
// The codec and txConfig are shared across all signers in a pool.
func newSignerFromKeyring(kr keyring.Keyring, keyName string, cfg SignerConfig, cdc codec.Codec, txConfig client.TxConfig) (*Signer, error) {
	keyInfo, err := kr.Key(keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", keyName, err)
	}

	addr, err := keyInfo.GetAddress()
	if err != nil {
		return nil, fmt.Errorf("failed to get address: %w", err)
	}

	// Convert GasAdjustment (float64) once at construction to math.LegacyDec so
	// the signing hot path uses deterministic big-int/decimal math and needs no
	// float→uint overflow guard. Config validation already bounds the value to
	// [1.0, 3.0] and rejects NaN/Inf, so FormatFloat + Parse cannot fail here.
	var gasAdjustmentDec math.LegacyDec
	if cfg.GasAdjustment > 0 {
		parsed, err := math.LegacyNewDecFromStr(strconv.FormatFloat(cfg.GasAdjustment, 'f', -1, 64))
		if err != nil {
			return nil, fmt.Errorf("parse gas_adjustment %f: %w", cfg.GasAdjustment, err)
		}
		gasAdjustmentDec = parsed
	}

	return &Signer{
		keyring:       kr,
		keyName:       keyName,
		address:       addr.String(),
		chainID:       cfg.ChainID,
		txConfig:      txConfig,
		cdc:           cdc,
		gasLimit:      cfg.GasLimit,
		maxGasLimit:   cfg.MaxGasLimit,
		gasAdjustment: gasAdjustmentDec,
		gasPrice:      cfg.GasPrice,
		feeDenom:      cfg.FeeDenom,
	}, nil
}

// Address returns the signer's address.
func (s *Signer) Address() string {
	return s.address
}

// SignTx builds, signs, and encodes a single-message transaction.
func (s *Signer) SignTx(ctx context.Context, msg sdk.Msg, accountAny *codectypes.Any) ([]byte, error) {
	return s.SignTxMulti(ctx, []sdk.Msg{msg}, accountAny)
}

// SignTxMulti builds, signs, and encodes a transaction with one or more messages.
func (s *Signer) SignTxMulti(ctx context.Context, msgs []sdk.Msg, accountAny *codectypes.Any) ([]byte, error) {
	return s.signTxInternal(ctx, msgs, accountAny, nil, nil)
}

// signTxInternal is the shared implementation for signing transactions.
// If seqOverride is non-nil, its value is used instead of the account's on-chain
// sequence. A pointer is used because sequence 0 is a valid Cosmos SDK value
// (first transaction from a new account).
// If gasLimitOverride is non-nil, its value is used instead of the configured
// gas limit, and the fee is recalculated accordingly.
func (s *Signer) signTxInternal(ctx context.Context, msgs []sdk.Msg, accountAny *codectypes.Any, seqOverride *uint64, gasLimitOverride *uint64) ([]byte, error) {
	var account authtypes.AccountI
	if err := s.cdc.UnpackAny(accountAny, &account); err != nil {
		return nil, fmt.Errorf("failed to unpack account: %w", err)
	}

	seq := account.GetSequence()
	if seqOverride != nil {
		seq = *seqOverride
	}

	txBuilder := s.txConfig.NewTxBuilder()
	if err := txBuilder.SetMsgs(msgs...); err != nil {
		return nil, fmt.Errorf("failed to set messages: %w", err)
	}

	gasLimit := s.gasLimit
	if gasLimitOverride != nil {
		gasLimit = *gasLimitOverride
	} else if !s.gasAdjustment.IsNil() && s.gasAdjustment.GT(math.LegacyOneDec()) {
		// Apply multiplier only on the normal path. OOG retries set
		// gasLimitOverride to a bumped value; we must not compound the
		// multiplier on top or we'd double-adjust. maxGasLimit cap is still
		// enforced so operators retain an upper bound.
		// Big-int/decimal arithmetic via cosmossdk.io/math avoids the
		// platform-dependent float→uint64 conversion semantics of a naive
		// implementation (amd64 returns MinInt64, arm64 saturates at MaxUint64).
		adjustedInt := s.gasAdjustment.MulInt(math.NewIntFromUint64(gasLimit)).TruncateInt()
		if !adjustedInt.IsUint64() {
			return nil, fmt.Errorf("gas adjustment overflow: gasLimit=%d * %s = %s exceeds uint64", gasLimit, s.gasAdjustment, adjustedInt)
		}
		adjusted := adjustedInt.Uint64()
		if s.maxGasLimit > 0 && adjusted > s.maxGasLimit {
			adjusted = s.maxGasLimit
		}
		gasLimit = adjusted
	}
	if gasLimit > uint64(stdmath.MaxInt64) {
		return nil, fmt.Errorf("gas limit %d overflows int64", gasLimit)
	}
	txBuilder.SetGasLimit(gasLimit)
	// Use math.Int to avoid int64 overflow for large gasLimit * gasPrice products.
	// Ceiling division: the chain validates fee against ceil(gasLimit * gasPrice / 1e6);
	// integer floor would under-pay by one base unit of fee_denom whenever the product
	// isn't an exact multiple of gasPriceDivisor.
	divisor := math.NewInt(gasPriceDivisor)
	feeInt := math.NewInt(int64(gasLimit)).Mul(math.NewInt(s.gasPrice)).Add(divisor).Sub(math.OneInt()).Quo(divisor)
	if feeInt.LT(math.NewInt(minFeeAmount)) {
		feeInt = math.NewInt(minFeeAmount)
	}
	txBuilder.SetFeeAmount(sdk.NewCoins(sdk.NewCoin(s.feeDenom, feeInt)))

	keyInfo, err := s.keyring.Key(s.keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	pubKey, err := keyInfo.GetPubKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get public key: %w", err)
	}

	sigV2 := signing.SignatureV2{
		PubKey: pubKey,
		Data: &signing.SingleSignatureData{
			SignMode:  signing.SignMode_SIGN_MODE_DIRECT,
			Signature: nil,
		},
		Sequence: seq,
	}
	if err := txBuilder.SetSignatures(sigV2); err != nil {
		return nil, fmt.Errorf("failed to set signatures: %w", err)
	}

	signerData := authsigning.SignerData{
		ChainID:       s.chainID,
		AccountNumber: account.GetAccountNumber(),
		Sequence:      seq,
		PubKey:        pubKey,
		Address:       s.address,
	}

	sigV2, err = SignWithPrivKey(
		ctx,
		signing.SignMode_SIGN_MODE_DIRECT,
		signerData,
		txBuilder,
		s.keyring,
		s.keyName,
		s.txConfig,
		seq,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}

	if err := txBuilder.SetSignatures(sigV2); err != nil {
		return nil, fmt.Errorf("failed to set final signature: %w", err)
	}

	txBytes, err := s.txConfig.TxEncoder()(txBuilder.GetTx())
	if err != nil {
		return nil, fmt.Errorf("failed to encode transaction: %w", err)
	}

	return txBytes, nil
}

// SignWithPrivKey signs using the keyring.
func SignWithPrivKey(
	ctx context.Context,
	signMode signing.SignMode,
	signerData authsigning.SignerData,
	txBuilder client.TxBuilder,
	kr keyring.Keyring,
	keyName string,
	txConfig client.TxConfig,
	sequence uint64,
) (signing.SignatureV2, error) {
	var sigV2 signing.SignatureV2

	// Generate the bytes to be signed
	signBytes, err := authsigning.GetSignBytesAdapter(
		ctx,
		txConfig.SignModeHandler(),
		signMode,
		signerData,
		txBuilder.GetTx(),
	)
	if err != nil {
		return sigV2, err
	}

	// Sign
	sig, _, err := kr.Sign(keyName, signBytes, signMode)
	if err != nil {
		return sigV2, err
	}

	keyInfo, err := kr.Key(keyName)
	if err != nil {
		return sigV2, err
	}

	pubKey, err := keyInfo.GetPubKey()
	if err != nil {
		return sigV2, err
	}

	sigV2 = signing.SignatureV2{
		PubKey: pubKey,
		Data: &signing.SingleSignatureData{
			SignMode:  signMode,
			Signature: sig,
		},
		Sequence: sequence,
	}

	return sigV2, nil
}
