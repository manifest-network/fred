package chain

import (
	"context"
	"fmt"

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

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// Signer handles transaction signing using a Cosmos keyring.
type Signer struct {
	keyring   keyring.Keyring
	keyName   string
	address   string
	chainID   string
	txConfig  client.TxConfig
	cdc       codec.Codec
}

// NewSigner creates a new signer using the specified keyring backend.
func NewSigner(backend, keyringDir, keyName, chainID string) (*Signer, error) {
	// Create codec with required types
	interfaceRegistry := codectypes.NewInterfaceRegistry()

	// Register crypto types for keyring deserialization
	cryptocodec.RegisterInterfaces(interfaceRegistry)
	authtypes.RegisterInterfaces(interfaceRegistry)
	billingtypes.RegisterInterfaces(interfaceRegistry)
	cdc := codec.NewProtoCodec(interfaceRegistry)

	// Create keyring
	kr, err := keyring.New("manifest", backend, keyringDir, nil, cdc)
	if err != nil {
		return nil, fmt.Errorf("failed to create keyring: %w", err)
	}

	// Get key info
	keyInfo, err := kr.Key(keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", keyName, err)
	}

	addr, err := keyInfo.GetAddress()
	if err != nil {
		return nil, fmt.Errorf("failed to get address: %w", err)
	}

	// Create tx config
	txConfig := authtx.NewTxConfig(cdc, authtx.DefaultSignModes)

	return &Signer{
		keyring:  kr,
		keyName:  keyName,
		address:  addr.String(),
		chainID:  chainID,
		txConfig: txConfig,
		cdc:      cdc,
	}, nil
}

// Address returns the signer's address.
func (s *Signer) Address() string {
	return s.address
}

// SignTx builds, signs, and encodes a transaction.
func (s *Signer) SignTx(ctx context.Context, msg sdk.Msg, accountAny *codectypes.Any) ([]byte, error) {
	// Unpack account
	var account authtypes.AccountI
	if err := s.cdc.UnpackAny(accountAny, &account); err != nil {
		return nil, fmt.Errorf("failed to unpack account: %w", err)
	}

	// Build the transaction
	txBuilder := s.txConfig.NewTxBuilder()
	if err := txBuilder.SetMsgs(msg); err != nil {
		return nil, fmt.Errorf("failed to set messages: %w", err)
	}

	// Set gas and fees (500k gas for batch operations like withdraw)
	txBuilder.SetGasLimit(500000)
	txBuilder.SetFeeAmount(sdk.NewCoins(sdk.NewCoin("umfx", math.NewInt(12500))))

	// Get the key
	keyInfo, err := s.keyring.Key(s.keyName)
	if err != nil {
		return nil, fmt.Errorf("failed to get key: %w", err)
	}

	pubKey, err := keyInfo.GetPubKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get public key: %w", err)
	}

	// Set signature with empty signature bytes first (for sign mode direct)
	sigV2 := signing.SignatureV2{
		PubKey: pubKey,
		Data: &signing.SingleSignatureData{
			SignMode:  signing.SignMode_SIGN_MODE_DIRECT,
			Signature: nil,
		},
		Sequence: account.GetSequence(),
	}
	if err := txBuilder.SetSignatures(sigV2); err != nil {
		return nil, fmt.Errorf("failed to set signatures: %w", err)
	}

	// Sign the transaction
	signerData := authsigning.SignerData{
		ChainID:       s.chainID,
		AccountNumber: account.GetAccountNumber(),
		Sequence:      account.GetSequence(),
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
		account.GetSequence(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}

	if err := txBuilder.SetSignatures(sigV2); err != nil {
		return nil, fmt.Errorf("failed to set final signature: %w", err)
	}

	// Encode the transaction
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
