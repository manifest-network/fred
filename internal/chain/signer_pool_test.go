package chain

import (
	"sync"
	"testing"

	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptocodec "github.com/cosmos/cosmos-sdk/crypto/codec"
	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtx "github.com/cosmos/cosmos-sdk/x/auth/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// newTestCodec creates a codec for signer pool tests (subset of production types).
func newTestCodec() codec.Codec {
	interfaceRegistry := codectypes.NewInterfaceRegistry()
	cryptocodec.RegisterInterfaces(interfaceRegistry)
	authtypes.RegisterInterfaces(interfaceRegistry)
	billingtypes.RegisterInterfaces(interfaceRegistry)
	return codec.NewProtoCodec(interfaceRegistry)
}

// newTestKeyringWithPrimary creates a "test" keyring with a primary key
// and returns the keyring and mnemonic for sub-key derivation.
func newTestKeyringWithPrimary(t *testing.T, dir string) (keyring.Keyring, string) {
	t.Helper()
	cdc := newTestCodec()
	kr, err := keyring.New("manifest", "test", dir, nil, cdc)
	require.NoError(t, err)
	supported, _ := kr.SupportedAlgorithms()
	_, mnemonic, err := kr.NewMnemonic("testkey", keyring.English, sdk.FullFundraiserPath, keyring.DefaultBIP39Passphrase, supported[0])
	require.NoError(t, err)
	return kr, mnemonic
}

// newTestSignerPool creates a SignerPool with N sub-signers using an in-memory keyring.
// The mnemonic is generated from the primary key; sub-keys are derived from it.
func newTestSignerPool(t *testing.T, subCount int) *SignerPool {
	t.Helper()

	cdc := newTestCodec()
	txConfig := authtx.NewTxConfig(cdc, authtx.DefaultSignModes)

	kr := keyring.NewInMemory(cdc)

	// Create primary key and capture mnemonic
	supported, _ := kr.SupportedAlgorithms()
	_, mnemonic, err := kr.NewMnemonic("testkey", keyring.English, sdk.FullFundraiserPath, keyring.DefaultBIP39Passphrase, supported[0])
	require.NoError(t, err)

	// Derive sub-keys
	for i := 1; i <= subCount; i++ {
		name := subKeyName("testkey", i)
		hdPath := hd.CreateHDPath(cosmosHDCoinType, uint32(i), 0).String()
		_, err := kr.NewAccount(name, mnemonic, keyring.DefaultBIP39Passphrase, hdPath, supported[0])
		require.NoError(t, err)
	}

	lkr := &lockedKeyring{kr: kr}

	cfg := SignerConfig{
		ChainID:  "test-1",
		GasLimit: 200000,
		GasPrice: 100,
		FeeDenom: "umfx",
	}

	primary, err := newSignerFromKeyring(lkr, "testkey", cfg, cdc, txConfig)
	require.NoError(t, err)

	pool := &SignerPool{primary: primary}
	for i := 1; i <= subCount; i++ {
		name := subKeyName("testkey", i)
		sub, err := newSignerFromKeyring(lkr, name, cfg, cdc, txConfig)
		require.NoError(t, err)
		pool.subSigners = append(pool.subSigners, sub)
	}
	return pool
}

func TestSignerPool_Acquire_RoundRobin(t *testing.T) {
	pool := newTestSignerPool(t, 3)

	// 9 calls should cycle sub1→sub2→sub3 three times
	seen := make([]string, 9)
	for i := range 9 {
		signer, isSub := pool.Acquire()
		assert.True(t, isSub, "Acquire should return sub-signer")
		seen[i] = signer.Address()
	}

	// Verify round-robin: indices 0,3,6 same; 1,4,7 same; 2,5,8 same
	assert.Equal(t, seen[0], seen[3])
	assert.Equal(t, seen[0], seen[6])
	assert.Equal(t, seen[1], seen[4])
	assert.Equal(t, seen[1], seen[7])
	assert.Equal(t, seen[2], seen[5])
	assert.Equal(t, seen[2], seen[8])

	// All three sub-signers should be different
	assert.NotEqual(t, seen[0], seen[1])
	assert.NotEqual(t, seen[1], seen[2])

	// Primary should never appear
	for _, addr := range seen {
		assert.NotEqual(t, pool.ProviderAddress(), addr)
	}
}

func TestSignerPool_Acquire_SingleSigner_ReturnsPrimary(t *testing.T) {
	pool := newTestSignerPool(t, 0)

	for range 5 {
		signer, isSub := pool.Acquire()
		assert.False(t, isSub)
		assert.Equal(t, pool.ProviderAddress(), signer.Address())
	}
}

func TestSignerPool_Acquire_ConcurrentSafety(t *testing.T) {
	pool := newTestSignerPool(t, 3)

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			signer, isSub := pool.Acquire()
			assert.True(t, isSub)
			assert.NotEmpty(t, signer.Address())
		}()
	}
	wg.Wait()
}

func TestSignerPool_Primary_AlwaysReturnsPrimary(t *testing.T) {
	pool := newTestSignerPool(t, 3)

	// Acquire some sub-signers to advance the counter
	for range 5 {
		pool.Acquire()
	}

	// Primary should be unaffected
	assert.Equal(t, pool.ProviderAddress(), pool.Primary().Address())
}

func TestSignerPool_ProviderAddress_Invariant(t *testing.T) {
	pool := newTestSignerPool(t, 3)
	expected := pool.primary.Address()

	// ProviderAddress must always return primary's address
	assert.Equal(t, expected, pool.ProviderAddress())

	// Even after acquiring sub-signers
	for range 10 {
		pool.Acquire()
	}
	assert.Equal(t, expected, pool.ProviderAddress())
}

func TestSignerPool_LaneCount(t *testing.T) {
	t.Run("no sub-signers", func(t *testing.T) {
		pool := newTestSignerPool(t, 0)
		assert.Equal(t, 1, pool.LaneCount())
	})

	t.Run("3 sub-signers", func(t *testing.T) {
		pool := newTestSignerPool(t, 3)
		assert.Equal(t, 3, pool.LaneCount())
	})
}

func TestSignerPool_Size(t *testing.T) {
	t.Run("no sub-signers", func(t *testing.T) {
		pool := newTestSignerPool(t, 0)
		assert.Equal(t, 1, pool.Size())
	})

	t.Run("3 sub-signers", func(t *testing.T) {
		pool := newTestSignerPool(t, 3)
		assert.Equal(t, 4, pool.Size()) // 1 primary + 3 sub
	})
}

func TestSignerPool_SubSignerAddresses(t *testing.T) {
	pool := newTestSignerPool(t, 3)

	addrs := pool.SubSignerAddresses()
	assert.Len(t, addrs, 3)

	// All addresses should be unique and non-empty
	seen := make(map[string]bool)
	for _, addr := range addrs {
		assert.NotEmpty(t, addr)
		assert.False(t, seen[addr], "duplicate sub-signer address: %s", addr)
		seen[addr] = true
		// Should not be the primary address
		assert.NotEqual(t, pool.ProviderAddress(), addr)
	}
}

func TestSignerPool_DemoteToSingleSigner(t *testing.T) {
	pool := newTestSignerPool(t, 3)
	require.Equal(t, 3, pool.LaneCount())

	pool.DemoteToSingleSigner()

	assert.Equal(t, 1, pool.LaneCount())
	assert.Equal(t, 1, pool.Size())
	assert.Empty(t, pool.SubSignerAddresses())

	// Acquire should now return primary
	signer, isSub := pool.Acquire()
	assert.False(t, isSub)
	assert.Equal(t, pool.ProviderAddress(), signer.Address())
}

func TestSignerPool_DemoteToSingleSigner_ConcurrentRead(t *testing.T) {
	pool := newTestSignerPool(t, 3)

	var wg sync.WaitGroup

	// Concurrent Acquire calls
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			signer, _ := pool.Acquire()
			assert.NotEmpty(t, signer.Address())
		}()
	}

	// Concurrent demotion
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.DemoteToSingleSigner()
	}()

	wg.Wait()

	// After demotion, should be single signer
	signer, isSub := pool.Acquire()
	assert.False(t, isSub)
	assert.Equal(t, pool.ProviderAddress(), signer.Address())
}

func TestNewSignerPool_KeyDerivation_FirstBoot(t *testing.T) {
	dir := t.TempDir()
	_, mnemonic := newTestKeyringWithPrimary(t, dir)

	// Create pool with mnemonic — should derive sub-keys
	pool, err := NewSignerPool(SignerPoolConfig{
		SignerConfig: SignerConfig{
			KeyringBackend: "test",
			KeyringDir:     dir,
			KeyName:        "testkey",
			ChainID:        "test-1",
		},
		SubSignerCount: 3,
		Mnemonic:       mnemonic,
	})
	require.NoError(t, err)
	assert.Equal(t, 3, pool.LaneCount())
	assert.Len(t, pool.SubSignerAddresses(), 3)
}

func TestNewSignerPool_KeyDerivation_SubsequentBoot(t *testing.T) {
	dir := t.TempDir()
	kr, mnemonic := newTestKeyringWithPrimary(t, dir)

	// First boot: create sub-keys with mnemonic
	supported, _ := kr.SupportedAlgorithms()
	for i := 1; i <= 3; i++ {
		name := subKeyName("testkey", i)
		hdPath := hd.CreateHDPath(cosmosHDCoinType, uint32(i), 0).String()
		_, err := kr.NewAccount(name, mnemonic, keyring.DefaultBIP39Passphrase, hdPath, supported[0])
		require.NoError(t, err)
	}

	// Second boot: no mnemonic — should load existing sub-keys
	pool, err := NewSignerPool(SignerPoolConfig{
		SignerConfig: SignerConfig{
			KeyringBackend: "test",
			KeyringDir:     dir,
			KeyName:        "testkey",
			ChainID:        "test-1",
		},
		SubSignerCount: 3,
		Mnemonic:       "", // no mnemonic
	})
	require.NoError(t, err)
	assert.Equal(t, 3, pool.LaneCount())
}

func TestNewSignerPool_PartialKeys_NoMnemonic(t *testing.T) {
	dir := t.TempDir()
	kr, mnemonic := newTestKeyringWithPrimary(t, dir)

	// Only create 2 of 3 sub-keys
	supported, _ := kr.SupportedAlgorithms()
	for i := 1; i <= 2; i++ {
		name := subKeyName("testkey", i)
		hdPath := hd.CreateHDPath(cosmosHDCoinType, uint32(i), 0).String()
		_, err := kr.NewAccount(name, mnemonic, keyring.DefaultBIP39Passphrase, hdPath, supported[0])
		require.NoError(t, err)
	}

	pool, err := NewSignerPool(SignerPoolConfig{
		SignerConfig: SignerConfig{
			KeyringBackend: "test",
			KeyringDir:     dir,
			KeyName:        "testkey",
			ChainID:        "test-1",
		},
		SubSignerCount: 3,
		Mnemonic:       "", // no mnemonic — can't derive sub-3
	})
	require.NoError(t, err)
	assert.Equal(t, 2, pool.LaneCount()) // only 2 sub-keys found
}

func TestNewSignerPool_MoreKeysThanCount(t *testing.T) {
	dir := t.TempDir()
	kr, mnemonic := newTestKeyringWithPrimary(t, dir)

	// Create 5 sub-keys
	supported, _ := kr.SupportedAlgorithms()
	for i := 1; i <= 5; i++ {
		name := subKeyName("testkey", i)
		hdPath := hd.CreateHDPath(cosmosHDCoinType, uint32(i), 0).String()
		_, err := kr.NewAccount(name, mnemonic, keyring.DefaultBIP39Passphrase, hdPath, supported[0])
		require.NoError(t, err)
	}

	// Request only 3 — should load exactly 3 (capped at SubSignerCount)
	pool, err := NewSignerPool(SignerPoolConfig{
		SignerConfig: SignerConfig{
			KeyringBackend: "test",
			KeyringDir:     dir,
			KeyName:        "testkey",
			ChainID:        "test-1",
		},
		SubSignerCount: 3,
		Mnemonic:       "", // keys already exist
	})
	require.NoError(t, err)
	assert.Equal(t, 3, pool.LaneCount()) // capped at configured count
}

func TestNewSignerPool_SingleSigner_Default(t *testing.T) {
	pool, err := NewSignerPool(SignerPoolConfig{
		SignerConfig: SignerConfig{
			KeyringBackend: "test",
			KeyringDir:     t.TempDir(),
			KeyName:        "nonexistent",
			ChainID:        "test-1",
		},
		SubSignerCount: 0,
	})
	// Should fail on primary key lookup
	assert.Error(t, err)
	assert.Nil(t, pool)
}
