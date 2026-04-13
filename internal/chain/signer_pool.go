package chain

import (
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/tx/signing"
)

const cosmosHDCoinType = 118

// nopRelease is the release function returned when Acquire hands back the
// primary signer (no per-signer mutex to unlock).
var nopRelease = func() {}

// SignerPool manages a primary signer and optional sub-signers for parallel
// transaction broadcasting via CosmosSDK authz.
type SignerPool struct {
	primary    *Signer
	mu         sync.RWMutex // protects subSigners (for DemoteToSingleSigner)
	subSigners []*Signer
	signerMu   []sync.Mutex // per-signer mutex for exclusive access
	next       atomic.Uint64
}

// SignerPoolConfig extends SignerConfig with sub-signer options.
type SignerPoolConfig struct {
	SignerConfig
	SubSignerCount int    // 0 = single signer (default)
	Mnemonic       string // only used on first boot to derive sub-signer keys
}

// lockedKeyring wraps a keyring.Keyring with a mutex for thread-safe access.
// The 99designs/keyring file backend has a mutable password field with no
// internal synchronization — concurrent Sign calls from multiple lanes race.
type lockedKeyring struct {
	mu sync.Mutex
	kr keyring.Keyring
}

func (lk *lockedKeyring) Backend() string {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.Backend()
}

func (lk *lockedKeyring) List() ([]*keyring.Record, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.List()
}

func (lk *lockedKeyring) SupportedAlgorithms() (keyring.SigningAlgoList, keyring.SigningAlgoList) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.SupportedAlgorithms()
}

func (lk *lockedKeyring) Key(uid string) (*keyring.Record, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.Key(uid)
}

func (lk *lockedKeyring) KeyByAddress(address sdk.Address) (*keyring.Record, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.KeyByAddress(address)
}

func (lk *lockedKeyring) Delete(uid string) error {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.Delete(uid)
}

func (lk *lockedKeyring) DeleteByAddress(address sdk.Address) error {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.DeleteByAddress(address)
}

func (lk *lockedKeyring) Rename(from, to string) error {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.Rename(from, to)
}

func (lk *lockedKeyring) NewMnemonic(uid string, language keyring.Language, hdPath, bip39Passphrase string, algo keyring.SignatureAlgo) (*keyring.Record, string, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.NewMnemonic(uid, language, hdPath, bip39Passphrase, algo)
}

func (lk *lockedKeyring) NewAccount(uid, mnemonic, bip39Passphrase, hdPath string, algo keyring.SignatureAlgo) (*keyring.Record, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.NewAccount(uid, mnemonic, bip39Passphrase, hdPath, algo)
}

func (lk *lockedKeyring) SaveLedgerKey(uid string, algo keyring.SignatureAlgo, hrp string, coinType, account, index uint32) (*keyring.Record, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.SaveLedgerKey(uid, algo, hrp, coinType, account, index)
}

func (lk *lockedKeyring) SaveOfflineKey(uid string, pubkey cryptotypes.PubKey) (*keyring.Record, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.SaveOfflineKey(uid, pubkey)
}

func (lk *lockedKeyring) SaveMultisig(uid string, pubkey cryptotypes.PubKey) (*keyring.Record, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.SaveMultisig(uid, pubkey)
}

func (lk *lockedKeyring) Sign(uid string, msg []byte, signMode signing.SignMode) ([]byte, cryptotypes.PubKey, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.Sign(uid, msg, signMode)
}

func (lk *lockedKeyring) SignByAddress(address sdk.Address, msg []byte, signMode signing.SignMode) ([]byte, cryptotypes.PubKey, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.SignByAddress(address, msg, signMode)
}

func (lk *lockedKeyring) ImportPrivKey(uid, armor, passphrase string) error {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.ImportPrivKey(uid, armor, passphrase)
}

func (lk *lockedKeyring) ImportPrivKeyHex(uid, privKey, algoStr string) error {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.ImportPrivKeyHex(uid, privKey, algoStr)
}

func (lk *lockedKeyring) ImportPubKey(uid, armor string) error {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.ImportPubKey(uid, armor)
}

func (lk *lockedKeyring) ExportPubKeyArmor(uid string) (string, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.ExportPubKeyArmor(uid)
}

func (lk *lockedKeyring) ExportPubKeyArmorByAddress(address sdk.Address) (string, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.ExportPubKeyArmorByAddress(address)
}

func (lk *lockedKeyring) ExportPrivKeyArmor(uid, encryptPassphrase string) (armor string, err error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.ExportPrivKeyArmor(uid, encryptPassphrase)
}

func (lk *lockedKeyring) ExportPrivKeyArmorByAddress(address sdk.Address, encryptPassphrase string) (armor string, err error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.ExportPrivKeyArmorByAddress(address, encryptPassphrase)
}

func (lk *lockedKeyring) MigrateAll() ([]*keyring.Record, error) {
	lk.mu.Lock()
	defer lk.mu.Unlock()
	return lk.kr.MigrateAll()
}

// verifyMnemonicMatchesPrimary derives account 0 from the mnemonic in a
// temporary in-memory keyring and checks that the resulting address matches
// the primary signer. This prevents deriving sub-keys from a wrong mnemonic.
func verifyMnemonicMatchesPrimary(mnemonic string, algo keyring.SignatureAlgo, primaryAddr string) error {
	cdc, _ := newCodecAndTxConfig()
	tmpKr := keyring.NewInMemory(cdc)
	rec, err := tmpKr.NewAccount("_verify", mnemonic, keyring.DefaultBIP39Passphrase, sdk.FullFundraiserPath, algo)
	if err != nil {
		return fmt.Errorf("failed to verify mnemonic: %w", err)
	}
	addr, err := rec.GetAddress()
	if err != nil {
		return fmt.Errorf("failed to get address from mnemonic: %w", err)
	}
	if addr.String() != primaryAddr {
		return fmt.Errorf("mnemonic does not match primary key: derived %s, expected %s", addr, primaryAddr)
	}
	return nil
}

// subKeyName returns the conventional name for a sub-signer key.
func subKeyName(primaryName string, index int) string {
	return fmt.Sprintf("%s-sub-%d", primaryName, index)
}

// NewSignerPool creates a signer pool with a primary signer and optional sub-signers.
// Sub-signer keys are derived from the mnemonic on first boot, and loaded from
// the keyring on subsequent boots.
func NewSignerPool(cfg SignerPoolConfig) (*SignerPool, error) {
	if cfg.KeyringBackend == "file" && cfg.Passphrase == "" {
		return nil, fmt.Errorf("keyring passphrase is required for file backend (set FRED_KEYRING_PASSPHRASE)")
	}

	cdc, txConfig := newCodecAndTxConfig()

	var userInput io.Reader
	if cfg.Passphrase != "" {
		userInput = newPassphraseReader(cfg.Passphrase)
	}
	kr, err := keyring.New("manifest", cfg.KeyringBackend, cfg.KeyringDir, userInput, cdc)
	if err != nil {
		return nil, fmt.Errorf("failed to create keyring: %w", err)
	}

	// Wrap for thread safety — concurrent signing from multiple lanes
	lkr := &lockedKeyring{kr: kr}

	primary, err := newSignerFromKeyring(lkr, cfg.KeyName, cfg.SignerConfig, cdc, txConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create primary signer: %w", err)
	}

	pool := &SignerPool{primary: primary}

	if cfg.SubSignerCount <= 0 {
		return pool, nil
	}

	// Derive missing sub-keys if mnemonic is available
	if cfg.Mnemonic != "" {
		supported, _ := lkr.SupportedAlgorithms()
		if len(supported) == 0 {
			return nil, fmt.Errorf("keyring reports no supported signing algorithms")
		}
		algo := supported[0]

		// Verify the mnemonic matches the primary key by deriving account 0
		// and comparing addresses. Prevents creating unrelated sub-keys that
		// can't be recovered from the same mnemonic.
		if err := verifyMnemonicMatchesPrimary(cfg.Mnemonic, algo, primary.Address()); err != nil {
			return nil, err
		}

		for i := 1; i <= cfg.SubSignerCount; i++ {
			name := subKeyName(cfg.KeyName, i)
			if _, err := lkr.Key(name); err == nil {
				continue // already exists
			}

			hdPath := hd.CreateHDPath(cosmosHDCoinType, uint32(i), 0).String()
			if _, err := lkr.NewAccount(name, cfg.Mnemonic, keyring.DefaultBIP39Passphrase, hdPath, algo); err != nil {
				slog.Warn("failed to derive sub-signer key", "name", name, "path", hdPath, "error", err)
				break
			}
			slog.Info("derived sub-signer key", "name", name, "path", hdPath)
		}
	}

	// Load sub-signer keys up to the configured count
	for i := 1; i <= cfg.SubSignerCount; i++ {
		name := subKeyName(cfg.KeyName, i)
		if _, err := lkr.Key(name); err != nil {
			slog.Warn("sub-signer key not found", "name", name)
			continue
		}

		sub, err := newSignerFromKeyring(lkr, name, cfg.SignerConfig, cdc, txConfig)
		if err != nil {
			slog.Warn("failed to create sub-signer", "name", name, "error", err)
			continue
		}
		pool.subSigners = append(pool.subSigners, sub)
	}

	if cfg.SubSignerCount > 0 && len(pool.subSigners) == 0 {
		slog.Warn("no sub-signer keys found, running in single-signer mode",
			"expected", cfg.SubSignerCount)
	} else if len(pool.subSigners) < cfg.SubSignerCount {
		slog.Warn("fewer sub-signer keys than requested",
			"found", len(pool.subSigners), "requested", cfg.SubSignerCount)
	}

	pool.signerMu = make([]sync.Mutex, len(pool.subSigners))

	return pool, nil
}

// Acquire returns the next available sub-signer with exclusive access.
// If no sub-signers exist, returns the primary signer (no locking needed).
// The caller MUST call the returned release function when done;
// failing to do so will deadlock the pool.
func (p *SignerPool) Acquire() (*Signer, bool, func()) {
	// Snapshot both slices under the same lock so they stay consistent
	// even if DemoteToSingleSigner runs concurrently.
	p.mu.RLock()
	subs := p.subSigners
	mus := p.signerMu
	p.mu.RUnlock()

	if len(subs) == 0 {
		return p.primary, false, nopRelease
	}

	n := uint64(len(subs))
	startIdx := p.next.Add(1) - 1

	// Try each signer starting from round-robin target, prefer unlocked.
	for i := uint64(0); i < n; i++ {
		idx := (startIdx + i) % n
		if mus[idx].TryLock() {
			return subs[idx], true, func() { mus[idx].Unlock() }
		}
	}

	// All signers busy — block on the round-robin target.
	idx := startIdx % n
	mus[idx].Lock()
	return subs[idx], true, func() { mus[idx].Unlock() }
}

// Primary returns the primary signer. Used for operations that must use the
// provider key (withdrawals, grants, funding).
func (p *SignerPool) Primary() *Signer {
	return p.primary
}

// ProviderAddress returns the primary signer's address.
func (p *SignerPool) ProviderAddress() string {
	return p.primary.Address()
}

// HasSubSigners returns true if the pool has any sub-signers.
func (p *SignerPool) HasSubSigners() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.subSigners) > 0
}

// Size returns the total number of signers (1 primary + N sub-signers).
func (p *SignerPool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return 1 + len(p.subSigners)
}

// LaneCount returns the number of parallel lanes for the batcher.
// With 0 sub-signers → 1 lane (primary). With N sub-signers → N lanes.
func (p *SignerPool) LaneCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.subSigners) == 0 {
		return 1
	}
	return len(p.subSigners)
}

// SubSignerAddresses returns the addresses of all sub-signers.
func (p *SignerPool) SubSignerAddresses() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	addrs := make([]string, len(p.subSigners))
	for i, s := range p.subSigners {
		addrs[i] = s.Address()
	}
	return addrs
}

// DemoteToSingleSigner removes all sub-signers, making the pool primary-only.
// Thread-safe, but must be called before the AckBatcher is constructed since
// it captures lane count at construction time.
func (p *SignerPool) DemoteToSingleSigner() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subSigners = nil
	p.signerMu = nil
}
