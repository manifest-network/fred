//go:build integration

package chain

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// BinaryEnv names the manifestd binary every chain runs. CI installs the
// release pinned in .manifestd-version.
const BinaryEnv = "MANIFESTD"

// logDirEnv, when set, receives the node's log so CI can upload it. The rest
// of the suite reads the same variable.
const logDirEnv = "FRED_E2E_LOG_DIR"

const (
	// validatorKey is the key `testnet init-files` creates for the one
	// validator.
	validatorKey = "node0"
	// commandTimeout bounds one manifestd CLI call.
	commandTimeout = 30 * time.Second
	// commitTimeout bounds the wait for a block or for a broadcast transaction
	// to be committed. Blocks come every 500ms.
	commitTimeout = 30 * time.Second
)

// Chain is one single-validator manifestd node in a temporary directory. This
// file uses only the standard library and the manifestd command line, so it
// depends on that command line and on its JSON output.
type Chain struct {
	// ID is the chain ID.
	ID string

	bin, home, rpc, grpc string
}

// Key is a key in the chain's test keyring.
type Key struct {
	Name     string
	Address  string
	Mnemonic string
}

// Start initializes a chain, lets prepare change it before its first block,
// starts the node on free ports and waits for that block. prepare runs before
// the node exists: it may add keys and genesis accounts and edit the genesis
// document, but not send transactions. Start skips the test when BinaryEnv is
// unset. Cleanup stops the node.
func Start(t *testing.T, prepare func(c *Chain)) *Chain {
	t.Helper()
	bin := os.Getenv(BinaryEnv)
	if bin == "" {
		t.Skipf("%s is not set; point it at the manifestd pinned in .manifestd-version", BinaryEnv)
	}
	dir := t.TempDir()
	c := &Chain{ID: "fred-e2e", bin: bin, rpc: freeAddr(t), grpc: freeAddr(t)}
	c.run(t, "testnet", "init-files", "--chain-id", c.ID, "--output-dir", dir, "--v", "1",
		"--keyring-backend", "test", "--commit-timeout", "500ms", "--minimum-gas-prices", "0umfx")
	c.home = filepath.Join(dir, validatorKey, "manifestd")
	prepare(c)

	logDir := dir
	if fromEnv := os.Getenv(logDirEnv); fromEnv != "" {
		logDir = fromEnv
	}
	_, port, _ := net.SplitHostPort(c.rpc)
	logPath := filepath.Join(logDir, "manifestd-"+port+".log")
	logFile, err := os.Create(logPath) //nolint:gosec // G304: a path under t.TempDir
	if err != nil {
		t.Fatalf("create node log: %v", err)
	}
	defer func() { _ = logFile.Close() }()
	node := exec.Command(bin, "start", "--home", c.home, //nolint:gosec // G204: the pinned chain binary and fixture-built arguments
		"--rpc.laddr", "tcp://"+c.rpc, "--grpc.address", c.grpc,
		"--p2p.laddr", "tcp://"+freeAddr(t), "--rpc.pprof_laddr", freeAddr(t),
		"--api.enable=false", "--minimum-gas-prices", "0umfx")
	node.Stdout, node.Stderr = logFile, logFile
	if err := node.Start(); err != nil {
		t.Fatalf("start manifestd: %v", err)
	}
	exited := make(chan struct{})
	go func() { _ = node.Wait(); close(exited) }()
	t.Cleanup(func() {
		_ = node.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			_ = node.Process.Kill()
			<-exited
		}
		if t.Failed() {
			t.Logf("manifestd log (%s):\n%s", logPath, tail(logPath))
		}
	})

	deadline := time.Now().Add(commitTimeout)
	for c.height() < 1 {
		select {
		case <-exited:
			t.Fatalf("manifestd exited during startup\n%s", tail(logPath))
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("manifestd produced no block within %s\n%s", commitTimeout, tail(logPath))
		}
		time.Sleep(200 * time.Millisecond)
	}
	return c
}

// AddGenesisAccount gives an address an account and a balance in the genesis
// document. Only valid inside Start's prepare.
func (c *Chain) AddGenesisAccount(t *testing.T, address, coins string) {
	t.Helper()
	c.run(t, "genesis", "add-genesis-account", address, coins, "--home", c.home, "--keyring-backend", "test")
}

// EditGenesis applies edit to the genesis document. Only valid inside Start's
// prepare.
func (c *Chain) EditGenesis(t *testing.T, edit func(genesis map[string]any)) {
	t.Helper()
	path := filepath.Join(c.home, "config", "genesis.json")
	raw, err := os.ReadFile(path) //nolint:gosec // G304: a path under t.TempDir
	if err != nil {
		t.Fatalf("read genesis: %v", err)
	}
	var genesis map[string]any
	if err := json.Unmarshal(raw, &genesis); err != nil {
		t.Fatalf("decode genesis: %v", err)
	}
	edit(genesis)
	if raw, err = json.Marshal(genesis); err != nil {
		t.Fatalf("encode genesis: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write genesis: %v", err)
	}
}

// Home is the node's home directory. Its keyring-test subdirectory is the
// keyring every key of this chain lives in.
func (c *Chain) Home() string { return c.home }

// GRPC is the node's gRPC address, as host:port.
func (c *Chain) GRPC() string { return c.grpc }

// WebSocketURL is the node's CometBFT event endpoint.
func (c *Chain) WebSocketURL() string { return "ws://" + c.rpc + "/websocket" }

// Validator is the address of the validator's key, which holds the genesis
// balances.
func (c *Chain) Validator(t *testing.T) string {
	t.Helper()
	return strings.TrimSpace(c.run(t, "keys", "show", validatorKey, "-a", "--home", c.home, "--keyring-backend", "test"))
}

// AddKey creates a key in the chain's keyring.
func (c *Chain) AddKey(t *testing.T, name string) Key {
	t.Helper()
	out := c.run(t, "keys", "add", name, "--output", "json", "--home", c.home, "--keyring-backend", "test")
	key := Key{Name: name}
	if err := json.Unmarshal([]byte(jsonObject(out)), &key); err != nil || key.Address == "" || key.Mnemonic == "" {
		t.Fatalf("keys add %s: unexpected output (%v):\n%s", name, err, out)
	}
	return key
}

// Tx signs and broadcasts a transaction with the named key and waits until it
// is committed. A transaction the chain rejects fails the test.
func (c *Chain) Tx(t *testing.T, from string, args ...string) {
	t.Helper()
	argv := append([]string{"tx"}, args...)
	argv = append(argv, "--from", from, "--home", c.home, "--keyring-backend", "test",
		"--chain-id", c.ID, "--node", "tcp://"+c.rpc, "--gas", "1000000",
		"--broadcast-mode", "sync", "--yes", "--output", "json")
	var sent struct {
		Code   int    `json:"code"`
		Hash   string `json:"txhash"`
		RawLog string `json:"raw_log"`
	}
	out := c.run(t, argv...)
	if err := json.Unmarshal([]byte(jsonObject(out)), &sent); err != nil || sent.Code != 0 || sent.Hash == "" {
		t.Fatalf("tx %v was not accepted (%v):\n%s", args, err, out)
	}
	// Broadcast returns before the block. The transaction's own result is
	// known only once a block holds it.
	deadline := time.Now().Add(commitTimeout)
	for {
		var committed struct {
			Code   int    `json:"code"`
			RawLog string `json:"raw_log"`
		}
		out, err := c.output("q", "tx", sent.Hash, "--node", "tcp://"+c.rpc, "--output", "json")
		if err == nil && json.Unmarshal([]byte(jsonObject(out)), &committed) == nil {
			if committed.Code != 0 {
				t.Fatalf("tx %v failed with code %d: %s", args, committed.Code, committed.RawLog)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("tx %v (%s) was not committed within %s: %v\n%s", args, sent.Hash, commitTimeout, err, out)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Query runs a query and decodes its JSON answer into out.
func (c *Chain) Query(t *testing.T, out any, args ...string) {
	t.Helper()
	argv := append([]string{"q"}, args...)
	raw := c.run(t, append(argv, "--node", "tcp://"+c.rpc, "--output", "json")...)
	if err := json.Unmarshal([]byte(jsonObject(raw)), out); err != nil {
		t.Fatalf("query %v: %v\n%s", args, err, raw)
	}
}

// height is the latest block height, or 0 while the node does not answer.
func (c *Chain) height() int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+c.rpc+"/status", nil)
	if err != nil {
		return 0
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	var status struct {
		Result struct {
			SyncInfo struct {
				LatestBlockHeight string `json:"latest_block_height"`
			} `json:"sync_info"`
		} `json:"result"`
	}
	if json.NewDecoder(resp.Body).Decode(&status) != nil {
		return 0
	}
	height, _ := strconv.Atoi(status.Result.SyncInfo.LatestBlockHeight)
	return height
}

// run executes one manifestd command and fails the test when it fails.
func (c *Chain) run(t *testing.T, args ...string) string {
	t.Helper()
	out, err := c.output(args...)
	if err != nil {
		t.Fatalf("manifestd %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func (c *Chain) output(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, c.bin, args...).CombinedOutput() //nolint:gosec // G204: the pinned chain binary and fixture-built arguments
	return string(out), err
}

// jsonObject returns out from its first brace on: manifestd prints notices
// such as the gas estimate before the JSON document.
func jsonObject(out string) string {
	if start := strings.Index(out, "{"); start >= 0 {
		return out[start:]
	}
	return out
}

// freeAddr picks a free loopback port for the node to listen on. The port is
// free when picked, not reserved; a node that loses it exits at startup and
// fails the test with its log.
func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick a free port: %v", err)
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().String()
}

// tail returns the end of a log file.
func tail(path string) string {
	const limit = 16 << 10
	data, err := os.ReadFile(path) //nolint:gosec // G304: a log file this package created
	if err != nil {
		return fmt.Sprintf("(log unreadable: %v)", err)
	}
	if len(data) > limit {
		data = data[len(data)-limit:]
	}
	return string(data)
}
