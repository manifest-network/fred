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

// Chain is one running single-validator manifestd node in a temporary
// directory. This file uses only the standard library and the manifestd
// command line, so it depends on that command line and on its JSON output.
type Chain struct {
	// ID is the chain ID.
	ID string

	bin, home, rpc, grpc string
	// node is the running manifestd process.
	node *os.Process
	// started is set once the node is about to run, which ends genesis
	// preparation.
	started bool
}

// Genesis is a chain before its first block: the only time accounts can be
// created with a balance and the genesis document can be edited. Start hands
// one to its prepare function and seals it when that function returns, so a
// change that could no longer reach the running node fails the test. The
// seal is the chain's own state, so a copy of the handle is sealed with it.
type Genesis struct {
	chain *Chain
}

// Key is a key in the chain's test keyring.
type Key struct {
	Name     string
	Address  string
	Mnemonic string
}

// Receipt is a committed transaction.
type Receipt struct {
	events []event
}

// event is one event of a transaction, as `manifestd query tx` prints it.
type event struct {
	Type       string `json:"type"`
	Attributes []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"attributes"`
}

// Start initializes a chain, lets prepare change its genesis, starts the node
// on free ports and waits for the first block. Start skips the test when
// BinaryEnv is unset. Cleanup stops the node.
func Start(t *testing.T, prepare func(genesis *Genesis)) *Chain {
	t.Helper()
	bin := os.Getenv(BinaryEnv)
	if bin == "" {
		t.Skipf("%s is not set; point it at the manifestd pinned in .manifestd-version", BinaryEnv)
	}
	dir := t.TempDir()
	c := &Chain{ID: "fred-e2e", bin: bin}
	c.run(t, "testnet", "init-files", "--chain-id", c.ID, "--output-dir", dir, "--v", "1",
		"--keyring-backend", "test", "--commit-timeout", "500ms", "--minimum-gas-prices", "0umfx")
	c.home = filepath.Join(dir, validatorKey, "manifestd")
	prepare(&Genesis{chain: c})
	c.started = true

	logDir := dir
	if fromEnv := os.Getenv(logDirEnv); fromEnv != "" {
		logDir = fromEnv
	}
	// The node's ports are free when picked, not reserved, and the node binds
	// them a moment later. A node that lost one to another process is started
	// again on fresh ports; nothing else has used it yet.
	for attempt := 1; ; attempt++ {
		addrs := freeAddrs(t, 4)
		c.rpc, c.grpc = addrs[0], addrs[1]
		_, port, _ := net.SplitHostPort(c.rpc)
		logPath := filepath.Join(logDir, "manifestd-"+port+".log")
		problem := c.launch(t, logPath, addrs[2], addrs[3])
		if problem == "" {
			return c
		}
		if attempt == 3 || !strings.Contains(tail(logPath), "address already in use") {
			t.Fatalf("%s\n%s", problem, tail(logPath))
		}
	}
}

// launch starts the node on the chain's ports, with the given addresses for
// its peer and profiling listeners, and waits for its second block and for
// its gRPC server, which the node starts on its own schedule and exits
// without. It returns what went wrong when the node did not get that far,
// after stopping it, and registers the node's cleanup when it did.
func (c *Chain) launch(t *testing.T, logPath, p2p, pprof string) (problem string) {
	t.Helper()
	logFile, err := os.Create(logPath) //nolint:gosec // G304: a path under t.TempDir or the suite's log directory
	if err != nil {
		t.Fatalf("create node log: %v", err)
	}
	defer func() { _ = logFile.Close() }()
	node := exec.Command(c.bin, "start", "--home", c.home, //nolint:gosec // G204: the pinned chain binary and fixture-built arguments
		"--rpc.laddr", "tcp://"+c.rpc, "--grpc.address", c.grpc,
		"--p2p.laddr", "tcp://"+p2p, "--rpc.pprof_laddr", pprof,
		"--api.enable=false", "--minimum-gas-prices", "0umfx")
	node.Stdout, node.Stderr = logFile, logFile
	if err := node.Start(); err != nil {
		t.Fatalf("start manifestd: %v", err)
	}
	exited := make(chan struct{})
	go func() { _ = node.Wait(); close(exited) }()
	stop := func() {
		// A paused node does not see SIGTERM until it continues.
		_ = node.Process.Signal(syscall.SIGCONT)
		_ = node.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			_ = node.Process.Kill()
			<-exited
		}
	}

	// The second block, not the first: the node reports a block before the
	// application has committed it, and until the first block is committed
	// every transaction is turned away with "please wait for first block".
	// A second block exists only once the first is committed.
	deadline := time.Now().Add(commitTimeout)
	for c.height() < 2 || !listening(c.grpc) {
		select {
		case <-exited:
			return "manifestd exited during startup"
		default:
		}
		if time.Now().After(deadline) {
			stop()
			return fmt.Sprintf("manifestd committed no block within %s", commitTimeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Something else can be listening where the node meant to.
	select {
	case <-exited:
		return "manifestd exited during startup"
	default:
	}
	c.node = node.Process
	t.Cleanup(func() {
		stop()
		if t.Failed() {
			t.Logf("manifestd log (%s):\n%s", logPath, tail(logPath))
		}
	})
	return ""
}

// AddKey creates a key in the chain's keyring.
func (g Genesis) AddKey(t *testing.T, name string) Key {
	t.Helper()
	g.requireOpen(t)
	return g.chain.AddKey(t, name)
}

// AddAccount gives an address an account and a balance.
func (g Genesis) AddAccount(t *testing.T, address, coins string) {
	t.Helper()
	g.requireOpen(t)
	g.chain.run(t, "genesis", "add-genesis-account", address, coins, "--home", g.chain.home, "--keyring-backend", "test")
}

// Edit applies edit to the genesis document.
func (g Genesis) Edit(t *testing.T, edit func(document map[string]any)) {
	t.Helper()
	g.requireOpen(t)
	path := filepath.Join(g.chain.home, "config", "genesis.json")
	raw, err := os.ReadFile(path) //nolint:gosec // G304: a path under t.TempDir
	if err != nil {
		t.Fatalf("read genesis: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode genesis: %v", err)
	}
	edit(document)
	if raw, err = json.Marshal(document); err != nil {
		t.Fatalf("encode genesis: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write genesis: %v", err)
	}
}

func (g Genesis) requireOpen(t *testing.T) {
	t.Helper()
	if g.sealed() {
		t.Fatal("the genesis was used after the chain started: the change would not reach the running node")
	}
}

// sealed reports whether the chain has started.
func (g Genesis) sealed() bool { return g.chain.started }

// Pause freezes the node: it stops answering and stops producing blocks, as
// a chain outage looks from outside. Queries and transactions block until
// Resume.
func (c *Chain) Pause(t *testing.T) {
	t.Helper()
	if err := c.node.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("pause manifestd: %v", err)
	}
}

// Resume lets a paused node continue from where it stopped.
func (c *Chain) Resume(t *testing.T) {
	t.Helper()
	if err := c.node.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("resume manifestd: %v", err)
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

// Tx signs and broadcasts a transaction with the named key, waits until it is
// committed, and returns its receipt. A transaction the chain rejects fails
// the test.
func (c *Chain) Tx(t *testing.T, from string, args ...string) Receipt {
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
			Code   int     `json:"code"`
			RawLog string  `json:"raw_log"`
			Events []event `json:"events"`
		}
		out, err := c.output("q", "tx", sent.Hash, "--node", "tcp://"+c.rpc, "--output", "json")
		if err == nil {
			err = json.Unmarshal([]byte(jsonObject(out)), &committed)
		}
		if err == nil {
			if committed.Code != 0 {
				t.Fatalf("tx %v failed with code %d: %s", args, committed.Code, committed.RawLog)
			}
			return Receipt{events: committed.Events}
		}
		if time.Now().After(deadline) {
			t.Fatalf("tx %v (%s) was not committed within %s: %v\n%s", args, sent.Hash, commitTimeout, err, out)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Event returns the attributes of the one event of the given type that the
// transaction emitted. This is how a caller learns what its own transaction
// created, without searching the chain for it.
func (r Receipt) Event(t *testing.T, eventType string) map[string]string {
	t.Helper()
	var found []map[string]string
	for _, event := range r.events {
		if event.Type != eventType {
			continue
		}
		attributes := make(map[string]string, len(event.Attributes))
		for _, attribute := range event.Attributes {
			attributes[attribute.Key] = attribute.Value
		}
		found = append(found, attributes)
	}
	if len(found) != 1 {
		t.Fatalf("the transaction emitted %d %s events, want exactly one: %+v", len(found), eventType, r.events)
	}
	return found[0]
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

// freeAddrs picks that many free loopback ports for the node to listen on.
// They are picked together, so they differ from each other: a port picked
// and released alone can be handed out again by the very next pick. The
// ports are free when picked, not reserved; a node that loses one exits at
// startup and is started again on fresh ports.
func freeAddrs(t *testing.T, count int) []string {
	t.Helper()
	addrs := make([]string, count)
	listeners := make([]net.Listener, 0, count)
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	for i := range addrs {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("pick a free port: %v", err)
		}
		listeners = append(listeners, listener)
		addrs[i] = listener.Addr().String()
	}
	return addrs
}

// listening reports whether something accepts connections on the address.
func listening(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
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
