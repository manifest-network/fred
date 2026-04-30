// create-test-leases.go creates N leases on a local chain and uploads payloads to Fred.
//
// Usage:
//
//	go run scripts/create-test-leases.go \
//	  -n 10 \
//	  -fred-url https://localhost:8080 \
//	  -sku 019c9b96-7a19-7000-8986-6999995a0c88 \
//	  -tenant-key user2 \
//	  -keyring-dir ~/.manifest-billing
package main

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptocodec "github.com/cosmos/cosmos-sdk/crypto/codec"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"

	"github.com/manifest-network/fred/internal/adr036"
	"github.com/manifest-network/fred/internal/auth"
)

const blockWait = 7 * time.Second

func main() {
	var (
		count        int
		fredURL      string
		skuUUID      string
		tenantKey    string
		fundFrom     string
		keyringDir   string
		chainID      string
		manifestd    string
		fundAmount   string
		insecure     bool
		node         string
		serviceName  string
		serviceNames string
	)

	flag.IntVar(&count, "n", 10, "number of leases to create")
	flag.StringVar(&fredURL, "fred-url", "https://localhost:8080", "Fred API URL")
	flag.StringVar(&skuUUID, "sku", "", "SKU UUID (required)")
	flag.StringVar(&tenantKey, "tenant-key", "user2", "tenant key name in keyring")
	flag.StringVar(&fundFrom, "fund-from", "user1", "key to fund tenant credit from")
	flag.StringVar(&keyringDir, "keyring-dir", os.ExpandEnv("$HOME/.manifest-billing"), "keyring directory")
	flag.StringVar(&chainID, "chain-id", "manifest-ledger-beta", "chain ID")
	flag.StringVar(&manifestd, "manifestd", os.ExpandEnv("$HOME/go/bin/manifestd"), "path to manifestd")
	flag.StringVar(&fundAmount, "fund-amount", "1000000000factory/manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj/upwr", "amount to fund tenant credit (default 1000 PWR)")
	flag.BoolVar(&insecure, "insecure", true, "skip TLS verification for Fred")
	flag.StringVar(&node, "node", "tcp://localhost:26657", "chain RPC endpoint (e.g. https://primary.example/api/chain/rpc)")
	flag.StringVar(&serviceName, "service-name", "", "optional service_name (RFC 1123 DNS label) per LeaseItem; required to attach custom domains")
	flag.StringVar(&serviceNames, "service-names", "", "comma-separated service entries for a stack lease. Each entry is 'name' or 'name=image' (e.g. 'web,api' or 'web=docker.io/library/nginx:alpine,api=docker.io/library/httpd:alpine'). Overrides --service-name and uses a stack manifest payload.")
	flag.Parse()

	if skuUUID == "" {
		log.Fatal("--sku is required")
	}

	// Set bech32 prefix before any address operations
	sdkConfig := sdk.GetConfig()
	sdkConfig.SetBech32PrefixForAccount("manifest", "manifestpub")

	// Open keyring and get tenant key
	interfaceRegistry := codectypes.NewInterfaceRegistry()
	cryptocodec.RegisterInterfaces(interfaceRegistry)
	authtypes.RegisterInterfaces(interfaceRegistry)
	cdc := codec.NewProtoCodec(interfaceRegistry)

	kr, err := keyring.New("manifest", "test", keyringDir, nil, cdc)
	if err != nil {
		log.Fatalf("failed to open keyring: %v", err)
	}

	keyInfo, err := kr.Key(tenantKey)
	if err != nil {
		log.Fatalf("tenant key %q not found: %v", tenantKey, err)
	}

	tenantAddr, err := keyInfo.GetAddress()
	if err != nil {
		log.Fatalf("failed to get tenant address: %v", err)
	}
	log.Printf("tenant: %s (%s)", tenantKey, tenantAddr)

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		},
	}

	txFlags := []string{
		"--keyring-dir", keyringDir,
		"--keyring-backend", "test",
		"--chain-id", chainID,
		"--node", node,
		"--gas", "auto", "--gas-adjustment", "1.5",
		"--gas-prices", "0.025umfx",
		"--broadcast-mode", "sync",
		"--output", "json",
		"--yes",
	}

	// Step 1: Fund tenant credit account
	log.Printf("funding tenant credit account with %s...", fundAmount)
	fundArgs := append([]string{"tx", "billing", "fund-credit",
		tenantAddr.String(), fundAmount,
		"--from", fundFrom,
	}, txFlags...)
	if out, err := runCmd(manifestd, fundArgs...); err != nil {
		log.Printf("fund-credit failed (may already be funded): %s", truncate(out, 200))
	} else {
		log.Printf("fund-credit tx broadcast, waiting for block...")
		waitForTx(manifestd, node, extractTxHash(out))
	}

	// Step 2: Create leases sequentially (each must wait for block inclusion)
	log.Printf("creating %d leases...", count)
	start := time.Now()
	var created int

	// Stack mode: split serviceNames into name[=image] entries. Empty slice =
	// single-service mode. Default image is the existing demo container.
	const defaultImage = "docker.io/lifted/demo-games:tetris"
	type stackService struct{ Name, Image string }
	var stackServices []stackService
	if serviceNames != "" {
		for _, raw := range strings.Split(serviceNames, ",") {
			raw = strings.TrimSpace(raw)
			if raw == "" {
				continue
			}
			name, image, hasImage := strings.Cut(raw, "=")
			if !hasImage || image == "" {
				image = defaultImage
			}
			stackServices = append(stackServices, stackService{Name: name, Image: image})
		}
	}

	// Build the per-service docker manifest. The leaseIndex is interpolated
	// into the env so re-runs of -n>1 each produce a distinguishable lease.
	buildSvcManifest := func(image string, leaseIndex int) string {
		return fmt.Sprintf(`{"image":%q,"ports":{"8080/tcp":{}},"env":{"LEASE_INDEX":"%d"},"tmpfs":["/var/cache/nginx","/var/run"]}`, image, leaseIndex)
	}

	for i := range count {
		var payload []byte
		var itemArgs []string
		if len(stackServices) > 0 {
			// Stack manifest: {"services": {"web": {...}, "api": {...}}}
			services := make(map[string]json.RawMessage, len(stackServices))
			for _, svc := range stackServices {
				services[svc.Name] = json.RawMessage(buildSvcManifest(svc.Image, i))
				itemArgs = append(itemArgs, fmt.Sprintf("%s:1:%s", skuUUID, svc.Name))
			}
			payload, err = json.Marshal(map[string]any{"services": services})
			if err != nil {
				log.Fatalf("failed to marshal stack manifest: %v", err)
			}
		} else {
			payload = []byte(buildSvcManifest(defaultImage, i))
			itemArg := skuUUID + ":1"
			if serviceName != "" {
				itemArg = itemArg + ":" + serviceName
			}
			itemArgs = []string{itemArg}
		}
		hash := sha256.Sum256(payload)
		metaHashHex := hex.EncodeToString(hash[:])

		log.Printf("[%d/%d] creating lease (%d item(s))...", i+1, count, len(itemArgs))
		createArgs := append([]string{"tx", "billing", "create-lease"}, itemArgs...)
		createArgs = append(createArgs,
			"--meta-hash", metaHashHex,
			"--from", tenantKey,
		)
		createArgs = append(createArgs, txFlags...)

		// Retry up to 3 times on sequence mismatch
		var txHash string
		for attempt := range 3 {
			if attempt > 0 {
				log.Printf("[%d/%d] retrying after sequence mismatch (attempt %d)...", i+1, count, attempt+1)
				time.Sleep(blockWait)
			}
			out, err := runCmd(manifestd, createArgs...)
			if err != nil {
				if strings.Contains(out, "account sequence mismatch") {
					continue // retry
				}
				log.Printf("[%d/%d] create-lease failed: %s", i+1, count, truncate(out, 200))
				break
			}
			txHash = extractTxHash(out)
			break
		}

		if txHash == "" {
			log.Printf("[%d/%d] no tx hash, skipping", i+1, count)
			continue
		}
		log.Printf("[%d/%d] tx broadcast: %s, waiting for block...", i+1, count, txHash)

		// Wait for tx inclusion and extract lease UUID from events
		leaseUUID := waitAndExtractLeaseUUID(manifestd, node, txHash)
		if leaseUUID == "" {
			log.Printf("[%d/%d] could not extract lease UUID", i+1, count)
			continue
		}
		log.Printf("[%d/%d] lease: %s", i+1, count, leaseUUID)

		// Step 3: Upload payload to Fred
		token := createPayloadToken(kr, tenantKey, tenantAddr.String(), leaseUUID, metaHashHex)
		if uploadPayload(httpClient, fredURL, leaseUUID, token, payload, i+1, count) {
			created++
		}
	}

	elapsed := time.Since(start)
	log.Printf("done: %d/%d leases created+uploaded in %s", created, count, elapsed)
}

func runCmd(bin string, args ...string) (string, error) {
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), "GOCOVERDIR=")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func extractTxHash(output string) string {
	jsonStart := strings.Index(output, "{")
	if jsonStart < 0 {
		return ""
	}
	var resp struct {
		TxHash string `json:"txhash"`
	}
	if err := json.Unmarshal([]byte(output[jsonStart:]), &resp); err != nil {
		return ""
	}
	return resp.TxHash
}

func waitForTx(manifestd, node, txHash string) {
	if txHash == "" {
		time.Sleep(blockWait)
		return
	}
	// Poll for tx inclusion
	for range 10 {
		time.Sleep(2 * time.Second)
		out, err := runCmd(manifestd, "q", "tx", txHash, "--node", node, "--output", "json")
		if err == nil && strings.Contains(out, `"txhash"`) {
			return
		}
	}
	log.Printf("timeout waiting for tx %s", txHash)
}

func waitAndExtractLeaseUUID(manifestd, node, txHash string) string {
	if txHash == "" {
		return ""
	}
	for range 10 {
		time.Sleep(2 * time.Second)
		out, _ := runCmd(manifestd, "q", "tx", txHash, "--node", node, "--output", "json")

		jsonStart := strings.Index(out, "{")
		if jsonStart < 0 {
			continue
		}

		// Look for lease_uuid in the events
		// Events contain key-value pairs like {"key":"lease_uuid","value":"..."}
		type Event struct {
			Type       string `json:"type"`
			Attributes []struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"attributes"`
		}
		type TxResponse struct {
			Code   int     `json:"code"`
			Events []Event `json:"events"`
			Logs   []struct {
				Events []Event `json:"events"`
			} `json:"logs"`
		}
		var resp TxResponse
		if err := json.Unmarshal([]byte(out[jsonStart:]), &resp); err != nil {
			continue
		}
		if resp.Code != 0 {
			log.Printf("tx %s failed with code %d", txHash, resp.Code)
			return ""
		}

		// Search events for lease_uuid
		for _, evt := range resp.Events {
			for _, attr := range evt.Attributes {
				if attr.Key == "lease_uuid" {
					return attr.Value
				}
			}
		}
		// Also check logs.events
		for _, logEntry := range resp.Logs {
			for _, evt := range logEntry.Events {
				for _, attr := range evt.Attributes {
					if attr.Key == "lease_uuid" {
						return attr.Value
					}
				}
			}
		}
	}
	return ""
}

func createPayloadToken(kr keyring.Keyring, keyName, tenantAddr, leaseUUID, metaHashHex string) string {
	timestamp := time.Now().Unix()
	signData := auth.FormatPayloadSignData(leaseUUID, metaHashHex, timestamp)

	keyInfo, err := kr.Key(keyName)
	if err != nil {
		log.Fatalf("failed to get key: %v", err)
	}
	pubKey, err := keyInfo.GetPubKey()
	if err != nil {
		log.Fatalf("failed to get public key: %v", err)
	}
	pubKeyB64 := base64.StdEncoding.EncodeToString(pubKey.Bytes())

	signBytes := adr036.CreateSignBytes(signData, tenantAddr)
	sig, _, err := kr.Sign(keyName, signBytes, 0)
	if err != nil {
		log.Fatalf("failed to sign: %v", err)
	}

	token := map[string]any{
		"tenant":     tenantAddr,
		"lease_uuid": leaseUUID,
		"meta_hash":  metaHashHex,
		"timestamp":  timestamp,
		"pub_key":    pubKeyB64,
		"signature":  base64.StdEncoding.EncodeToString(sig),
	}

	jsonBytes, _ := json.Marshal(token)
	return base64.StdEncoding.EncodeToString(jsonBytes)
}

func uploadPayload(client *http.Client, fredURL, leaseUUID, token string, payload []byte, idx, total int) bool {
	url := fmt.Sprintf("%s/v1/leases/%s/data", fredURL, leaseUUID)
	req, err := http.NewRequest("POST", url, strings.NewReader(string(payload)))
	if err != nil {
		log.Printf("[%d/%d] failed to create request: %v", idx, total, err)
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[%d/%d] upload failed: %v", idx, total, err)
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("[%d/%d] payload uploaded for %s", idx, total, leaseUUID)
		return true
	}
	log.Printf("[%d/%d] upload returned %d: %s", idx, total, resp.StatusCode, truncate(string(body), 200))
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
