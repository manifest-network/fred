//go:build ignore

// deploy-app.go deploys a single container image to a given SKU: creates the
// lease on-chain, builds a minimal manifest, and uploads it to Fred.
//
// Usage:
//
//	go run scripts/deploy-app.go \
//	  -image docker.io/library/nginx:alpine \
//	  -sku 019c9b96-7a19-7000-8986-6999995a0c88 \
//	  -fred-url https://fred.testnet.example \
//	  -node https://primary.testnet.example/api/chain/rpc \
//	  -chain-id manifest-ledger-beta
//
// On success the lease UUID is printed to stdout on its own line for easy
// capture (everything else goes to stderr via log).
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
		image       string
		skuUUID     string
		fredURL     string
		tenantKey   string
		fundFrom    string
		keyringDir  string
		chainID     string
		manifestd   string
		fundAmount  string
		insecure    bool
		node        string
		serviceName string
		portsCSV    string
		envCSV      string
		skipFund    bool
	)

	flag.StringVar(&image, "image", "", "container image to deploy (required)")
	flag.StringVar(&skuUUID, "sku", "", "SKU UUID (required)")
	flag.StringVar(&fredURL, "fred-url", "https://localhost:8080", "Fred API URL")
	flag.StringVar(&tenantKey, "tenant-key", "user2", "tenant key name in keyring")
	flag.StringVar(&fundFrom, "fund-from", "user1", "key to fund tenant credit from")
	flag.StringVar(&keyringDir, "keyring-dir", os.ExpandEnv("$HOME/.manifest-billing"), "keyring directory")
	flag.StringVar(&chainID, "chain-id", "manifest-ledger-beta", "chain ID")
	flag.StringVar(&manifestd, "manifestd", os.ExpandEnv("$HOME/go/bin/manifestd"), "path to manifestd")
	flag.StringVar(&fundAmount, "fund-amount", "1000000000factory/manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj/upwr", "amount to fund tenant credit")
	flag.BoolVar(&insecure, "insecure", true, "skip TLS verification for Fred")
	flag.StringVar(&node, "node", "tcp://localhost:26657", "chain RPC endpoint")
	flag.StringVar(&serviceName, "service-name", "", "optional service_name (RFC 1123 DNS label); required to attach custom domains")
	flag.StringVar(&portsCSV, "ports", "8080/tcp", "comma-separated ports (e.g. '8080/tcp,9090/tcp')")
	flag.StringVar(&envCSV, "env", "", "comma-separated KEY=VAL env vars")
	flag.BoolVar(&skipFund, "skip-fund", false, "skip the tenant credit funding step")
	flag.Parse()

	if image == "" {
		log.Fatal("--image is required")
	}
	if skuUUID == "" {
		log.Fatal("--sku is required")
	}

	sdkConfig := sdk.GetConfig()
	sdkConfig.SetBech32PrefixForAccount("manifest", "manifestpub")

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

	if !skipFund {
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
	}

	payload := buildManifest(image, portsCSV, envCSV)
	hash := sha256.Sum256(payload)
	metaHashHex := hex.EncodeToString(hash[:])

	itemArg := skuUUID + ":1"
	if serviceName != "" {
		itemArg = itemArg + ":" + serviceName
	}

	log.Printf("creating lease for sku %s (image %s)...", skuUUID, image)
	createArgs := append([]string{"tx", "billing", "create-lease", itemArg,
		"--meta-hash", metaHashHex,
		"--from", tenantKey,
	}, txFlags...)

	var txHash string
	for attempt := range 3 {
		if attempt > 0 {
			log.Printf("retrying create-lease (attempt %d)...", attempt+1)
			time.Sleep(blockWait)
		}
		out, err := runCmd(manifestd, createArgs...)
		if err != nil {
			if strings.Contains(out, "account sequence mismatch") {
				continue
			}
			log.Fatalf("create-lease failed: %s", truncate(out, 400))
		}
		txHash = extractTxHash(out)
		break
	}
	if txHash == "" {
		log.Fatal("create-lease produced no tx hash")
	}
	log.Printf("tx broadcast: %s, waiting for block...", txHash)

	leaseUUID := waitAndExtractLeaseUUID(manifestd, node, txHash)
	if leaseUUID == "" {
		log.Fatal("could not extract lease UUID from tx events")
	}
	log.Printf("lease: %s", leaseUUID)

	token := createPayloadToken(kr, tenantKey, tenantAddr.String(), leaseUUID, metaHashHex)
	if !uploadPayload(httpClient, fredURL, leaseUUID, token, payload) {
		log.Fatal("payload upload failed")
	}
	log.Printf("deployed %s to lease %s", image, leaseUUID)
	fmt.Println(leaseUUID)
}

func buildManifest(image, portsCSV, envCSV string) []byte {
	ports := map[string]struct{}{}
	for _, p := range strings.Split(portsCSV, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			ports[p] = struct{}{}
		}
	}
	env := map[string]string{}
	if envCSV != "" {
		for _, kv := range strings.Split(envCSV, ",") {
			kv = strings.TrimSpace(kv)
			if kv == "" {
				continue
			}
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				log.Fatalf("bad --env entry %q (expected KEY=VAL)", kv)
			}
			env[k] = v
		}
	}
	m := map[string]any{
		"image": image,
		"ports": ports,
	}
	if len(env) > 0 {
		m["env"] = env
	}
	b, err := json.Marshal(m)
	if err != nil {
		log.Fatalf("failed to marshal manifest: %v", err)
	}
	return b
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
		for _, evt := range resp.Events {
			for _, attr := range evt.Attributes {
				if attr.Key == "lease_uuid" {
					return attr.Value
				}
			}
		}
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

func uploadPayload(client *http.Client, fredURL, leaseUUID, token string, payload []byte) bool {
	url := fmt.Sprintf("%s/v1/leases/%s/data", fredURL, leaseUUID)
	req, err := http.NewRequest("POST", url, strings.NewReader(string(payload)))
	if err != nil {
		log.Printf("failed to create request: %v", err)
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("upload failed: %v", err)
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true
	}
	log.Printf("upload returned %d: %s", resp.StatusCode, truncate(string(body), 400))
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
