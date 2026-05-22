//go:build ignore

// deploy-app.go deploys a single container image to a given SKU: creates the
// lease on-chain, builds a minimal manifest, and uploads it to Fred.
//
// For richer manifests (tmpfs, health_check, command, depends_on stacks, …)
// pass -manifest-file pointing at a JSON file matching the schema in
// internal/backend/shared/manifest/manifest.go. The file is hashed and
// uploaded verbatim; -image / -ports / -env are ignored. -service-name
// still applies to the on-chain lease item.
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
// fund-credit is always signed by -tenant-key, so the tenant address must
// already hold gas (-gas-denom) plus the credit denom embedded in
// -fund-amount. To top it up, pass -faucet-url and -faucet-denom: the
// script POSTs once to <faucet-url>/credit for the chosen denom, waits a
// block, then runs fund-credit. Faucet drips are fixed-size — set
// -fund-amount no higher than the per-drip cap configured on the target
// faucet. Pass -skip-fund if the tenant credit account is already topped
// up.
//
// On success the lease UUID is printed to stdout on its own line for easy
// capture (everything else goes to stderr via log).
package main

import (
	"bytes"
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
		image        string
		skuUUID      string
		fredURL      string
		tenantKey    string
		faucetURL    string
		faucetDenom  string
		gasDenom     string
		gasPrice     string
		keyringDir   string
		chainID      string
		manifestd    string
		fundAmount   string
		insecure     bool
		node         string
		serviceName  string
		portsCSV     string
		envCSV       string
		manifestFile string
		skipFund     bool
	)

	flag.StringVar(&image, "image", "", "container image to deploy (required)")
	flag.StringVar(&skuUUID, "sku", "", "SKU UUID (required)")
	flag.StringVar(&fredURL, "fred-url", "https://localhost:8080", "Fred API URL")
	flag.StringVar(&tenantKey, "tenant-key", "user2", "tenant key name in keyring")
	flag.StringVar(&faucetURL, "faucet-url", "https://faucet.testnet.manifest.network", "cosmjs-style faucet base URL; when non-empty, the script drips -faucet-denom to the tenant before fund-credit (set to empty string to skip)")
	flag.StringVar(&faucetDenom, "faucet-denom", "factory/manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj/upwr", "denom to request from the faucet (only used with -faucet-url)")
	flag.StringVar(&gasDenom, "gas-denom", "factory/manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj/upwr", "denom used for tx gas fees (combined with -gas-price to form --gas-prices)")
	flag.StringVar(&gasPrice, "gas-price", "1.0", "gas price per unit (combined with -gas-denom to form --gas-prices)")
	flag.StringVar(&keyringDir, "keyring-dir", os.ExpandEnv("$HOME/.manifest-billing"), "keyring directory")
	flag.StringVar(&chainID, "chain-id", "manifest-ledger-beta", "chain ID")
	flag.StringVar(&manifestd, "manifestd", os.ExpandEnv("$HOME/go/bin/manifestd"), "path to manifestd")
	flag.StringVar(&fundAmount, "fund-amount", "1000000000factory/manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj/upwr", "amount to fund tenant credit")
	flag.BoolVar(&insecure, "insecure", true, "skip TLS verification for Fred (default on for self-signed devnet/testnet certs; set false for production)")
	flag.StringVar(&node, "node", "tcp://localhost:26657", "chain RPC endpoint")
	flag.StringVar(&serviceName, "service-name", "", "optional service_name (RFC 1123 DNS label); required to attach custom domains")
	flag.StringVar(&portsCSV, "ports", "8080/tcp", "comma-separated ports (e.g. '8080/tcp,9090/tcp')")
	flag.StringVar(&envCSV, "env", "", "comma-separated KEY=VAL env vars")
	flag.StringVar(&manifestFile, "manifest-file", "", "path to a JSON manifest file (flat or stack shape); when set, -image/-ports/-env are ignored (-service-name still applies to the on-chain lease item)")
	flag.BoolVar(&skipFund, "skip-fund", false, "skip the tenant credit funding step")
	flag.Parse()

	if image == "" && manifestFile == "" {
		log.Fatal("--image (or --manifest-file) is required")
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
		"--gas-prices", gasPrice + gasDenom,
		"--broadcast-mode", "sync",
		"--output", "json",
		"--yes",
	}

	if !skipFund {
		if faucetURL != "" {
			log.Printf("dripping %s from faucet to %s...", faucetDenom, tenantAddr)
			if err := dripFromFaucet(httpClient, faucetURL, tenantAddr.String(), faucetDenom); err != nil {
				log.Printf("faucet drip failed (may be on cooldown): %v", err)
			}
			log.Printf("waiting for faucet funds to land...")
			time.Sleep(blockWait)
		}

		log.Printf("funding tenant credit account with %s...", fundAmount)
		fundArgs := append([]string{"tx", "billing", "fund-credit",
			tenantAddr.String(), fundAmount,
			"--from", tenantKey,
		}, txFlags...)
		out, err := runCmd(manifestd, fundArgs...)
		if err != nil {
			log.Fatalf("fund-credit broadcast failed: %s", truncate(out, 2000))
		}
		txHash, code, rawLog, ok := extractTxResult(out)
		if !ok {
			log.Fatalf("fund-credit returned unparseable response: %s", truncate(out, 2000))
		}
		if code != 0 {
			log.Fatalf("fund-credit rejected at broadcast (code %d): %s", code, truncate(rawLog, 1000))
		}
		log.Printf("fund-credit tx %s broadcast, waiting for block...", txHash)
		if err := waitForTx(manifestd, node, txHash); err != nil {
			log.Fatalf("fund-credit failed on-chain: %v", err)
		}
	}

	var payload []byte
	source := "image " + image
	if manifestFile != "" {
		var err error
		payload, err = os.ReadFile(manifestFile)
		if err != nil {
			log.Fatalf("failed to read manifest file: %v", err)
		}
		if !json.Valid(payload) {
			log.Fatalf("manifest file %s is not valid JSON", manifestFile)
		}
		source = "manifest " + manifestFile
	} else {
		payload = buildManifest(image, portsCSV, envCSV)
	}
	hash := sha256.Sum256(payload)
	metaHashHex := hex.EncodeToString(hash[:])

	itemArg := skuUUID + ":1"
	if serviceName != "" {
		itemArg = itemArg + ":" + serviceName
	}

	log.Printf("creating lease for sku %s (%s)...", skuUUID, source)
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
			log.Fatalf("create-lease failed: %s", truncate(out, 2000))
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
	log.Printf("deployed %s to lease %s", source, leaseUUID)
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

func extractTxResult(output string) (txHash string, code int, rawLog string, ok bool) {
	jsonStart := strings.Index(output, "{")
	if jsonStart < 0 {
		return "", 0, "", false
	}
	var resp struct {
		TxHash string `json:"txhash"`
		Code   int    `json:"code"`
		RawLog string `json:"raw_log"`
	}
	if err := json.Unmarshal([]byte(output[jsonStart:]), &resp); err != nil {
		return "", 0, "", false
	}
	return resp.TxHash, resp.Code, resp.RawLog, true
}

func extractTxHash(output string) string {
	h, _, _, _ := extractTxResult(output)
	return h
}

func waitForTx(manifestd, node, txHash string) error {
	if txHash == "" {
		time.Sleep(blockWait)
		return nil
	}
	for range 10 {
		time.Sleep(2 * time.Second)
		out, err := runCmd(manifestd, "q", "tx", txHash, "--node", node, "--output", "json")
		if err != nil {
			continue
		}
		_, code, rawLog, ok := extractTxResult(out)
		if !ok {
			continue
		}
		if code != 0 {
			return fmt.Errorf("tx %s on-chain code %d: %s", txHash, code, truncate(rawLog, 1000))
		}
		return nil
	}
	return fmt.Errorf("timeout waiting for tx %s", txHash)
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
	jsonBytes, err := json.Marshal(token)
	if err != nil {
		log.Fatalf("failed to marshal payload token: %v", err)
	}
	return base64.StdEncoding.EncodeToString(jsonBytes)
}

func uploadPayload(client *http.Client, fredURL, leaseUUID, token string, payload []byte) bool {
	url := fmt.Sprintf("%s/v1/leases/%s/data", fredURL, leaseUUID)
	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
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
	body, readErr := io.ReadAll(resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true
	}
	if readErr != nil {
		log.Printf("upload returned %d (response body read error: %v)", resp.StatusCode, readErr)
	} else {
		log.Printf("upload returned %d: %s", resp.StatusCode, truncate(string(body), 400))
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func dripFromFaucet(client *http.Client, faucetURL, address, denom string) error {
	url := strings.TrimRight(faucetURL, "/") + "/credit"
	body, err := json.Marshal(map[string]string{"denom": denom, "address": address})
	if err != nil {
		return fmt.Errorf("marshal faucet request: %w", err)
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, readErr := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	if readErr != nil {
		return fmt.Errorf("faucet %d (response body read error: %w)", resp.StatusCode, readErr)
	}
	return fmt.Errorf("faucet %d: %s", resp.StatusCode, truncate(string(respBody), 200))
}
