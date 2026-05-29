package chain

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"strconv"
	"sync"
	"time"

	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/manifest-network/fred/internal/metrics"
)

// Role label values for the signer balance gauge / failures counter.
const (
	signerRoleProvider  = "provider"
	signerRoleSubSigner = "sub_signer"
)

// SignerBalanceCollector implements prometheus.Collector and emits the
// `fred_signer_balance` gauge on every /metrics scrape.
//
// On Collect, the pool is snapshotted (live reads of ProviderAddress and
// SubSignerAddresses), one balance query is fanned out per address against
// the bank module, and each successful response yields one gauge series.
// The balance (a math.Int) is emitted as a float64 via a big.Float
// intermediate with no int64 round-trip, so balances above math.MaxInt64
// (e.g. the ~1e29 umfx dev provider) produce a healthy series instead of
// being misclassified as a failure; float64 is exact for integers <= 2^53,
// with negligible relative rounding above that for threshold alerting.
// The configured denom is both queried from bank and emitted as the `denom`
// label so the gauge is accurate on any deployment (not just umfx-denominated
// networks). Per-address query failures — a bank RPC error or a nil/empty
// response, never a value-representation condition — drop only that address's
// series and bump metrics.SignerBalanceQueryFailures (no per-index counter
// cardinality).
//
// The collector is intentionally stateless across scrapes — it does not
// cache balances, does not run a background sampler, and does not allocate
// goroutines outside of Collect. This means the gauge naturally tracks the
// live pool (e.g. after DemoteToSingleSigner) without any extra wiring.
type SignerBalanceCollector struct {
	bankQ           bankQuerier
	pool            *SignerPool
	denom           string
	scrapeTimeout   time.Duration
	desc            *prometheus.Desc
	failuresCounter *prometheus.CounterVec
}

// NewSignerBalanceCollector constructs a per-scrape signer balance collector.
// denom is the bank module denom to query (e.g. "umfx") and is also emitted as
// the `denom` label on each series so the metric stays accurate when the
// network uses a non-umfx fee denom. scrapeTimeout bounds the wall time of a
// single Collect call across all fanned-out queries.
func NewSignerBalanceCollector(bankQ bankQuerier, pool *SignerPool, denom string, scrapeTimeout time.Duration) *SignerBalanceCollector {
	return &SignerBalanceCollector{
		bankQ:         bankQ,
		pool:          pool,
		denom:         denom,
		scrapeTimeout: scrapeTimeout,
		desc: prometheus.NewDesc(
			"fred_signer_balance",
			"Per-signer balance in the configured fee denom, sampled on each /metrics scrape. Labels: role (provider|sub_signer), bech32 address, index (slice position for sub_signer, empty for provider), denom (bank denom queried).",
			[]string{"role", "address", "index", "denom"},
			nil,
		),
		failuresCounter: metrics.SignerBalanceQueryFailures,
	}
}

// Describe implements prometheus.Collector.
func (c *SignerBalanceCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements prometheus.Collector. Safe for concurrent calls.
func (c *SignerBalanceCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.scrapeTimeout)
	defer cancel()

	providerAddr := c.pool.ProviderAddress()
	subAddrs := c.pool.SubSignerAddresses()

	type target struct {
		role  string
		addr  string
		index string
	}
	targets := make([]target, 0, 1+len(subAddrs))
	targets = append(targets, target{role: signerRoleProvider, addr: providerAddr, index: ""})
	for i, a := range subAddrs {
		targets = append(targets, target{role: signerRoleSubSigner, addr: a, index: strconv.Itoa(i)})
	}

	type result struct {
		amount float64
		err    error
	}
	results := make([]result, len(targets))

	var wg sync.WaitGroup
	for i := range targets {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := c.bankQ.Balance(ctx, &banktypes.QueryBalanceRequest{
				Address: targets[i].addr,
				Denom:   c.denom,
			})
			if err != nil {
				results[i] = result{err: err}
				return
			}
			if resp == nil || resp.Balance == nil {
				results[i] = result{err: errors.New("nil balance response")}
				return
			}
			// Convert math.Int → float64 directly via an arbitrary-precision
			// big.Float intermediate. There is no int64 round-trip and no
			// representation-based rejection: large balances (the dev provider
			// holds ~1e29 umfx) emit a healthy gauge series instead of being
			// misclassified as a query failure. float64 is exact for integers
			// ≤ 2^53; above that the ~2^-52 relative rounding is irrelevant for
			// threshold alerting. Float64() stays finite far below any realistic
			// balance, so the discarded big.Accuracy is safe to ignore.
			f, _ := new(big.Float).SetInt(resp.Balance.Amount.BigInt()).Float64()
			results[i] = result{amount: f}
		}(i)
	}
	wg.Wait()

	for i, t := range targets {
		r := results[i]
		if r.err != nil {
			slog.Warn("signer balance query failed",
				"role", t.role,
				"address", t.addr,
				"denom", c.denom,
				"error", r.err,
			)
			c.failuresCounter.WithLabelValues(t.role, t.addr, c.denom).Inc()
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			prometheus.GaugeValue,
			r.amount,
			t.role,
			t.addr,
			t.index,
			c.denom,
		)
	}
}
