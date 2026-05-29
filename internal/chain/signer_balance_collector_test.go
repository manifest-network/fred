package chain

import (
	"context"
	"errors"
	stdmath "math"
	"strconv"
	"sync"
	"testing"
	"time"

	"cosmossdk.io/math"
	sdk "github.com/cosmos/cosmos-sdk/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/manifest-network/fred/internal/metrics"
)

// gatherGaugeSeries gathers all `fred_signer_balance` series from the
// given registry and returns one entry per (label-set → value).
type gaugeSample struct {
	role    string
	address string
	index   string
	denom   string
	value   float64
}

func gatherGaugeSeries(t *testing.T, reg *prometheus.Registry) []gaugeSample {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)

	var out []gaugeSample
	for _, fam := range families {
		if fam.GetName() != "fred_signer_balance" {
			continue
		}
		for _, m := range fam.GetMetric() {
			s := gaugeSample{value: m.GetGauge().GetValue()}
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "role":
					s.role = lp.GetValue()
				case "address":
					s.address = lp.GetValue()
				case "index":
					s.index = lp.GetValue()
				case "denom":
					s.denom = lp.GetValue()
				}
			}
			out = append(out, s)
		}
	}
	return out
}

// newCollectorForTest constructs a SignerBalanceCollector wired to a fresh
// Prometheus registry. It returns the collector and the registry so callers
// can gather/assert without polluting prometheus.DefaultRegisterer. The
// failures counter (metrics.SignerBalanceQueryFailures) is global; tests
// Reset it before use.
func newCollectorForTest(t *testing.T, bankQ bankQuerier, pool *SignerPool, denom string, timeout time.Duration) (*SignerBalanceCollector, *prometheus.Registry) {
	t.Helper()
	metrics.SignerBalanceQueryFailures.Reset()
	collector := NewSignerBalanceCollector(bankQ, pool, denom, timeout)
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(collector))
	return collector, reg
}

func TestSignerBalanceCollector_SingleSignerMode_EmitsOnlyProvider(t *testing.T) {
	pool := newTestSignerPool(t, 0)
	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			return &banktypes.QueryBalanceResponse{
				Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(42)},
			}, nil
		},
	}

	_, reg := newCollectorForTest(t, bankQ, pool, "umfx", 5*time.Second)
	samples := gatherGaugeSeries(t, reg)
	require.Len(t, samples, 1, "should emit only the provider series in single-signer mode")
	assert.Equal(t, "provider", samples[0].role)
	assert.Equal(t, pool.ProviderAddress(), samples[0].address)
	assert.Equal(t, "", samples[0].index)
	assert.Equal(t, "umfx", samples[0].denom)
	assert.Equal(t, 42.0, samples[0].value)
}

func TestSignerBalanceCollector_MultiSigner_EmitsAllSeries(t *testing.T) {
	pool := newTestSignerPool(t, 3)
	subAddrs := pool.SubSignerAddresses()
	providerAddr := pool.ProviderAddress()

	// Per-address amounts: provider=1000, sub[0]=10, sub[1]=20, sub[2]=30.
	amounts := map[string]int64{
		providerAddr: 1000,
		subAddrs[0]:  10,
		subAddrs[1]:  20,
		subAddrs[2]:  30,
	}
	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			amt, ok := amounts[in.Address]
			require.True(t, ok, "unexpected balance query for %s", in.Address)
			require.Equal(t, "umfx", in.Denom)
			return &banktypes.QueryBalanceResponse{
				Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(amt)},
			}, nil
		},
	}

	_, reg := newCollectorForTest(t, bankQ, pool, "umfx", 5*time.Second)
	samples := gatherGaugeSeries(t, reg)
	require.Len(t, samples, 4, "should emit 1 provider + 3 sub_signer series")

	byKey := make(map[string]gaugeSample, len(samples))
	for _, s := range samples {
		byKey[s.role+"|"+s.index] = s
	}

	require.Contains(t, byKey, "provider|")
	assert.Equal(t, providerAddr, byKey["provider|"].address)
	assert.Equal(t, "umfx", byKey["provider|"].denom)
	assert.Equal(t, 1000.0, byKey["provider|"].value)

	for i, addr := range subAddrs {
		key := "sub_signer|" + strconv.Itoa(i)
		require.Contains(t, byKey, key, "missing series for slice index %d", i)
		assert.Equal(t, addr, byKey[key].address)
		assert.Equal(t, "umfx", byKey[key].denom)
		assert.Equal(t, float64(amounts[addr]), byKey[key].value)
	}
}

func TestSignerBalanceCollector_PerAddressFailure_DropsSeriesAndIncrementsCounter(t *testing.T) {
	pool := newTestSignerPool(t, 3)
	subAddrs := pool.SubSignerAddresses()
	providerAddr := pool.ProviderAddress()

	failAddr := subAddrs[1]
	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			if in.Address == failAddr {
				return nil, errors.New("rpc unavailable")
			}
			return &banktypes.QueryBalanceResponse{
				Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(100)},
			}, nil
		},
	}

	collector, reg := newCollectorForTest(t, bankQ, pool, "umfx", 5*time.Second)

	// First scrape: 3 series emitted (no index=1), counter at 1 for that role/address.
	samples := gatherGaugeSeries(t, reg)
	require.Len(t, samples, 3, "failing series should be dropped")
	for _, s := range samples {
		assert.NotEqual(t, "1", s.index, "series for slice index 1 should not be emitted")
		assert.NotEqual(t, failAddr, s.address, "series for failing address should not be emitted")
	}
	// Provider still present
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("sub_signer", failAddr, "umfx")))
	// No counter increment for provider or for other sub-signers.
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("provider", providerAddr, "umfx")))
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("sub_signer", subAddrs[0], "umfx")))
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("sub_signer", subAddrs[2], "umfx")))

	// Second scrape: same failure → counter at 2, still no series for index 1.
	_ = collector // keep reference
	samples2 := gatherGaugeSeries(t, reg)
	require.Len(t, samples2, 3)
	assert.Equal(t, 2.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("sub_signer", failAddr, "umfx")))
}

func TestSignerBalanceCollector_AllQueriesFail_NoGaugeSeries(t *testing.T) {
	pool := newTestSignerPool(t, 3)
	subAddrs := pool.SubSignerAddresses()
	providerAddr := pool.ProviderAddress()

	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			return nil, errors.New("chain unreachable")
		},
	}

	_, reg := newCollectorForTest(t, bankQ, pool, "umfx", 5*time.Second)
	samples := gatherGaugeSeries(t, reg)
	require.Empty(t, samples, "no gauge series when all queries fail")

	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("provider", providerAddr, "umfx")))
	for _, addr := range subAddrs {
		assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("sub_signer", addr, "umfx")))
	}
}

func TestSignerBalanceCollector_ContextTimeout_DoesNotBlockScrape(t *testing.T) {
	pool := newTestSignerPool(t, 2)

	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	// Use a short timeout so the test runs fast; production passes 5s.
	_, reg := newCollectorForTest(t, bankQ, pool, "umfx", 100*time.Millisecond)

	start := time.Now()
	samples := gatherGaugeSeries(t, reg)
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond, "Collect should have waited until the configured per-scrape timeout fired")
	assert.Less(t, elapsed, 1*time.Second, "Collect must not block past the per-scrape timeout")
	assert.Empty(t, samples, "no gauge series should be emitted on timeout")

	// All 1+N addresses bumped the failures counter.
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("provider", pool.ProviderAddress(), "umfx")))
	for _, addr := range pool.SubSignerAddresses() {
		assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("sub_signer", addr, "umfx")))
	}
}

func TestSignerBalanceCollector_DemotedPool_OnlyProviderSeries(t *testing.T) {
	pool := newTestSignerPool(t, 3)

	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			return &banktypes.QueryBalanceResponse{
				Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(7)},
			}, nil
		},
	}

	_, reg := newCollectorForTest(t, bankQ, pool, "umfx", 5*time.Second)

	pool.DemoteToSingleSigner()

	samples := gatherGaugeSeries(t, reg)
	require.Len(t, samples, 1, "post-demotion, only provider series should be emitted")
	assert.Equal(t, "provider", samples[0].role)
	assert.Equal(t, pool.ProviderAddress(), samples[0].address)
	assert.Equal(t, "", samples[0].index)
	assert.Equal(t, 7.0, samples[0].value)
}

func TestSignerBalanceCollector_RaceFreeUnderConcurrentCollect(t *testing.T) {
	pool := newTestSignerPool(t, 3)

	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			return &banktypes.QueryBalanceResponse{
				Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(1)},
			}, nil
		},
	}

	collector, _ := newCollectorForTest(t, bankQ, pool, "umfx", 5*time.Second)

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := make(chan prometheus.Metric, 16)
			done := make(chan struct{})
			go func() {
				for range ch {
				}
				close(done)
			}()
			collector.Collect(ch)
			close(ch)
			<-done
		}()
	}
	wg.Wait()
}

// TestSignerBalanceCollector_LargeBalance_EmitsGaugeNoFailure guards the
// ENG-252 fix: a balance larger than math.MaxInt64 (the dev provider holds
// ~1e29 umfx) must now emit a healthy gauge series carrying a finite, positive
// value — NOT be misclassified as a query failure. Before the fix, the int64
// round-trip rejected such balances via IsInt64() and dropped the series while
// bumping fred_signer_balance_query_failures_total on every scrape.
func TestSignerBalanceCollector_LargeBalance_EmitsGaugeNoFailure(t *testing.T) {
	pool := newTestSignerPool(t, 0)
	// 1e29 is well above math.MaxInt64 (~9.2e18); this exercises the former
	// overflow path and is in the real dev-provider range.
	large, ok := math.NewIntFromString("100000000000000000000000000000") // 1e29
	require.True(t, ok)

	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			return &banktypes.QueryBalanceResponse{
				Balance: &sdk.Coin{Denom: "umfx", Amount: large},
			}, nil
		},
	}

	_, reg := newCollectorForTest(t, bankQ, pool, "umfx", 5*time.Second)
	samples := gatherGaugeSeries(t, reg)
	require.Len(t, samples, 1, "large balance must emit exactly one provider gauge series")
	assert.Equal(t, "provider", samples[0].role)
	assert.Equal(t, pool.ProviderAddress(), samples[0].address)
	assert.Equal(t, "", samples[0].index)
	assert.Equal(t, "umfx", samples[0].denom)

	v := samples[0].value
	require.False(t, stdmath.IsInf(v, 0), "gauge value must be finite")
	require.False(t, stdmath.IsNaN(v), "gauge value must not be NaN")
	assert.Greater(t, v, 0.0, "gauge value must be positive")
	assert.InEpsilon(t, 1e29, v, 1e-6, "gauge value must approximate the 1e29 balance within float64 rounding")

	// A valid large balance is NOT a query failure: the counter stays at zero.
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("provider", pool.ProviderAddress(), "umfx")))
}

// TestSignerBalanceCollector_NonDefaultDenom_PropagatesThroughQueryAndLabel
// guards the denom-passthrough contract added in commit 25d36e9. Without
// this test, every other test in this file would still pass if
// NewSignerBalanceCollector silently ignored its denom argument and
// hard-coded "umfx" internally (since they all use "umfx").
func TestSignerBalanceCollector_NonDefaultDenom_PropagatesThroughQueryAndLabel(t *testing.T) {
	pool := newTestSignerPool(t, 1)
	const testDenom = "uatom"

	var (
		observedDenomsMu sync.Mutex
		observedDenoms   []string
	)
	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			observedDenomsMu.Lock()
			observedDenoms = append(observedDenoms, in.Denom)
			observedDenomsMu.Unlock()
			return &banktypes.QueryBalanceResponse{
				Balance: &sdk.Coin{Denom: testDenom, Amount: math.NewInt(7)},
			}, nil
		},
	}

	_, reg := newCollectorForTest(t, bankQ, pool, testDenom, 5*time.Second)
	samples := gatherGaugeSeries(t, reg)
	require.Len(t, samples, 2, "should emit provider + 1 sub_signer with the non-default denom")

	// Every Balance request received by the mock must use the configured denom.
	observedDenomsMu.Lock()
	defer observedDenomsMu.Unlock()
	require.NotEmpty(t, observedDenoms)
	for _, d := range observedDenoms {
		assert.Equal(t, testDenom, d, "Balance query must use the configured denom (not a hard-coded umfx)")
	}

	// Every emitted gauge series must carry the configured denom label.
	for _, s := range samples {
		assert.Equal(t, testDenom, s.denom, "gauge series must carry the configured denom label (not a hard-coded umfx)")
	}
}
