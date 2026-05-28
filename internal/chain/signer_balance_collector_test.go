package chain

import (
	"context"
	"errors"
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

// gatherGaugeSeries gathers all `fred_signer_balance_umfx` series from the
// given registry and returns one entry per (label-set → value).
type gaugeSample struct {
	role    string
	address string
	index   string
	value   float64
}

func gatherGaugeSeries(t *testing.T, reg *prometheus.Registry) []gaugeSample {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)

	var out []gaugeSample
	for _, fam := range families {
		if fam.GetName() != "fred_signer_balance_umfx" {
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
func newCollectorForTest(t *testing.T, bankQ bankQuerier, pool *SignerPool, timeout time.Duration) (*SignerBalanceCollector, *prometheus.Registry) {
	t.Helper()
	metrics.SignerBalanceQueryFailures.Reset()
	collector := NewSignerBalanceCollector(bankQ, pool, "umfx", timeout)
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

	_, reg := newCollectorForTest(t, bankQ, pool, 5*time.Second)
	samples := gatherGaugeSeries(t, reg)
	require.Len(t, samples, 1, "should emit only the provider series in single-signer mode")
	assert.Equal(t, "provider", samples[0].role)
	assert.Equal(t, pool.ProviderAddress(), samples[0].address)
	assert.Equal(t, "", samples[0].index)
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

	_, reg := newCollectorForTest(t, bankQ, pool, 5*time.Second)
	samples := gatherGaugeSeries(t, reg)
	require.Len(t, samples, 4, "should emit 1 provider + 3 sub_signer series")

	byKey := make(map[string]gaugeSample, len(samples))
	for _, s := range samples {
		byKey[s.role+"|"+s.index] = s
	}

	require.Contains(t, byKey, "provider|")
	assert.Equal(t, providerAddr, byKey["provider|"].address)
	assert.Equal(t, 1000.0, byKey["provider|"].value)

	for i, addr := range subAddrs {
		key := "sub_signer|" + strconv.Itoa(i)
		require.Contains(t, byKey, key, "missing series for slice index %d", i)
		assert.Equal(t, addr, byKey[key].address)
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

	collector, reg := newCollectorForTest(t, bankQ, pool, 5*time.Second)

	// First scrape: 3 series emitted (no index=1), counter at 1 for that role/address.
	samples := gatherGaugeSeries(t, reg)
	require.Len(t, samples, 3, "failing series should be dropped")
	for _, s := range samples {
		assert.NotEqual(t, "1", s.index, "series for slice index 1 should not be emitted")
		assert.NotEqual(t, failAddr, s.address, "series for failing address should not be emitted")
	}
	// Provider still present
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("sub_signer", failAddr)))
	// No counter increment for provider or for other sub-signers.
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("provider", providerAddr)))
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("sub_signer", subAddrs[0])))
	assert.Equal(t, 0.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("sub_signer", subAddrs[2])))

	// Second scrape: same failure → counter at 2, still no series for index 1.
	_ = collector // keep reference
	samples2 := gatherGaugeSeries(t, reg)
	require.Len(t, samples2, 3)
	assert.Equal(t, 2.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("sub_signer", failAddr)))
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

	_, reg := newCollectorForTest(t, bankQ, pool, 5*time.Second)
	samples := gatherGaugeSeries(t, reg)
	require.Empty(t, samples, "no gauge series when all queries fail")

	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("provider", providerAddr)))
	for _, addr := range subAddrs {
		assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("sub_signer", addr)))
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
	_, reg := newCollectorForTest(t, bankQ, pool, 100*time.Millisecond)

	start := time.Now()
	samples := gatherGaugeSeries(t, reg)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 6*time.Second, "Collect must not block past the per-scrape timeout")
	assert.Empty(t, samples, "no gauge series should be emitted on timeout")

	// All 1+N addresses bumped the failures counter.
	assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("provider", pool.ProviderAddress())))
	for _, addr := range pool.SubSignerAddresses() {
		assert.Equal(t, 1.0, promtestutil.ToFloat64(metrics.SignerBalanceQueryFailures.WithLabelValues("sub_signer", addr)))
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

	_, reg := newCollectorForTest(t, bankQ, pool, 5*time.Second)

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

	collector, _ := newCollectorForTest(t, bankQ, pool, 5*time.Second)

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
