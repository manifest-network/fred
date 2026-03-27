package docker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTbfParams(t *testing.T) {
	t.Run("basic rate conversion", func(t *testing.T) {
		rate, burst, limit := tbfParams(10, 0, 0, 0)  // 10 Mbps, auto burst, default latency/HZ
		assert.Equal(t, uint64(10*1_000_000/8), rate) // 1,250,000 bytes/sec
		assert.True(t, burst >= 1500, "burst must be >= MTU")
		assert.True(t, limit > burst, "limit must be > burst")
	})

	t.Run("explicit burst overrides auto-calculation", func(t *testing.T) {
		_, burst, _ := tbfParams(100, 64, 50, 250) // 64KB explicit burst
		assert.Equal(t, uint32(64*1024), burst)
	})

	t.Run("auto burst floor at high rates", func(t *testing.T) {
		// 250 Mbps at HZ=250: rate = 31,250,000 bytes/sec
		// min burst = ceil(31,250,000 / 250) = 125,000 bytes = ~122 KB
		_, burst, _ := tbfParams(250, 0, 50, 250)
		assert.True(t, burst >= 125000, "burst must be >= rate/HZ at high rates, got %d", burst)
	})

	t.Run("auto burst floor 32KB at low rates", func(t *testing.T) {
		// 1 Mbps at HZ=250: rate = 125,000 bytes/sec
		// ceil(125,000 / 250) = 500 bytes, but floor is 32KB
		_, burst, _ := tbfParams(1, 0, 50, 250)
		assert.Equal(t, uint32(32*1024), burst)
	})

	t.Run("burst floor at MTU", func(t *testing.T) {
		// Tiny explicit burst gets raised to MTU
		_, burst, _ := tbfParams(1, 1, 50, 250) // 1KB explicit, below MTU
		assert.True(t, burst >= 1500, "burst must be >= MTU, got %d", burst)
	})

	t.Run("latency default 50ms", func(t *testing.T) {
		rate1, _, limit1 := tbfParams(10, 32, 0, 250)  // latencyMs=0 → default 50ms
		rate2, _, limit2 := tbfParams(10, 32, 50, 250) // latencyMs=50 explicit
		assert.Equal(t, rate1, rate2)
		assert.Equal(t, limit1, limit2)
	})

	t.Run("kernelHZ default 250", func(t *testing.T) {
		_, burst1, _ := tbfParams(100, 0, 50, 0)   // kernelHZ=0 → default 250
		_, burst2, _ := tbfParams(100, 0, 50, 250) // kernelHZ=250 explicit
		assert.Equal(t, burst1, burst2)
	})

	t.Run("higher HZ produces smaller burst", func(t *testing.T) {
		_, burst250, _ := tbfParams(250, 0, 50, 250)
		_, burst1000, _ := tbfParams(250, 0, 50, 1000)
		assert.True(t, burst1000 <= burst250, "higher HZ should produce smaller or equal burst")
	})

	t.Run("limit formula", func(t *testing.T) {
		rate, burst, limit := tbfParams(10, 32, 100, 250)
		// limit = rate * latencyMs / 1000 + burst
		expected := uint32(rate*100/1000) + burst
		assert.Equal(t, expected, limit)
	})

	t.Run("limit saturates at uint32 max for extreme rates", func(t *testing.T) {
		// 100 Gbps with 100ms latency would overflow uint32 without saturation.
		// rate = 100000 * 1_000_000 / 8 = 12,500,000,000 bytes/sec
		// limit = 12,500,000,000 * 100 / 1000 + burst = 1,250,000,000 + burst
		// That fits, but 400 Gbps would not: 50,000,000,000 * 100 / 1000 = 5,000,000,000 > uint32 max
		_, _, limit := tbfParams(400_000, 32, 100, 250)
		assert.Equal(t, ^uint32(0), limit, "limit should saturate to uint32 max, not overflow")
	})
}

func TestIfbDeviceName(t *testing.T) {
	t.Run("truncated to 15 chars", func(t *testing.T) {
		name := ifbDeviceName("abcdef1234567890")
		assert.Equal(t, "ifb-abcdef12345", name) // 4 + 11 = 15
		assert.Len(t, name, 15)
	})

	t.Run("short container ID", func(t *testing.T) {
		name := ifbDeviceName("abc")
		assert.Equal(t, "ifb-abc", name)
	})

	t.Run("exact 11 chars", func(t *testing.T) {
		name := ifbDeviceName("12345678901")
		assert.Equal(t, "ifb-12345678901", name)
		assert.Len(t, name, 15)
	})
}

func TestNoopBandwidthManager(t *testing.T) {
	m := &noopBandwidthManager{}

	assert.NoError(t, m.Apply(context.Background(), "cid", 100, 32, 50, 250))
	assert.NoError(t, m.Remove(context.Background(), "cid"))
	assert.NoError(t, m.Validate())
}

func TestNewBandwidthManager(t *testing.T) {
	t.Run("noop when no bandwidth SKUs", func(t *testing.T) {
		m := newBandwidthManager(nil, false, nil)
		_, ok := m.(*noopBandwidthManager)
		assert.True(t, ok)
	})

	t.Run("tc manager when bandwidth SKUs exist", func(t *testing.T) {
		mockResolver := &mockPIDResolver{}
		m := newBandwidthManager(mockResolver, true, nil)
		_, ok := m.(*tcBandwidthManager)
		assert.True(t, ok)
	})
}

func TestApplyBandwidthLimit_SkipsZeroBandwidth(t *testing.T) {
	applied := false
	mock := &mockBandwidthManager{
		ApplyFn: func(_ context.Context, _ string, _ int64, _ int, _ int, _ int) error {
			applied = true
			return nil
		},
	}
	b := &Backend{
		bandwidth: mock,
		cfg:       DefaultConfig(),
	}

	b.applyBandwidthLimit(context.Background(), "cid", SKUProfile{BandwidthMbps: 0}, nil)
	assert.False(t, applied, "Apply should not be called for BandwidthMbps=0")
}

func TestApplyBandwidthLimit_CallsApply(t *testing.T) {
	var capturedRate int64
	mock := &mockBandwidthManager{
		ApplyFn: func(_ context.Context, _ string, rateMbps int64, _ int, _ int, _ int) error {
			capturedRate = rateMbps
			return nil
		},
	}
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.bandwidth = mock
	b.applyBandwidthLimit(context.Background(), "container-123", SKUProfile{BandwidthMbps: 100}, b.logger)
	assert.Equal(t, int64(100), capturedRate)
}

// mockPIDResolver is a test helper for containerPIDResolver.
type mockPIDResolver struct {
	ContainerPIDFn func(ctx context.Context, containerID string) (int, error)
}

func (m *mockPIDResolver) ContainerPID(ctx context.Context, containerID string) (int, error) {
	if m.ContainerPIDFn != nil {
		return m.ContainerPIDFn(ctx, containerID)
	}
	return 0, nil
}

// mockBandwidthManager is a test helper for bandwidthManager.
type mockBandwidthManager struct {
	ApplyFn    func(ctx context.Context, containerID string, rateMbps int64, burstKB int, latencyMs int, kernelHZ int) error
	RemoveFn   func(ctx context.Context, containerID string) error
	ValidateFn func() error
}

func (m *mockBandwidthManager) Apply(ctx context.Context, containerID string, rateMbps int64, burstKB int, latencyMs int, kernelHZ int) error {
	if m.ApplyFn != nil {
		return m.ApplyFn(ctx, containerID, rateMbps, burstKB, latencyMs, kernelHZ)
	}
	return nil
}

func (m *mockBandwidthManager) Remove(ctx context.Context, containerID string) error {
	if m.RemoveFn != nil {
		return m.RemoveFn(ctx, containerID)
	}
	return nil
}

func (m *mockBandwidthManager) Validate() error {
	if m.ValidateFn != nil {
		return m.ValidateFn()
	}
	return nil
}
