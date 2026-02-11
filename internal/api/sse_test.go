package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/testutil"
)

func TestSSEBroker_SubscribeAndPublish(t *testing.T) {
	broker := NewSSEBroker()
	ch := broker.Subscribe("lease-1")

	event := SSEEvent{
		LeaseUUID: "lease-1",
		Status:    "ready",
		Timestamp: time.Now().Format(time.RFC3339),
	}
	broker.Publish(event)

	select {
	case received := <-ch:
		assert.Equal(t, "lease-1", received.LeaseUUID)
		assert.Equal(t, "ready", received.Status)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestSSEBroker_MultipleClientsForSameLease(t *testing.T) {
	broker := NewSSEBroker()
	ch1 := broker.Subscribe("lease-1")
	ch2 := broker.Subscribe("lease-1")

	event := SSEEvent{
		LeaseUUID: "lease-1",
		Status:    "failed",
		Error:     "container exited",
		Timestamp: time.Now().Format(time.RFC3339),
	}
	broker.Publish(event)

	for _, ch := range []<-chan SSEEvent{ch1, ch2} {
		select {
		case received := <-ch:
			assert.Equal(t, "lease-1", received.LeaseUUID)
			assert.Equal(t, "failed", received.Status)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for event on one of the clients")
		}
	}
}

func TestSSEBroker_UnsubscribeStopsDelivery(t *testing.T) {
	broker := NewSSEBroker()
	ch := broker.Subscribe("lease-1")

	broker.Unsubscribe("lease-1", ch)

	// Publish after unsubscribe — should not be received (channel is closed).
	broker.Publish(SSEEvent{
		LeaseUUID: "lease-1",
		Status:    "ready",
	})

	// Channel should be closed.
	_, ok := <-ch
	assert.False(t, ok, "expected channel to be closed after unsubscribe")
}

func TestSSEBroker_SlowClientDropsEvents(t *testing.T) {
	broker := NewSSEBroker()
	ch := broker.Subscribe("lease-1")

	// Fill the buffer (sseChannelBuffer = 16).
	for i := 0; i < sseChannelBuffer+5; i++ {
		broker.Publish(SSEEvent{
			LeaseUUID: "lease-1",
			Status:    "ready",
		})
	}

	// Should not panic or block. We should get exactly sseChannelBuffer events.
	count := 0
	for {
		select {
		case <-ch:
			count++
		default:
			goto done
		}
	}
done:
	assert.Equal(t, sseChannelBuffer, count, "expected exactly buffer-size events, extras should be dropped")
}

func TestSSEBroker_DifferentLeasesAreIsolated(t *testing.T) {
	broker := NewSSEBroker()
	ch1 := broker.Subscribe("lease-1")
	ch2 := broker.Subscribe("lease-2")

	broker.Publish(SSEEvent{
		LeaseUUID: "lease-1",
		Status:    "ready",
	})

	// ch1 should get the event.
	select {
	case received := <-ch1:
		assert.Equal(t, "lease-1", received.LeaseUUID)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event on ch1")
	}

	// ch2 should NOT get the event.
	select {
	case <-ch2:
		t.Fatal("ch2 should not receive events for lease-1")
	case <-time.After(50 * time.Millisecond):
		// Expected: no event.
	}
}

func TestSSEBroker_UnsubscribeNonexistentLease(t *testing.T) {
	broker := NewSSEBroker()
	ch := make(chan SSEEvent)

	// Should not panic.
	require.NotPanics(t, func() {
		broker.Unsubscribe("nonexistent", ch)
	})
}

func TestSSEBroker_PublishToNoSubscribers(t *testing.T) {
	broker := NewSSEBroker()

	// Should not panic.
	require.NotPanics(t, func() {
		broker.Publish(SSEEvent{
			LeaseUUID: "lease-1",
			Status:    "ready",
		})
	})
}

func TestStreamLeaseEvents_RequiresAuth(t *testing.T) {
	broker := NewSSEBroker()
	h := &Handlers{
		sseBroker:    broker,
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
	}

	req := httptest.NewRequest("GET", "/v1/leases/"+testutil.ValidUUID1+"/events", nil)
	req.SetPathValue("lease_uuid", testutil.ValidUUID1)
	// No Authorization header.

	rec := httptest.NewRecorder()
	h.StreamLeaseEvents(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestStreamLeaseEvents_Returns501WhenBrokerNil(t *testing.T) {
	h := &Handlers{
		sseBroker:    nil,
		providerUUID: testutil.ValidUUID1,
		bech32Prefix: "manifest",
	}

	req := httptest.NewRequest("GET", "/v1/leases/"+testutil.ValidUUID1+"/events", nil)
	req.SetPathValue("lease_uuid", testutil.ValidUUID1)

	rec := httptest.NewRecorder()
	h.StreamLeaseEvents(rec, req)

	assert.Equal(t, http.StatusNotImplemented, rec.Code)
}

func TestStreamLeaseEvents_ReceivesEvent(t *testing.T) {
	broker := NewSSEBroker()
	kp := testutil.NewTestKeyPair("test-tenant")
	leaseUUID := testutil.ValidUUID1
	providerUUID := testutil.ValidUUID2
	validToken := testutil.CreateTestToken(kp, leaseUUID, time.Now())

	chainClient := &mockChainClient{
		getLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         leaseUUID,
				Tenant:       kp.Address,
				ProviderUuid: providerUUID,
				State:        billingtypes.LEASE_STATE_ACTIVE,
			}, nil
		},
	}

	h := &Handlers{
		client:       chainClient,
		sseBroker:    broker,
		providerUUID: providerUUID,
		bech32Prefix: "manifest",
	}

	// Use an httptest.Server so we get real SSE behavior (streaming).
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/leases/{lease_uuid}/events", h.StreamLeaseEvents)
	server := httptest.NewServer(mux)
	defer server.Close()

	// Connect as SSE client.
	req, err := http.NewRequest("GET", server.URL+"/v1/leases/"+leaseUUID+"/events", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+validToken)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	// Publish an event after a short delay to ensure the client is subscribed.
	go func() {
		time.Sleep(100 * time.Millisecond)
		broker.Publish(SSEEvent{
			LeaseUUID: leaseUUID,
			Status:    "ready",
			Timestamp: time.Now().Format(time.RFC3339),
		})
	}()

	// Read the first SSE data line.
	scanner := bufio.NewScanner(resp.Body)
	var sseData string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			sseData = strings.TrimPrefix(line, "data: ")
			break
		}
	}
	require.NotEmpty(t, sseData, "expected to receive SSE data line")

	var event SSEEvent
	require.NoError(t, json.Unmarshal([]byte(sseData), &event))
	assert.Equal(t, leaseUUID, event.LeaseUUID)
	assert.Equal(t, "ready", event.Status)
}
