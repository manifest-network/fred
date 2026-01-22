package backend

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPClient_Provision(t *testing.T) {
	// Create test server
	var receivedReq ProvisionRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/provision" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		if err := json.NewDecoder(r.Body).Decode(&receivedReq); err != nil {
			t.Errorf("failed to decode request: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(ProvisionResponse{ProvisionID: "test-123"})
	}))
	defer server.Close()

	// Create client
	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test",
		BaseURL: server.URL,
		Timeout: 5 * time.Second,
	})

	// Test provision
	err := client.Provision(context.Background(), ProvisionRequest{
		LeaseUUID:    "lease-uuid-1",
		Tenant:       "tenant-1",
		ProviderUUID: "provider-1",
		SKU:          "gpu-a100",
		CallbackURL:  "http://fred/callback",
	})

	if err != nil {
		t.Fatalf("Provision() error = %v", err)
	}

	if receivedReq.LeaseUUID != "lease-uuid-1" {
		t.Errorf("LeaseUUID = %q, want %q", receivedReq.LeaseUUID, "lease-uuid-1")
	}
	if receivedReq.SKU != "gpu-a100" {
		t.Errorf("SKU = %q, want %q", receivedReq.SKU, "gpu-a100")
	}
}

func TestHTTPClient_GetInfo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if r.URL.Path == "/info/found-uuid" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(LeaseInfo{
				"host":     "test.example.com",
				"port":     8080,
				"protocol": "https",
			})
			return
		}

		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test",
		BaseURL: server.URL,
	})

	// Test found
	info, err := client.GetInfo(context.Background(), "found-uuid")
	if err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}
	if (*info)["host"] != "test.example.com" {
		t.Errorf("Host = %q, want %q", (*info)["host"], "test.example.com")
	}

	// Test not found
	_, err = client.GetInfo(context.Background(), "not-found-uuid")
	if err != ErrNotProvisioned {
		t.Errorf("GetInfo() error = %v, want ErrNotProvisioned", err)
	}
}

func TestHTTPClient_Deprovision(t *testing.T) {
	var receivedUUID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/deprovision" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		var req map[string]string
		json.NewDecoder(r.Body).Decode(&req)
		receivedUUID = req["lease_uuid"]

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test",
		BaseURL: server.URL,
	})

	err := client.Deprovision(context.Background(), "lease-to-delete")
	if err != nil {
		t.Fatalf("Deprovision() error = %v", err)
	}
	if receivedUUID != "lease-to-delete" {
		t.Errorf("received UUID = %q, want %q", receivedUUID, "lease-to-delete")
	}
}

func TestHTTPClient_ListProvisions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/provisions" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"provisions": []ProvisionInfo{
				{LeaseUUID: "uuid-1", Status: "ready"},
				{LeaseUUID: "uuid-2", Status: "provisioning"},
			},
		})
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test",
		BaseURL: server.URL,
	})

	provisions, err := client.ListProvisions(context.Background())
	if err != nil {
		t.Fatalf("ListProvisions() error = %v", err)
	}
	if len(provisions) != 2 {
		t.Errorf("got %d provisions, want 2", len(provisions))
	}
}

func TestHTTPClient_Name(t *testing.T) {
	client := NewHTTPClient(HTTPClientConfig{
		Name:    "my-backend",
		BaseURL: "http://example.com",
	})

	if client.Name() != "my-backend" {
		t.Errorf("Name() = %q, want %q", client.Name(), "my-backend")
	}
}
