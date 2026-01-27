package api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequestTimeoutMiddleware_CompletesWithinTimeout(t *testing.T) {
	timeout := 100 * time.Millisecond
	middleware := requestTimeoutMiddleware(timeout)

	handlerCalled := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	})

	wrapped := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	wrapped.ServeHTTP(rec, req)
	duration := time.Since(start)

	if !handlerCalled {
		t.Error("handler was not called")
	}

	if rec.Code != http.StatusOK {
		t.Errorf("status code = %d, want %d", rec.Code, http.StatusOK)
	}

	if rec.Body.String() != "success" {
		t.Errorf("body = %q, want %q", rec.Body.String(), "success")
	}

	// Should complete quickly, well under the timeout
	if duration > timeout {
		t.Errorf("request took %v, expected to complete well under %v", duration, timeout)
	}
}

func TestRequestTimeoutMiddleware_ExceedsTimeout(t *testing.T) {
	timeout := 50 * time.Millisecond
	middleware := requestTimeoutMiddleware(timeout)

	handlerStarted := make(chan struct{})
	handlerFinished := make(chan struct{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerStarted)
		// Simulate slow handler - wait for context cancellation or longer
		select {
		case <-r.Context().Done():
			// Context was cancelled - expected behavior
		case <-time.After(500 * time.Millisecond):
			// Handler would complete without checking context
		}
		close(handlerFinished)
	})

	wrapped := middleware(handler)

	req := httptest.NewRequest("GET", "/slow", nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	wrapped.ServeHTTP(rec, req)
	duration := time.Since(start)

	// Middleware should return after timeout, not wait for handler
	if duration > timeout+20*time.Millisecond {
		t.Errorf("middleware took %v, expected to return around %v", duration, timeout)
	}

	// Wait for handler to finish (it should detect context cancellation)
	select {
	case <-handlerFinished:
		// Good - handler finished
	case <-time.After(100 * time.Millisecond):
		t.Error("handler did not finish after context cancellation")
	}
}

func TestRequestTimeoutMiddleware_ContextCancellationPropagation(t *testing.T) {
	timeout := 50 * time.Millisecond
	middleware := requestTimeoutMiddleware(timeout)

	var contextErr atomic.Value

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Wait for context to be cancelled
		<-r.Context().Done()
		contextErr.Store(r.Context().Err())
	})

	wrapped := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	wrapped.ServeHTTP(rec, req)

	// Give handler goroutine time to store the error
	time.Sleep(20 * time.Millisecond)

	err := contextErr.Load()
	if err == nil {
		t.Fatal("context error was not captured")
	}

	if err != context.DeadlineExceeded {
		t.Errorf("context error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestRequestTimeoutMiddleware_ZeroTimeout(t *testing.T) {
	// Zero timeout should cause immediate context cancellation
	timeout := 0 * time.Millisecond
	middleware := requestTimeoutMiddleware(timeout)

	var contextCancelled atomic.Bool

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// With zero timeout, context should already be done or very quickly done
		select {
		case <-r.Context().Done():
			contextCancelled.Store(true)
		case <-time.After(50 * time.Millisecond):
			// Context wasn't cancelled quickly
		}
	})

	wrapped := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	wrapped.ServeHTTP(rec, req)
	duration := time.Since(start)

	// Should return very quickly
	if duration > 100*time.Millisecond {
		t.Errorf("request took %v, expected to return quickly with zero timeout", duration)
	}

	// Give handler time to check context
	time.Sleep(20 * time.Millisecond)

	if !contextCancelled.Load() {
		t.Error("context was not cancelled with zero timeout")
	}
}

func TestRequestTimeoutMiddleware_NegativeTimeout(t *testing.T) {
	// Negative timeout behaves like zero timeout - immediate cancellation
	timeout := -10 * time.Millisecond
	middleware := requestTimeoutMiddleware(timeout)

	var contextCancelled atomic.Bool

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			contextCancelled.Store(true)
		case <-time.After(50 * time.Millisecond):
			// Context wasn't cancelled
		}
	})

	wrapped := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	wrapped.ServeHTTP(rec, req)
	duration := time.Since(start)

	// Should return very quickly
	if duration > 100*time.Millisecond {
		t.Errorf("request took %v, expected quick return with negative timeout", duration)
	}

	// Give handler time to check context
	time.Sleep(20 * time.Millisecond)

	if !contextCancelled.Load() {
		t.Error("context was not cancelled with negative timeout")
	}
}

func TestRequestTimeoutMiddleware_HandlerWritesBeforeTimeout(t *testing.T) {
	timeout := 100 * time.Millisecond
	middleware := requestTimeoutMiddleware(timeout)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Write response immediately
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("created"))
		// Then do some work (but still complete within timeout)
		time.Sleep(10 * time.Millisecond)
	})

	wrapped := middleware(handler)

	req := httptest.NewRequest("POST", "/create", nil)
	rec := httptest.NewRecorder()

	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("status code = %d, want %d", rec.Code, http.StatusCreated)
	}

	if rec.Body.String() != "created" {
		t.Errorf("body = %q, want %q", rec.Body.String(), "created")
	}
}

func TestRequestTimeoutMiddleware_PreservesRequestContext(t *testing.T) {
	timeout := 100 * time.Millisecond
	middleware := requestTimeoutMiddleware(timeout)

	type ctxKey string
	const testKey ctxKey = "test-key"
	const testValue = "test-value"

	var capturedValue atomic.Value

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that original context values are preserved
		if v := r.Context().Value(testKey); v != nil {
			capturedValue.Store(v)
		}
		w.WriteHeader(http.StatusOK)
	})

	wrapped := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	// Add a value to the request context
	ctx := context.WithValue(req.Context(), testKey, testValue)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	captured := capturedValue.Load()
	if captured != testValue {
		t.Errorf("context value = %v, want %v", captured, testValue)
	}
}

func TestRequestTimeoutMiddleware_ConcurrentRequests(t *testing.T) {
	timeout := 100 * time.Millisecond
	middleware := requestTimeoutMiddleware(timeout)

	var completedCount atomic.Int32

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Small random-ish delay based on request
		time.Sleep(10 * time.Millisecond)
		completedCount.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	wrapped := middleware(handler)

	const numRequests = 50
	done := make(chan struct{}, numRequests)

	for i := 0; i < numRequests; i++ {
		go func() {
			req := httptest.NewRequest("GET", "/concurrent", nil)
			rec := httptest.NewRecorder()
			wrapped.ServeHTTP(rec, req)
			done <- struct{}{}
		}()
	}

	// Wait for all requests to complete
	for i := 0; i < numRequests; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for request %d", i)
		}
	}

	if completedCount.Load() != numRequests {
		t.Errorf("completed count = %d, want %d", completedCount.Load(), numRequests)
	}
}

func TestRequestTimeoutMiddleware_SlowHandlerDoesNotBlockOthers(t *testing.T) {
	timeout := 50 * time.Millisecond
	middleware := requestTimeoutMiddleware(timeout)

	slowHandlerStarted := make(chan struct{})
	slowHandlerDone := make(chan struct{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			close(slowHandlerStarted)
			// This handler ignores context cancellation (bad practice, but tests middleware)
			time.Sleep(200 * time.Millisecond)
			close(slowHandlerDone)
			return
		}
		// Fast handler
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("fast"))
	})

	wrapped := middleware(handler)

	// Start slow request
	go func() {
		req := httptest.NewRequest("GET", "/slow", nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)
	}()

	// Wait for slow handler to start
	<-slowHandlerStarted

	// Fast request should complete quickly even while slow request is running
	req := httptest.NewRequest("GET", "/fast", nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	wrapped.ServeHTTP(rec, req)
	duration := time.Since(start)

	if duration > timeout {
		t.Errorf("fast request took %v, expected to complete quickly", duration)
	}

	if rec.Code != http.StatusOK {
		t.Errorf("fast request status = %d, want %d", rec.Code, http.StatusOK)
	}

	// Wait for slow handler to finish to avoid test pollution
	<-slowHandlerDone
}

// Tests for readBodyWithContext

func TestReadBodyWithContext_Success(t *testing.T) {
	ctx := context.Background()
	data := []byte("test payload data")
	body := bytes.NewReader(data)

	result, err := readBodyWithContext(ctx, body)
	if err != nil {
		t.Fatalf("readBodyWithContext() error = %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Errorf("readBodyWithContext() = %q, want %q", result, data)
	}
}

func TestReadBodyWithContext_EmptyBody(t *testing.T) {
	ctx := context.Background()
	body := bytes.NewReader([]byte{})

	result, err := readBodyWithContext(ctx, body)
	if err != nil {
		t.Fatalf("readBodyWithContext() error = %v", err)
	}

	if len(result) != 0 {
		t.Errorf("readBodyWithContext() length = %d, want 0", len(result))
	}
}

func TestReadBodyWithContext_LargeBody(t *testing.T) {
	ctx := context.Background()
	// Create a body larger than the chunk size (32KB)
	data := make([]byte, 100*1024) // 100KB
	for i := range data {
		data[i] = byte(i % 256)
	}
	body := bytes.NewReader(data)

	result, err := readBodyWithContext(ctx, body)
	if err != nil {
		t.Fatalf("readBodyWithContext() error = %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Errorf("readBodyWithContext() data mismatch, got %d bytes, want %d bytes", len(result), len(data))
	}
}

func TestReadBodyWithContext_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	data := []byte("test payload data")
	body := bytes.NewReader(data)

	_, err := readBodyWithContext(ctx, body)
	if err == nil {
		t.Fatal("readBodyWithContext() error = nil, want context.Canceled")
	}

	if err != context.Canceled {
		t.Errorf("readBodyWithContext() error = %v, want %v", err, context.Canceled)
	}
}

func TestReadBodyWithContext_ContextCancelledDuringRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create a slow reader that allows us to cancel mid-read
	slowReader := &slowReader{
		data:       make([]byte, 100*1024), // 100KB
		chunkSize:  1024,                   // 1KB per read
		onRead:     func() { time.Sleep(5 * time.Millisecond) },
		cancelFunc: cancel,
		cancelAt:   10, // Cancel after 10 reads
	}

	_, err := readBodyWithContext(ctx, slowReader)
	if err == nil {
		t.Fatal("readBodyWithContext() error = nil, want context.Canceled")
	}

	if err != context.Canceled {
		t.Errorf("readBodyWithContext() error = %v, want %v", err, context.Canceled)
	}
}

func TestReadBodyWithContext_ReadError(t *testing.T) {
	ctx := context.Background()
	expectedErr := io.ErrUnexpectedEOF
	body := &errorReader{err: expectedErr}

	_, err := readBodyWithContext(ctx, body)
	if err == nil {
		t.Fatal("readBodyWithContext() error = nil, want error")
	}

	if err != expectedErr {
		t.Errorf("readBodyWithContext() error = %v, want %v", err, expectedErr)
	}
}

// slowReader is a test helper that reads slowly and can cancel context mid-read
type slowReader struct {
	data       []byte
	pos        int
	chunkSize  int
	onRead     func()
	cancelFunc context.CancelFunc
	cancelAt   int
	readCount  int
}

func (r *slowReader) Read(p []byte) (n int, err error) {
	if r.onRead != nil {
		r.onRead()
	}

	r.readCount++
	if r.cancelFunc != nil && r.readCount >= r.cancelAt {
		r.cancelFunc()
	}

	if r.pos >= len(r.data) {
		return 0, io.EOF
	}

	end := r.pos + r.chunkSize
	if end > len(r.data) {
		end = len(r.data)
	}

	n = copy(p, r.data[r.pos:end])
	r.pos += n
	return n, nil
}

// errorReader always returns an error
type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}
