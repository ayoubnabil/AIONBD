package aionbd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSearchCollectionTopKOmitsLimitWhenNil(t *testing.T) {
	t.Parallel()

	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/collections/demo/search/topk" {
			t.Fatalf("unexpected path: %s", request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		writeJSON(t, writer, map[string]any{
			"metric": "dot",
			"mode":   "auto",
			"hits":   []map[string]any{},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)
	_, err := client.SearchCollectionTopK(context.Background(), "demo", []float32{1, 2}, &SearchTopKOptions{})
	if err != nil {
		t.Fatalf("search top-k failed: %v", err)
	}

	if _, found := captured["limit"]; found {
		t.Fatalf("expected no limit field, got: %#v", captured["limit"])
	}
}

func TestSearchCollectionTopKDefaultsLimitToTenWhenOptionsNil(t *testing.T) {
	t.Parallel()

	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if err := json.NewDecoder(request.Body).Decode(&captured); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		writeJSON(t, writer, map[string]any{
			"metric": "dot",
			"mode":   "auto",
			"hits":   []map[string]any{},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)
	_, err := client.SearchCollectionTopK(context.Background(), "demo", []float32{1, 2}, nil)
	if err != nil {
		t.Fatalf("search top-k failed: %v", err)
	}

	limit, ok := captured["limit"].(float64)
	if !ok || int(limit) != 10 {
		t.Fatalf("expected default limit=10, got: %#v", captured["limit"])
	}
}

func TestListPointsOmitLimitInCursorMode(t *testing.T) {
	t.Parallel()

	var capturedQuery string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		capturedQuery = request.URL.RawQuery
		writeJSON(t, writer, map[string]any{
			"points":        []map[string]any{{"id": 1}},
			"total":         1,
			"next_offset":   nil,
			"next_after_id": nil,
		})
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)
	_, err := client.ListPoints(context.Background(), "demo", &ListPointsOptions{
		AfterID: Uint64Ptr(7),
	})
	if err != nil {
		t.Fatalf("list points failed: %v", err)
	}
	if strings.Contains(capturedQuery, "limit=") {
		t.Fatalf("expected no limit query parameter, got: %s", capturedQuery)
	}
	if capturedQuery != "after_id=7" {
		t.Fatalf("unexpected query: %s", capturedQuery)
	}
}

func TestListPointsRejectsMixedOffsetAndAfterID(t *testing.T) {
	t.Parallel()

	client := NewClient("http://unit.test", nil)
	_, err := client.ListPoints(context.Background(), "demo", &ListPointsOptions{
		Offset:  1,
		AfterID: Uint64Ptr(3),
	})
	if err == nil || !strings.Contains(err.Error(), "offset must be 0") {
		t.Fatalf("expected mixed mode error, got: %v", err)
	}
}

func TestClientAddsAuthHeaders(t *testing.T) {
	t.Parallel()

	var receivedAPIKey string
	var receivedBearer string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedAPIKey = request.Header.Get("x-api-key")
		receivedBearer = request.Header.Get("Authorization")
		writeJSON(t, writer, map[string]any{"status": "live", "uptime_ms": 1})
	}))
	defer server.Close()

	client := NewClient(server.URL, &ClientOptions{
		APIKey:      "key-a",
		BearerToken: "token-a",
	})
	if _, err := client.Live(context.Background()); err != nil {
		t.Fatalf("live failed: %v", err)
	}
	if receivedAPIKey != "key-a" {
		t.Fatalf("unexpected api key header: %q", receivedAPIKey)
	}
	if receivedBearer != "Bearer token-a" {
		t.Fatalf("unexpected bearer header: %q", receivedBearer)
	}
}

func TestMetricsPrometheusReturnsRawText(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "text/plain")
		_, _ = writer.Write([]byte("aionbd_collections 3\n"))
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)
	payload, err := client.MetricsPrometheus(context.Background())
	if err != nil {
		t.Fatalf("metrics prometheus failed: %v", err)
	}
	if payload != "aionbd_collections 3\n" {
		t.Fatalf("unexpected payload: %q", payload)
	}
}

func TestHTTPErrorExposesStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusBadRequest)
		_, _ = writer.Write([]byte(`{"error":"boom"}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)
	_, err := client.Live(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	requestErr, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected *Error, got %T", err)
	}
	if requestErr.Status != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", requestErr.Status)
	}
}

func TestInvalidJSONResponseExposesError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("not-json"))
	}))
	defer server.Close()

	client := NewClient(server.URL, nil)
	_, err := client.Live(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	requestErr, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected *Error, got %T", err)
	}
	if !strings.Contains(requestErr.Error(), "invalid JSON response") {
		t.Fatalf("unexpected error: %v", requestErr)
	}
}

func writeJSON(t *testing.T, writer http.ResponseWriter, payload map[string]any) {
	t.Helper()
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(payload); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
