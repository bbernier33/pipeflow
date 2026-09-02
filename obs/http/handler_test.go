package obshttp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	pipeflow "github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/obs"
	obshttp "github.com/bbernier33/pipeflow/obs/http"
)

func observedHandler(t *testing.T, options obshttp.Options) http.Handler {
	t.Helper()
	collector := obs.NewCollector(obs.Options{})
	pipeline := pipeflow.NewPipeline("orders", pipeflow.NewStage("load",
		pipeflow.NewStep("produce", func() (string, error) { return "private-business-value-9f73", nil }),
	)).WithObserver(collector)
	if _, err := pipeline.Run(context.Background()); err != nil {
		t.Fatalf("run observed pipeline: %v", err)
	}
	handler, err := obshttp.NewHandler(collector, options)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	return handler
}

func TestSnapshotEndpointUsesVersionedPayloadFreeJSON(t *testing.T) {
	handler := observedHandler(t, obshttp.Options{})
	request := httptest.NewRequest(http.MethodGet, "/v1/snapshot", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q", got)
	}
	if got := response.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q", got)
	}
	var document struct {
		Schema    string            `json:"schema"`
		Pipelines []json.RawMessage `json:"pipelines"`
		Traces    []json.RawMessage `json:"traces"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &document); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if document.Schema != obshttp.SchemaVersion {
		t.Fatalf("schema = %q", document.Schema)
	}
	if len(document.Pipelines) == 0 || len(document.Traces) == 0 {
		t.Fatalf("expected pipeline and trace data")
	}
	if strings.Contains(response.Body.String(), "private-business-value-9f73") {
		t.Fatal("transport exposed a business payload")
	}
}

func TestHealthAndLivenessEndpoints(t *testing.T) {
	handler := observedHandler(t, obshttp.Options{})
	for _, path := range []string{"/healthz", "/v1/health", "/v1/workers", "/v1/queues"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Errorf("%s status = %d", path, response.Code)
		}
	}
}

func TestAuthorizationAndMethods(t *testing.T) {
	handler := observedHandler(t, obshttp.Options{Authorize: func(r *http.Request) bool {
		return r.Header.Get("Authorization") == "Bearer secret"
	}})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/snapshot", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", response.Code)
	}

	request := httptest.NewRequest(http.MethodGet, "/v1/snapshot", nil)
	request.Header.Set("Authorization", "Bearer secret")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("authorized status = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/snapshot", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d", response.Code)
	}
	if got := response.Header().Get("Allow"); got != "GET, HEAD" {
		t.Fatalf("Allow = %q", got)
	}
}

func TestHeadAndNotFound(t *testing.T) {
	handler := observedHandler(t, obshttp.Options{})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodHead, "/v1/snapshot", nil))
	if response.Code != http.StatusOK || response.Body.Len() != 0 {
		t.Fatalf("HEAD status/body = %d/%d", response.Code, response.Body.Len())
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/missing", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d", response.Code)
	}
}

type panicSource struct{}

func (panicSource) Snapshot() obs.Snapshot { panic("private source failure") }

func TestSourcePanicIsContained(t *testing.T) {
	handler, err := obshttp.NewHandler(panicSource{}, obshttp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/snapshot", nil))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", response.Code)
	}
	if strings.Contains(response.Body.String(), "private source failure") {
		t.Fatal("panic detail leaked")
	}
}

func TestConcurrentRequests(t *testing.T) {
	handler := observedHandler(t, obshttp.Options{})
	var wait sync.WaitGroup
	for i := 0; i < 100; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/snapshot", nil))
			if response.Code != http.StatusOK {
				t.Errorf("status = %d", response.Code)
			}
		}()
	}
	wait.Wait()
}

func TestNewHandlerRejectsNilSource(t *testing.T) {
	if _, err := obshttp.NewHandler(nil, obshttp.Options{}); err == nil {
		t.Fatal("expected nil source error")
	}
}
