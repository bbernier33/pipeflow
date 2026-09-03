package obshttp_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bbernier33/pipeflow/obs"
	obshttp "github.com/bbernier33/pipeflow/obs/http"
)

func TestClientFetchesAuthenticatedDashboard(t *testing.T) {
	collector := obs.NewCollector(obs.Options{})
	handler, err := obshttp.NewHandler(collector, obshttp.Options{Authorize: func(r *http.Request) bool { return r.Header.Get("Authorization") == "Bearer token" }})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	dashboard, err := (obshttp.Client{BaseURL: server.URL, HTTPClient: server.Client(), Token: "token"}).Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if dashboard.Snapshot.Schema != obshttp.SchemaVersion || dashboard.RetrievedAt.IsZero() {
		t.Fatalf("dashboard=%+v", dashboard)
	}
}

func TestClientValidatesURLStatusAndSchema(t *testing.T) {
	if _, err := (obshttp.Client{BaseURL: "relative"}).Fetch(context.Background()); err == nil {
		t.Fatal("expected URL error")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"schema":"future","snapshot":{"schema":"future"},"analysis":{}}`))
	}))
	defer server.Close()
	if _, err := (obshttp.Client{BaseURL: server.URL, HTTPClient: server.Client()}).Fetch(context.Background()); err == nil {
		t.Fatal("expected schema error")
	}
	if _, err := (obshttp.Client{BaseURL: server.URL}).Fetch(nil); err == nil {
		t.Fatal("expected context error")
	}
}
