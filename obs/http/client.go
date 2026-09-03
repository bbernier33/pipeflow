package obshttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bbernier33/pipeflow/obs"
)

type OperationalSnapshot struct {
	Schema           string                `json:"schema"`
	CapturedAt       time.Time             `json:"captured_at"`
	Pipelines        []obs.PipelineView    `json:"pipelines"`
	Runs             []obs.RunView         `json:"runs"`
	Errors           []obs.ErrorGroup      `json:"errors"`
	Workers          []obs.WorkerView      `json:"workers"`
	Queues           []obs.QueueView       `json:"queues"`
	Recoveries       []obs.RecoveryView    `json:"recoveries"`
	Circuits         []obs.CircuitView     `json:"circuits"`
	Idempotency      []obs.IdempotencyView `json:"idempotency"`
	Resources        obs.ResourceView      `json:"resources"`
	DroppedTraces    uint64                `json:"dropped_traces"`
	DroppedMetrics   uint64                `json:"dropped_metrics"`
	DroppedProfiles  uint64                `json:"dropped_profiles"`
	DroppedResources uint64                `json:"dropped_resources"`
}

type Dashboard struct {
	RetrievedAt time.Time
	Snapshot    OperationalSnapshot
	Analysis    obs.Analysis
}

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
	Token      string
}

func (c Client) Fetch(ctx context.Context) (Dashboard, error) {
	if ctx == nil {
		return Dashboard{}, errors.New("pipeflow obs http: nil Client context")
	}
	base, err := c.baseURL()
	if err != nil {
		return Dashboard{}, err
	}
	var document struct {
		Schema   string              `json:"schema"`
		Snapshot OperationalSnapshot `json:"snapshot"`
		Analysis obs.Analysis        `json:"analysis"`
	}
	if err := c.get(ctx, base+"/v1/dashboard", &document); err != nil {
		return Dashboard{}, err
	}
	if document.Schema != SchemaVersion || document.Snapshot.Schema != SchemaVersion {
		return Dashboard{}, errors.New("pipeflow obs http: unsupported response schema")
	}
	return Dashboard{RetrievedAt: time.Now(), Snapshot: document.Snapshot, Analysis: document.Analysis}, nil
}

func (c Client) baseURL() (string, error) {
	value := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", errors.New("pipeflow obs http: Client BaseURL must be an absolute HTTP URL")
	}
	return value, nil
}

func (c Client) get(ctx context.Context, target string, destination any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("pipeflow obs http: create request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if c.Token != "" {
		request.Header.Set("Authorization", "Bearer "+c.Token)
	}
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("pipeflow obs http: request %s: %w", request.URL.Path, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("pipeflow obs http: request %s returned %s", request.URL.Path, response.Status)
	}
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		return fmt.Errorf("pipeflow obs http: decode %s: %w", request.URL.Path, err)
	}
	return nil
}
