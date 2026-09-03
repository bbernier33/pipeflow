package obs_test

import (
	"testing"
	"time"

	pipeflow "github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/obs"
)

func TestCorrelateResourcesUsesLatestPrecedingFreshSample(t *testing.T) {
	base := time.Now()
	snapshot := obs.Snapshot{
		RecentResources: []obs.ResourceSample{{OccurredAt: base.Add(2 * time.Second), HeapAlloc: 20}, {OccurredAt: base, HeapAlloc: 10}},
		Traces:          []pipeflow.TraceEvent{{OccurredAt: base.Add(time.Second)}, {OccurredAt: base.Add(3 * time.Second)}, {OccurredAt: base.Add(10 * time.Second)}},
	}
	correlations, err := obs.CorrelateResources(snapshot, obs.CorrelationOptions{MaxSampleAge: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if len(correlations) != 2 || correlations[0].Resource.HeapAlloc != 10 || correlations[1].Resource.HeapAlloc != 20 || correlations[1].SampleAge != time.Second {
		t.Fatalf("correlations=%+v", correlations)
	}
	if snapshot.RecentResources[0].HeapAlloc != 20 {
		t.Fatal("correlation mutated snapshot")
	}
}

func TestCorrelateResourcesValidatesFreshness(t *testing.T) {
	if _, err := obs.CorrelateResources(obs.Snapshot{}, obs.CorrelationOptions{MaxSampleAge: -time.Second}); err == nil {
		t.Fatal("expected max age error")
	}
}
