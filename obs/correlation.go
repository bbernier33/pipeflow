package obs

import (
	"errors"
	"sort"
	"time"

	pipeflow "github.com/bbernier33/pipeflow"
)

type CorrelationOptions struct {
	MaxSampleAge time.Duration
}

type ResourceCorrelation struct {
	Event     pipeflow.TraceEvent `json:"event"`
	Resource  ResourceSample      `json:"resource"`
	SampleAge time.Duration       `json:"sample_age_ns"`
}

// CorrelateResources associates each Trace event with the newest resource
// sample at or before that event. Association is temporal evidence, not proof
// that execution caused the resource values.
func CorrelateResources(snapshot Snapshot, options CorrelationOptions) ([]ResourceCorrelation, error) {
	if options.MaxSampleAge < 0 {
		return nil, errors.New("pipeflow obs: correlation max sample age cannot be negative")
	}
	if options.MaxSampleAge == 0 {
		options.MaxSampleAge = time.Minute
	}
	samples := append([]ResourceSample(nil), snapshot.RecentResources...)
	sort.SliceStable(samples, func(i, j int) bool { return samples[i].OccurredAt.Before(samples[j].OccurredAt) })
	result := make([]ResourceCorrelation, 0, len(snapshot.Traces))
	for _, event := range snapshot.Traces {
		index := sort.Search(len(samples), func(i int) bool { return samples[i].OccurredAt.After(event.OccurredAt) }) - 1
		if index < 0 {
			continue
		}
		age := event.OccurredAt.Sub(samples[index].OccurredAt)
		if age > options.MaxSampleAge {
			continue
		}
		result = append(result, ResourceCorrelation{Event: event, Resource: samples[index], SampleAge: age})
	}
	return result, nil
}
