package history_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pipeflow "github.com/bbernier33/pipeflow"
	"github.com/bbernier33/pipeflow/obs"
	"github.com/bbernier33/pipeflow/obs/history"
)

func snapshotAt(captured time.Time, pipeline string) obs.Snapshot {
	return obs.Snapshot{CapturedAt: captured, Pipelines: []obs.PipelineView{{Name: pipeline}}}
}

func TestStorePersistsQueriesAndReopens(t *testing.T) {
	dir := t.TempDir()
	store, err := history.Open(dir, history.Options{MaxSnapshots: 10, Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for i := range 3 {
		if err := store.Append(snapshotAt(base.Add(time.Duration(i)*time.Minute), string(rune('a'+i)))); err != nil {
			t.Fatal(err)
		}
	}
	values, err := store.Query(history.Query{From: base.Add(time.Minute), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0].Pipelines[0].Name != "b" {
		t.Fatalf("values=%+v", values)
	}
	latest, ok, err := store.Latest()
	if err != nil || !ok || latest.Pipelines[0].Name != "c" {
		t.Fatalf("latest=%+v ok=%v err=%v", latest, ok, err)
	}

	reopened, err := history.Open(dir, history.Options{MaxSnapshots: 10})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := reopened.Stats()
	if err != nil || stats.Snapshots != 3 || !stats.Oldest.Equal(base) || !stats.Newest.Equal(base.Add(2*time.Minute)) {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}

func TestStoreAppliesCountAndAgeRetention(t *testing.T) {
	store, err := history.Open(t.TempDir(), history.Options{MaxSnapshots: 2, MaxAge: 90 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 4 {
		if err := store.Append(snapshotAt(base.Add(time.Duration(i)*time.Hour), "p")); err != nil {
			t.Fatal(err)
		}
	}
	values, err := store.Query(history.Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || !values[0].CapturedAt.Equal(base.Add(2*time.Hour)) {
		t.Fatalf("values=%+v", values)
	}
}

func TestStoreNeverPersistsBusinessPayload(t *testing.T) {
	collector := obs.NewCollector(obs.Options{})
	pipeline := pipeflow.NewPipeline("private", pipeflow.NewStage("s", pipeflow.NewStep("produce", func() (string, error) { return "secret-payload-1f9a", nil }))).WithObserver(collector)
	if _, err := pipeline.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store, err := history.Open(dir, history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(collector.Snapshot()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret-payload-1f9a") {
		t.Fatal("business payload persisted")
	}
}

func TestStoreReportsCorruptAndUnknownRecords(t *testing.T) {
	dir := t.TempDir()
	store, err := history.Open(dir, history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000001-00000000000000000001.json"), []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Query(history.Query{}); err == nil || !strings.Contains(err.Error(), "decode record") {
		t.Fatalf("err=%v", err)
	}
}

func TestStoreSupportsConcurrentAppendAndQuery(t *testing.T) {
	store, err := history.Open(t.TempDir(), history.Options{MaxSnapshots: 200})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	var wait sync.WaitGroup
	for i := range 50 {
		wait.Add(2)
		go func(i int) {
			defer wait.Done()
			if err := store.Append(snapshotAt(base.Add(time.Duration(i)*time.Nanosecond), "p")); err != nil {
				t.Error(err)
			}
		}(i)
		go func() {
			defer wait.Done()
			if _, err := store.Query(history.Query{Limit: 5}); err != nil {
				t.Error(err)
			}
		}()
	}
	wait.Wait()
	stats, err := store.Stats()
	if err != nil || stats.Snapshots != 50 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}

func TestStoreValidatesInputs(t *testing.T) {
	if _, err := history.Open("", history.Options{}); err == nil {
		t.Fatal("expected directory error")
	}
	store, err := history.Open(t.TempDir(), history.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(obs.Snapshot{}); err == nil {
		t.Fatal("expected capture time error")
	}
	if _, err := store.Query(history.Query{Limit: -1}); err == nil {
		t.Fatal("expected limit error")
	}
}
