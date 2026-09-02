package history_test

import (
	"fmt"
	"os"

	"github.com/bbernier33/pipeflow/obs"
	"github.com/bbernier33/pipeflow/obs/history"
)

func ExampleStore() {
	dir, _ := os.MkdirTemp("", "pipeflow-history-example")
	defer os.RemoveAll(dir)
	store, _ := history.Open(dir, history.Options{MaxSnapshots: 100})
	collector := obs.NewCollector(obs.Options{})

	_ = store.Append(collector.Snapshot())
	latest, ok, _ := store.Latest()
	fmt.Println(ok, latest.CapturedAt.IsZero())
	// Output: true false
}
