package obshttp_test

import (
	"log"
	"net/http"

	"github.com/bbernier33/pipeflow/obs"
	obshttp "github.com/bbernier33/pipeflow/obs/http"
)

func ExampleNewHandler() {
	collector := obs.NewCollector(obs.Options{})
	handler, err := obshttp.NewHandler(collector, obshttp.Options{
		Authorize: func(r *http.Request) bool {
			return r.Header.Get("Authorization") == "Bearer example-token"
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/operations/", http.StripPrefix("/operations", handler))
	// Pass collector to Pipeline.WithObserver and mux to your existing server.
}
