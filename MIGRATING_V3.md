# Migrating to Pipeflow v3

V3 uses Go semantic import versioning. Add the module dependency:

```sh
go get github.com/bbernier33/pipeflow/v3@v3.0.0
```

Change imports throughout the application, tests, and adapters:

```go
// Before
import "github.com/bbernier33/pipeflow"

// After
import "github.com/bbernier33/pipeflow/v3"
```

Apply the same change to subpackages, for example
`github.com/bbernier33/pipeflow/v3/obs` and
`github.com/bbernier33/pipeflow/v3/readiness`. Package names and the ordinary
Pipeline/Stage/Step composition model remain the same.

Types from the original module and `/v3` are different Go types. Update any
adapters that exchange Pipeflow Context, Step, Pipeline, report, or policy types;
do not mix the two import paths in those boundaries. Then run:

```sh
go mod tidy
go test ./...
go vet ./...
```

For concurrency-sensitive applications, also run `go test -race ./...` with a
compatible C toolchain. V3 requires Go 1.26.5 or newer.

The original module and v1.0.0 tag remain available for applications staying on
v1. Existing Core semantics are retained while v3 adds optional resilience,
observation, and readiness APIs. Refer to the release notes for scope and
operational limitations.
