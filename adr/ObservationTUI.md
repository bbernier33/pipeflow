# Observation TUI

## Decision

`cmd/pipeflow-obs` is a disconnected, read-only terminal dashboard over the
versioned Observation HTTP API. `obs/http.Client` owns request construction,
Bearer authorization, status handling, schema validation, and Snapshot plus
Explain retrieval. `obs/tui` owns deterministic rendering and refresh behavior.

The dashboard summarizes Pipeline health and active runs, Workers and queues,
Go runtime resources, Recovery/Circuit/Idempotency state, derived Explain
findings, and evidence-retention drops. It creates no connection to Pipeflow
Core and has no execution-control methods.

The default interactive mode refreshes periodically, preserves operation across
transient fetch failures, and shuts down through context cancellation. The CLI
maps Ctrl+C and SIGTERM to cancellation. One-shot output supports scripts and
tests. ANSI color and screen clearing are independently optional.

## Security and dependency boundary

Bearer tokens are read from an environment variable selected by `-token-env`;
there is no token command-line flag that would expose a credential through
process listings. The embedding application remains responsible for TLS,
authorization policy, bind address, and network exposure.

The implementation uses the Go standard library and deliberately avoids a
terminal-framework dependency in this first slice. Mouse input, interactive
drill-down, history graphs, terminal-width adaptation, WebSocket streaming,
and execution control remain deferred.
