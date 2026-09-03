# Observation HTTP Transport

## Decision

Pipeflow provides `pipeflow/obs/http`, a small read-only `http.Handler` over an
`obs.Source`. The package depends on the Observation package, never on
execution internals, and uses only the Go standard library.

The transport exposes:

- `GET` and `HEAD /healthz` for handler liveness;
- `GET` and `HEAD /v1/health` for derived Pipeline health;
- `GET` and `HEAD /v1/workers` for tracked Worker runtime state;
- `GET` and `HEAD /v1/queues` for tracked Worker queue state;
- `GET` and `HEAD /v1/resilience` for Recovery, Circuit, and Idempotency views;
- `GET` and `HEAD /v1/config` for registered topology and effective settings;
- `GET` and `HEAD /v1/explain` for current derived findings and bottlenecks;
- `GET` and `HEAD /v1/resources` for runtime samples and temporal correlation;
- `GET` and `HEAD /v1/dashboard` for an atomic Snapshot plus Explain view;
- optional `GET` and `HEAD /v1/history` for bounded persistent snapshots;
- `GET` and `HEAD /v1/snapshot` for the complete bounded snapshot.

Every document carries schema `pipeflow.obs.http/v1`. Transport-owned wire
types provide stable snake-case JSON while Core observation types remain free
of serialization policy. Business payloads and error messages are absent
because `obs.Source` never receives them.

## Operational boundary

The handler is safe to mount in an application's existing server. It disables
caching, rejects mutation methods, contains source panics, and supports an
optional request authorizer. Authorization is applied to all endpoints,
including liveness, when configured.

The application remains responsible for its bind address, TLS, server
lifecycle, authentication implementation, request limits, and access logs.
Pipeflow does not silently open a network listener.

When configured with a History source, `/v1/history` supports bounded time
range queries. The handler never writes or prunes history.

## Deferred

Streaming protocols, remote execution control, multi-process discovery,
metrics exporters, OpenTelemetry integration, and graphical interfaces remain
separate future slices. The read-only TUI consumes this HTTP contract.
