# Pipeflow v3.0.0

V3 includes the Core++ resilience, Observation/Operations, and Production
Readiness milestones developed since v1.0.0.

## Installation and migration

```sh
go get github.com/bbernier33/pipeflow/v3@v3.0.0
go install github.com/bbernier33/pipeflow/v3/cmd/pipeflow-obs@v3.0.0
```

Update all Pipeflow imports to `github.com/bbernier33/pipeflow/v3` and its
subpackages. The package identifier remains `pipeflow`. See
[MIGRATING_V3.md](MIGRATING_V3.md). Go 1.26.5 or newer is required.

## Included capabilities

- Core: ordinary-function value flow, configuration, Source/Sink roles, bounded
  Worker/Stream runtimes, Operational Recovery, shared circuit breakers, and
  idempotency guards with bounded store finalization.
- Observation: payload-free collection, HTTP transport, persistent snapshot
  history, queue/worker and resilience views, effective configuration,
  bottleneck analysis, resource correlation, and the observation TUI/CLI.
- Readiness: scenarios, contract/topology inspection, safe E2E verification,
  payload and semantic mutation, load/pressure testing, controlled injection,
  Recovery/circuit/idempotency assertions, seeded chaos replay, explicitly gated
  progressive integration, untested-path detection, and aggregate reporting.
- Correctness: late results after deadlines are rejected, rate-limit override
  flags are accurate, and external idempotency finalization cannot wedge run
  completion indefinitely.

## Operational contracts

Queues are in-memory, and exactly-once processing is not promised. Store
durability belongs to adapters. Readiness reflects declared requirements and
supplied evidence; it never automatically authorizes production writes.
Application code must wire real boundaries consistently with declarations.

No v2 release is required to upgrade directly from v1 to v3. The previous v2/v3
labels described development milestones; v3.0.0 is the public major release
containing them. V1 tags and imports remain available unchanged.
