# Idempotency Guard

## Decision

Pipeflow provides an opt-in `IdempotencyGuard` for pass-through Steps. The
application supplies a stable typed key extractor and an `IdempotencyStore`
whose `Claim` operation is atomic.

The lifecycle is:

```text
claim new -> execute Step (including retry and Recovery) -> complete
in progress -> reject without executing
already completed -> skip and preserve the flowing input
execution failure -> release for a later attempt
```

Only pass-through functions (`func() error` and `func(T) error`) may be guarded.
A value-producing Step cannot be skipped without replaying its prior business
output, and Core deliberately does not store those payloads.

## Store contract

`IdempotencyStore` separates Core execution semantics from DynamoDB, Redis,
SQL, or other persistence. Implementations define durability, lease expiry,
transaction boundaries, and crash consistency. Store errors fail execution and
are included in the payload-free Step report.

`NewMemoryIdempotencyStore` is concurrency-safe but process-local and
non-durable. It is suitable for tests and single-process protection, not for
coordination between replicas.

## Composition

One claim surrounds the complete Step execution, so individual retries and
Operational Recovery attempts do not claim again. Duplicate-completed checks
happen before Circuit Breaker admission because no dependency call will occur.
If a claimed execution is short-circuited or otherwise fails, its claim is
released.

Reports contain the guard name, claim state, outcome, and store error. Raw keys
and business values are never retained.

## Guarantees and limitations

Pipeflow does not promise exactly-once execution. Duplicate protection depends
on a stable application key and the backing store's atomicity and durability.
A process crash or store completion failure after an external side effect is
an inherently ambiguous boundary; applications needing stronger guarantees
must use an appropriate transactional design.

Waiting for in-progress duplicates, leases, persistence adapters, distributed
coordination, and automatic output replay are deferred.
