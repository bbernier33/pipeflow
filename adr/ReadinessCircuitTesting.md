# Circuit-breaker readiness testing

`ExpectCircuit(path, CircuitExpectation)` composes with ordinary readiness
Scenarios and compares an existing Step CircuitReport exactly: dependency,
state before/after, half-open probe flag, and short-circuit flag. Optional Step
status and error constraints check terminal behavior, with `errors.Is` preserving
Go error identity. Missing circuit evidence or a missing/ambiguous path fails.

Tests exercise real shared breakers across Pipeline runs: qualifying failures
reach the threshold, open circuits suppress calls, cooldown permits a half-open
probe, successful probes close, failing probes reopen, and the probe concurrency
limit rejects competing work. Business errors excluded by IsFailure leave the
circuit closed. Cooldown tests wait for the breaker's reported RetryAt rather
than depending on a guessed sleep duration.

These assertions describe each invocation's recorded evidence. Under concurrent
runs, the breaker's global state can change afterward. They do not claim that a
report is a live snapshot. Pipelines and Scenarios must share the same explicit
breaker instance when exercising a sequence; create a fresh instance for each
independent experiment. Reports retain no business output.
