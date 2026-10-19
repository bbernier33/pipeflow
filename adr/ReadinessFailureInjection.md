# Controlled failure injection

The readiness package provides `Inject(function, Fault)` to compose controlled
conditions with real Step execution. The returned function preserves its exact
Go signature, so existing contract validation remains available. Source and Sink
roles use the same adapter.

Faults can inject errors, panics, delays, cancellation, or a cooperative wait for
the execution deadline. Core policies and reports handle the resulting behavior.
`Calls` limits injection to the first N invocations of the wrapper. Its atomic
counter is shared across runs; concurrent allocation follows invocation order.
Build a fresh wrapper for each independent experiment.

Ordinary functions have no execution context and delayed calls return
synchronously. Cooperative delay and timeout injection require the context-aware
Pipeflow signature. Waiting for cancellation requires an execution deadline or
an explicitly controlled cancellation source. The caller owns safe test
boundaries and explicit Scenario expectations. Fault definitions are test
configuration; normal RunReports retain execution evidence without payloads.
