# Idempotency readiness testing

`ExpectIdempotency(path, IdempotencyExpectation)` checks the existing Step
IdempotencyReport through an ordinary Scenario expectation. Guard, Claim, and
Outcome must match exactly; optional Step status, Step error, and store error
constraints use normal `errors.Is` semantics. Missing report evidence or a
missing/ambiguous Step path fails. As in Core reports, duplicate outcomes have
an empty Claim field rather than claiming that a new claim was acquired.

Tests exercise real Pipeline runs against shared process-local stores: first
execution, completed duplicate redelivery, independent keys, concurrent claim
rejection, failed/releasable work followed by successful redelivery, and
Complete/Release errors. A deadline-aware store also exercises bounded
finalization. Side-effect counters verify that suppressed calls do not execute.

Reports expose no idempotency keys or business inputs. Share the guard and store
between scenarios when testing redelivery; construct fresh state for independent
experiments. Applications remain responsible for checks on real side effects,
leases, persistence, crashes, and the backing store's consistency guarantees.
Passing these checks does not establish exactly-once processing.
