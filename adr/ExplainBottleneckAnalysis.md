# Explain and Derived Bottleneck Analysis

## Decision

`obs.Analyze` is a pure function over one detached `obs.Snapshot`. It produces
structured findings with stable codes, severity, structural location, and
numeric evidence. It performs no I/O, retains no state, and cannot affect
collection or execution.

The initial rules cover:

- bounded-queue utilization;
- worker saturation while work remains in flight;
- Step execution-time concentration within a Pipeline;
- Pipeline failure rate with a minimum evidence count;
- repeated or failed Operational Recovery;
- open and half-open Circuits;
- Idempotency duplicate rate and store failures.

Thresholds are explicit through `AnalysisOptions`, with conservative defaults.
Rate rules require minimum sample counts. Step-time concentration requires at
least two observed Steps. Findings are deterministically ordered, and the first
high-severity pressure/dependency/time-concentration finding is exposed as the
primary bottleneck candidate.

## Interpretation boundary

A finding states only what its evidence supports. Queue utilization is a
point-in-time observation, cumulative counters are not rates over a hidden
window, and concentrated execution time does not prove why a Step is slow.
“Primary bottleneck” means the strongest current candidate under the configured
rules, not a causal guarantee.

No findings means no configured threshold was crossed; it is not a health
proof. Historical trend comparison, resource correlation, adaptive baselines,
alerts, and automated configuration recommendations remain future work.
