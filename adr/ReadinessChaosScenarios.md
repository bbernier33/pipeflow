# Reproducible chaos scenarios

`ChaosPlan` coordinates a finite sequence of application-defined experiments.
It derives an independent case seed from the plan seed and case index using
SplitMix64. The mapping does not consume shared generator state or depend on
the configured case count. `ChaosCase.Random` creates a private seeded generator
for selecting plausible faults, pressure settings, and inputs before execution.

The experiment constructs fresh state and returns exactly one existing Scenario,
LoadPlan, or PressurePlan result in `ChaosEvidence`. These plans own execution,
assertions, and reports. This permits fault-plus-pressure combinations without
adding another execution runtime or an automatic random failure policy.

Each retained case records its index, seed, verdict, evidence, and tool errors.
No mutated input or business output is added to the retained result. Experiments
describe generated conditions through the Scenario arrangement using safe
metadata. PASS means the underlying expectations passed; REVIEW propagates;
missing/invalid evidence, callback errors, and callback panics fail the case.
Cases continue after individual failures. Parent cancellation stops further
cases and is reported as plan failure.

`RunCase(ctx, index)` rebuilds and executes only the requested case. Replay
requires the same plan seed and experiment definition, deterministic selection
from the supplied seed, and fresh fault wrappers, guards, stores, and fixtures.
Record the application revision/toolchain with test artifacts when comparing
runs across changes. Side effects and external infrastructure remain controlled
by the application.

Seeded conditions are reproducible; wall-clock durations, Run IDs, concurrent
scheduling, and changing external services are not guaranteed to replay exactly.
Draw random choices before launching workers and never share rand.Rand between
goroutines without synchronization. This milestone does not perform automatic
production writes, fault shrinking, persistence, or distributed orchestration.
