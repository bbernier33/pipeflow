# Readiness Contract and Topology Inspection

## Status

Accepted for the second v3.x Production Readiness increment.

## Decision

Core exposes additive, payload-free `InspectFlow` and `InspectFlowInput`
methods. They return the existing definition topology plus structured typed
producer/consumer boundaries. Inspection reuses the same internal Step flow
validation used before execution, including Go assignability and nil rules.

The `readiness` package maps this evidence to its standard verdicts:

- compatible known topology: PASS;
- incompatible definition or boundary: FAIL;
- a boundary whose producer type is not statically known: REVIEW.

Endpoints contain stable definition paths, names, kinds, and Go type names.
They never contain functions or business values. Pass-through Steps preserve
the preceding producer identity, matching runtime value-flow semantics.

## Consequences

Applications can report the exact failing boundary without parsing validation
error text. An application accepting external input can use
`InspectContractsInput` with a representative value to resolve its initial
boundary. Conditional replacement and custom StageItems can remain unknown and
therefore require review rather than receiving false compatibility claims.
