# ADR-028: Definition-only Pipeline Introspection

- **Status:** Accepted
- **Date:** 2026-08-29

## Decision

`Pipeline.Describe()` returns a recursive `Description` value:

```go
type Description struct {
    Kind     DescriptionKind
    Name     string
    Type     string
    Children []Description
}
```

Stable `DescriptionKind` constants identify Pipeline, Stage, Step,
ConcurrentSteps, Parallel, Branch, Subflow, custom StageItem, and invalid item
nodes. Existing declaration order is preserved at every level.

The same value implements `fmt.Stringer` and renders a compact Unicode tree.
The structured fields are the primary programmatic API; text rendering is a
convenience for CLI output, logs, and documentation.

Each call recursively allocates new node slices. Callers may transform or
annotate their returned tree without mutating Pipeline definitions. Describe
does not validate and is safe for malformed trees: typed nil items become
explicit invalid nodes rather than panicking. Custom StageItems are opaque and
provide their concrete Go type name without an inferred internal structure.

## Data Boundary

Descriptions contain definition shape and identity only. They exclude action
functions, predicates, hooks, policies, flowing values, result metadata,
Context data, RunReport facts, live status, timings, errors, and control
methods. Calling Describe never invokes user code or starts execution.

## Boundaries

Core does not generate Mermaid, JSON/YAML schemas, documentation sites, CLI
styling, diagrams, UIs, execution plans, DAG edges, or editable definitions.
Those tools can consume the structured description separately.
