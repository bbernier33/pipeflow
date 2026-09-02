# Effective Configuration View

## Decision

`obs.Collector.TrackPipeline` explicitly registers one immutable Pipeline
definition by name. The Collector captures Core's existing `Describe()` tree
and, when available, its `EffectiveConfig()` result. Registration validates the
Pipeline and returns an idempotent removal function.

The topology contains names, structural kinds, and Step roles only. Effective
configuration contains resolved execution-policy values and the source layer
that won precedence. Neither representation contains functions, Context data,
business values, credentials, reports, or mutable execution state.

Snapshots deep-copy nested descriptions and configurations. Mutating a returned
view cannot alter the Collector or Pipeline. Registration does not attach an
Observer, start execution, or enable hot configuration changes.

## Transport and persistence

The HTTP transport exposes registered definitions at `/v1/config` and includes
them in `/v1/snapshot`. Transport-owned DTOs keep snake-case JSON policy out of
Core. Persistent history records definitions present in a captured snapshot,
allowing incident records to preserve the configuration evidence available at
that time.

Unconfigured Pipelines expose topology and no effective configuration because
Core intentionally distinguishes “WithConfig resolved this Pipeline” from a
plain Go definition. Synthesizing an alternate default-resolution model in the
Observation package would risk disagreeing with execution and is therefore not
done.
