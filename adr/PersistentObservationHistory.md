# Persistent Observation History

## Decision

`pipeflow/obs/history` persists immutable Observation snapshots in an explicit
application-owned directory. Recording is an application action:
`Store.Append(collector.Snapshot())`. The store is not an Observer, creates no
goroutine, and never runs inside a Pipeline execution boundary.

Each snapshot is a separate versioned JSON record committed with a temporary
file and same-directory rename. Optional `Sync` requests an operating-system
file sync before commit. The store supports chronological time-range queries,
bounded results, latest-record lookup, and storage statistics.

Retention is explicit. `MaxSnapshots` bounds record count and `MaxAge` bounds
capture age; when both are zero the safe default is 1000 snapshots. Pruning
occurs after a successful append. Corrupt or unsupported records return named,
actionable errors and are not silently skipped.

## Data and ownership boundary

History stores the same payload-free Snapshot exposed by `obs.Source`: names,
IDs, statuses, classifications, timestamps, durations, counts, and derived
views. It cannot contain flowing business values because the Collector never
receives them. Applications still choose a directory appropriate for their
security, backup, disk-quota, and filesystem requirements.

One Store is concurrency-safe within one process. Multiple processes must not
write the same directory. Distributed storage, database/object-store adapters,
encryption, compression, replication, and automatic recording schedules remain
separate future work.
