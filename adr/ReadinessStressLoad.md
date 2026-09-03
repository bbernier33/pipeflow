# Readiness Stress and Load Testing

## Status

Accepted for the sixth v3.x Production Readiness increment.

## Decision

`readiness.LoadPlan` repeatedly executes the existing finite Pipeline Scenario
with a fixed run count, bounded concurrency, optional start-rate limit, and an
optional deterministic per-index input factory. It does not introduce another
Worker or scheduling runtime.

Every run retains its complete, payload-free Pipeflow RunReport plus duration
and Scenario verdict, preserving Step, attempt, Recovery, cleanup, and error
evidence without business output. Aggregate evidence includes completion counts, Pipeline and
Scenario failure counts, reviews, observed maximum concurrency, wall duration,
throughput, and min/mean/p50/p95/p99/max Pipeline latency.

Pipeline failures are counted separately from Scenario failures because an
expected rejected input can be a passing readiness case. Explicit aggregate
checks enforce application requirements. Built-ins cover Pipeline failure rate,
p95 latency, and minimum throughput; custom checks use the same PASS, FAIL, and
REVIEW contract.

## Boundaries

This is bounded finite load, not a benchmark framework, distributed load
generator, or durable queue. Worker/Stream pressure, saturation, backpressure,
drain, ordering, and resource bounds belong to the following flow-pressure
milestone. Payload generation and external test coordination remain
application-owned.
