// Package readiness exercises an assembled Pipeflow Pipeline against explicit
// production-readiness expectations.
//
// A Scenario separates its arrangement, real Pipeline execution, and checks.
// Checks inspect the transient business output and Pipeflow's payload-free
// RunReport. Result deliberately does not retain the business output.
package readiness
