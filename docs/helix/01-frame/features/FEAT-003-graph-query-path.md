---
ddx:
  id: FEAT-003
  type: feature-specification
  activity: frame
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.prd
    kind: informed_by
---

# Feature Specification: FEAT-003 — Graph query path

**Feature ID**: FEAT-003
**Status**: Draft
**Priority**: P0
**Owner**: Ashlar owner; technical lead to designate
**Covered PRD Subsystem(s)**: Graph query path
**Covered PRD Requirements**: FR-3
**Cross-Subsystem Rationale**: None — single subsystem.

## Overview

Analytics engineers can follow typed relationships over the selected gold schema.
The capability is a supported query path with examples and limits; a new graph
language or compiler is not a prerequisite.

## Ideal Future State

An engineer answers a concrete TypeA–TypeB–TypeC question and can explain
which identities, parallel paths, snapshot, and unsupported behaviors it returns.

## Problem Statement

A physically valid table layout can still be unsuitable for graph queries.
Unbounded expansion, type filtering, and degree skew can make a small attractive
example misleading at the scale described in the brief.

## Functional Areas

Single capability: bounded relationship queries and their feasibility evidence.

## Requirements

- **QUERY-01:** The query path must support isolated-node lookup, filtered
  directed one-hop traversal, and a fixed two-hop pattern over typed properties.
- **QUERY-02:** Query examples must state direction, endpoint/type filters,
  duplicate-node versus distinct-path semantics, parallel-edge treatment,
  cycle handling, and which published snapshot they observe.
- **QUERY-03:** Evaluate SQL joins as a baseline and GraphFrames motif finding
  as an optional consumer. Record compatibility, additional dependencies,
  limitations, and reasons to select or reject each. Recursive SQL is an
  optional version-gated experiment, not an implicit requirement.
- **QUERY-04:** Results must match a hand-auditable oracle on a shared fixture;
  adapters must preserve original node and edge identities without collisions.
- **QUERY-05:** The feasibility record must name node/edge counts, degree skew,
  isolated fraction, property width, selectivity, compute/version, query plan,
  elapsed time and cost where available. Extrapolation must be marked as such.

### Non-Functional Requirements

- 100% query result identity and multiplicity agreement on the selected corpus.
- Every reported measurement identifies workload and execution environment.
- No billion-node support claim from a smaller trial; latency/cost budgets are
  PRD Q5 and must be resolved before performance acceptance.

## User Stories

- [US-003 — Query a typed graph](../user-stories/US-003-query-typed-graph.md)

## Edge Cases and Error Handling

An absent starting node yields no matches; an isolated starting node remains
queryable with no outgoing results. Parallel edges must not disappear without
an explicit distinct-node request. Cycles must not expand a fixed-depth query
beyond its stated depth. Reaching a resource cap must be reported, not presented
as a complete result. Invalid unpublished snapshots must not be queried as valid.

## Success Metrics

The analytics engineer reproduces all bounded-query answers from the package
and can identify the selected execution path and its unsupported operations.

## Constraints and Assumptions

GraphFrames is a Spark compute library, not a store. A fixed two-hop join is
in scope; arbitrary multi-hop algorithms are deferred. Query result surface and
adapter mappings belong in a subsequent Contract after PRD Q4–Q5 are resolved.

## Dependencies

FEAT-002, PRD FR-3, and [query research](../../00-discover/research.md).
GraphFrames is an evaluation candidate, not a selected mandatory dependency.

## Out of Scope

New query language/compiler, general path algorithms, GraphQL, MCP, Fabric,
Neo4j, and cross-store live query federation.

## Review Checklist

- [x] One PRD subsystem, testable behavior, named story, and explicit boundaries.
- [x] Exact shared surfaces deferred to Contracts, not invented here.
- [ ] Design decisions, executable acceptance evidence, and owner approval.
