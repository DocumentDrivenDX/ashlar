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

- **QUERY-06:** Identity lookup, filtered lists, one-hop and fixed-depth traversal,
  counts grouped by a property or related entity property, and changes-since reads
  must declare work/result bounds. List continuation and truncation must be
  observable; a partial count must never be presented as an exact complete count.
- **QUERY-07:** Every read must identify its publication boundary and per-source
  progress. A caller requiring a minimum source position must receive a result
  meeting it or an explicit not-yet-satisfied outcome under a bounded wait policy.
  Multi-feed progress must not be collapsed into an invented scalar position.
- **QUERY-08:** Reads on behalf of an end user must preserve the authorization
  context through the selected enforcement boundary. Empty authorized results,
  authorization refusals and unavailable execution must be distinguished when
  policy permits; refusal detail must not leak protected entity existence.
  Unsupported user delegation must be reported, never replaced silently with
  broader service permissions.
- **QUERY-09:** Counts and traversal must respect the same effective row/column
  policy as lookup. Cached or continued results must preserve the declared
  publication and authorization boundary or explicitly require a restart.

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

## Consumer input and open decisions

[Consumer input](../../00-discover/hot-store-publication-input.md) P4–P6 motivates
QUERY-06–QUERY-09. Exact cursor format, query syntax and response/error surfaces
belong in a read Contract. PRD Q11 must select count semantics, pagination order,
minimum-position timeout and authorization disclosure rules. Unity Catalog
(Databricks’ data governance service) is a candidate enforcement boundary;
end-user delegation and effective policy require version-specific execution
proof. “Interactive” and “a few seconds” are workload aspirations, not SLAs.

## Native lookup and scale direction

The owner selected 1B nodes with more edges and requested low-latency singleton
lookup directly on Databricks. SPIKE-001 records provisional performance gates
and table-layout candidates. Fabric is an optional bounded graph projection;
it must not be required by the native singleton path.

## Dependencies

FEAT-002, FEAT-004 for incremental progress, PRD FR-3, and [query research](../../00-discover/research.md).
GraphFrames is an evaluation candidate, not a selected mandatory dependency.

## Out of Scope

New query language/compiler, general path algorithms, GraphQL, MCP, production
graph projection deployments, and cross-store live query federation. PuppyGraph,
GraphFrames and bounded Fabric table-mapping experiments are in scope under the
owner’s 2026-10-05 direction. Native singleton queries remain on Databricks.

## Review Checklist

- [x] One PRD subsystem, testable behavior, named story, and explicit boundaries.
- [x] Exact shared surfaces deferred to Contracts, not invented here.
- [ ] Design decisions, executable acceptance evidence, and owner approval.
