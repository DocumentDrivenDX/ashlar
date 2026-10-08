---
ddx:
  id: FEAT-002
  type: feature-specification
  activity: frame
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.prd
    kind: informed_by
---

# Feature Specification: FEAT-002 — Databricks schema package

**Feature ID**: FEAT-002
**Status**: Draft
**Priority**: P0
**Owner**: Ashlar owner; technical lead to designate
**Covered PRD Subsystem(s)**: Databricks schema package
**Covered PRD Requirements**: FR-2
**Cross-Subsystem Rationale**: None — single subsystem.

## Overview

Engineers receive an inspectable physical-schema package for the graph profile.
The package covers Databricks gold Delta tables and the evidence needed to trust
them. Logical identity, physical placement, and runtime enforcement are distinct.

## Ideal Future State

An engineer can create the tables, load the synthetic example, detect invalid
records, and explain evolution and publication limits from one reviewed package.

## Problem Statement

Choosing node/edge columns alone leaves duplicates, missing endpoints, typed
properties, publication consistency, and sparse nodes undefined. Informational
keys do not close those gaps.

## Functional Areas

| Area | Responsibility |
| --- | --- |
| Schema projection | Physical representation and creation package |
| Integrity and evolution | Validation, revision compatibility, and publication boundaries |

## Requirements

### Schema projection

- **SCHEMA-01:** The package must provide a complete mapping and create-table
  DDL for a selected Databricks profile, including nodes independent of edges,
  typed edges, properties, and provenance from FEAT-001.
- **SCHEMA-02:** The design must compare shared tables, per-type tables, and a
  hybrid using the same workload. It must justify property representation and
  query access by evidence; Postgres JSONB/index assumptions must not transfer
  to Delta by name alone.
- **SCHEMA-03:** Every selected logical construct must identify its physical
  representation or explicit residual; unsupported mappings must not silently
  coerce values, collapse parallel edges, or drop isolated nodes.

### Integrity and evolution

- **SCHEMA-04:** The package must classify each constraint as declared,
  externally validated, write-enforced, or unsupported, with evidence. Include
  identity uniqueness, endpoint existence/type, nullability, and cardinality.
- **SCHEMA-05:** The package must define revision compatibility and explain
  additive versus breaking changes, rename/identity behavior, migration,
  replay, deletes, and late-arriving endpoints for the selected subset.
- **SCHEMA-06:** The package must define a reproducible publication/read boundary
  across related tables. Interrupted updates must not silently produce a claimed
  valid snapshot. Per-table atomicity must not be treated as multi-table atomicity.
- **SCHEMA-07:** Governance and source/value/edge version metadata must remain
  traceable; unimplemented access controls and retention must be explicit.

- **SCHEMA-08:** Canonical tables must preserve Truss-compatible catalog IDs,
  object/edge identity, typed endpoints, property maps, retained unknown content
  and property-level history. Warehouse optimizations must be explicit projections
  or recorded storage choices, not changes to logical identity or source meaning.
- **SCHEMA-09:** The package must provide native Databricks singleton lookup
  independent of Fabric, and scalar table mappings for PuppyGraph, GraphFrames
  and bounded Microsoft Fabric Graph projections. Each mapping must identify
  target restrictions, omitted meaning and native execution evidence.

- **SCHEMA-10:** Ashlar’s physical model must be maintained as UMF. Reusable
  Delta DDL generation belongs to UMF, including versioned extensions for missing
  semantics. Generated installation output and microsite diagram data must come
  from that model. Unknown meaning must survive serialization and block unsafe
  DDL generation; logical references must not imply warehouse enforcement.

### Non-Functional Requirements

- 100% of accepted DDL and valid fixtures must execute on one pinned target.
- 100% of authored duplicate, endpoint, and nullability counterexamples detected.
- Zero compatibility claims without target/version/subset and execution evidence.

## Schema-package milestone acceptance

For the owner-revised physical design milestone, accept the candidate when the
six checks in the [milestone acceptance record](../../02-design/spikes/SPIKE-001-table-layout/layout-milestone-acceptance.md)
are supported by bounded native evidence and exact local package review. Existing
SCHEMA-01–SCHEMA-09 remain governing product requirements. Full production/source
and external-engine qualification belong to their implementation milestones.
Timing and1B/5B runtime targets do not block packaging or the next build slice;
unmet targets must remain visible. No heavy or repeated benchmark is required.

## User Stories

- [US-002 — Validate a schema package](../user-stories/US-002-validate-schema-package.md)

## Edge Cases and Error Handling

Duplicate identities, source-key collisions, unknown types, missing endpoints,
and narrowing property conversions must produce identifiable validation failures.
An isolated node is valid. Publication policy must state whether an invalid row
blocks the snapshot or is quarantined; no accepted snapshot may hide the failure.
An incomplete multi-table update remains unpublished or explicitly inconsistent.

## Success Metrics

One independent platform engineer can reproduce table creation and the positive
and negative fixture outcomes without private naming or coercion conventions.

## Constraints and Assumptions

Gold Delta is selected; layout, partition/clustering, history, and ingestion
mechanism are undecided. Exact columns, data types, keys, and DDL belong in a
subsequent Contract, governed by PRD Q2–Q4 and Q7.

## Dependencies

FEAT-001, PRD FR-2, one selected Databricks execution environment.
[Constraint evidence](../../00-discover/resources/databricks-constraints.md)
governs the current informational/enforced distinction, not all future versions.

## Out of Scope

Live operational connectors, transactional write engines, deployment, universal
migration tooling, and production authorization implementation.

## Review Checklist

- [x] One PRD subsystem, testable behavior, named story, and explicit boundaries.
- [x] Exact shared surfaces deferred to Contracts, not invented here.
- [ ] Design decisions, executable acceptance evidence, and owner approval.
