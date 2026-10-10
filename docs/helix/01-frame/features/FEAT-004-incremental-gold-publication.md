---
ddx:
  id: FEAT-004
  type: feature-specification
  activity: frame
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.prd
    kind: informed_by
---

# Feature Specification: FEAT-004 — Incremental gold publication

**Feature ID**: FEAT-004
**Status**: Draft
**Priority**: P0
**Owner**: Ashlar owner; producer and platform leads to designate
**Covered PRD Subsystem(s)**: Incremental gold publication
**Covered PRD Requirements**: FR-4
**Cross-Subsystem Rationale**: None — single subsystem.

## Overview

Platform engineers publish reproducible graph state from an external replayable
change feed under an admitted UMF model and declared source profile. The
potential-consumer input P1–P4/P7 supplies the replay, history and progress
requirements. Each transport retains its original transactions and semantics.

## Ideal Future State

A consumer can identify the source progress reflected in gold, observe a delete,
and request a historical version without mistaking an incomplete publication
for a complete one. A platform engineer can restart publication and obtain the
same accepted state.

## Problem Statement

Arrival order, retries and partial table updates can silently overwrite newer
values, resurrect deleted entities or report progress beyond the data available
for reads. Producer integrity alone does not demonstrate projection integrity.

## Functional Areas

| Area | Responsibility |
| --- | --- |
| Deterministic application | Replay, ordering, revision barriers and deletion |
| Published state | Consistent visibility, progress and retained history |

## Requirements

### Deterministic application

- **PUB-01:** Applying the same accepted logical changes repeatedly must yield
  the same state and history. Restart from a retained source position must
  reproduce the uninterrupted result without duplicate effects.
- **PUB-02:** Within the declared version scope, an older change must not
  overwrite a newer accepted value. Conflicting content at the same identity
  and version must be reported under an explicit conflict policy.
- **PUB-03:** Deletion must remove an entity from current reads and retain enough
  deletion evidence to prevent resurrection by stale replay or bulk bootstrap.
  An intentional later recreation must follow a declared identity/version policy.
- **PUB-04:** A schema revision must be supported before dependent data is
  published. An unsupported revision must stop the affected publication boundary,
  identify the obstruction, and retain the last complete supported state.
- **PUB-05:** Producer identity and endpoint guarantees must be verified against
  a declared profile; invalid projected data must be rejected or explicitly
  quarantined. Partial entity/property updates and source transaction boundaries
  require a declared application policy before executable conformance.

### Published state

- **PUB-06:** A complete publication must expose the source positions it actually
  reflects and their observation time. Progress must not advance past unapplied
  changes, rejected revisions or incomplete related-table publication.
- **PUB-07:** Related reads must use a reproducible publication boundary or
  explicitly report inconsistent progress. Positions from independent feeds
  must remain distinct; no global ordering may be inferred from them.
- **PUB-08:** Retained entity versions must be attributable to source progress
  and publication time. Historical reads and changes-since reads must disclose
  retention limits; an expired position must produce an explicit recovery need.
- **PUB-09:** Freshness reports must distinguish source event time, observed
  progress and publication time. Unavailable measurements must remain unknown;
  no latency budget is accepted without a named workload and environment.
- **PUB-10:** Producer, publisher and consumer must share a synthetic conformance
  corpus specifying inputs, expected current/history state and progress, including
  refusal and recovery cases. All identities and unsupported meaning must remain
  accounted for.

### Non-Functional Requirements

Zero unexplained state differences between uninterrupted and replayed corpus
runs; zero stale resurrection cases; every published-progress claim backed by
visible state. Compatibility claims require target, version, subset and evidence.
Bounded examples use synthetic or provenance-reviewed domain-pack data. Real
source support requires its own authorization, native transport and durable
acknowledgement evidence; a local fixture cannot establish Truss support.

## User Stories

- [US-004 — Trust replayed publication](../user-stories/US-004-trust-replayed-publication.md)

## Edge Cases and Error Handling

Feed gaps, out-of-order changes, equal-version conflicts, unknown revisions,
missing endpoints, interrupted publication and expired cursors must be visible.
Deleting a node requires a declared incident-edge policy. Tombstone expiry must
not silently reopen stale resurrection. Cross-feed collisions require explicit
source authority. No transport is assumed to deliver exactly once.

## Success Metrics

An independent engineer reproduces every corpus state, history and progress
outcome, including all replay and refusal cases, without undocumented ordering
or identity conventions.

## Constraints and Assumptions

Unity Catalog managed Delta is the warehouse storage target.
[CONTRACT-003](../../02-design/contracts/CONTRACT-003-delta-graph-tables.md)
defines physical roles, source admission, history and durable acknowledgement;
[CONTRACT-001](../../02-design/contracts/CONTRACT-001-publication-boundary.md)
defines complete publication and per-source progress. Logical identities and
revision references remain opaque. Property-level source events require an
explicit semantics-preserving adapter; an edge triple cannot replace independent
edge identity or collapse parallel occurrences.

## Dependencies

PRD FR-4; FEAT-001’s admitted UMF model and FEAT-002’s integrity/publication
boundary; CONTRACT-001 and CONTRACT-003. Each source and model requires separate
admission. An unavailable external source blocks only its own qualification,
while other admitted sources can ingest and publish independently.

## Out of Scope

Cross-store query federation, production provisioning and an application API
facade. Reusable UMF interpretation and evolution capabilities belong upstream
in UMF; Ashlar owns graph bindings, source application and publication.

## Review Checklist

- [x] Traceable behavior, consumer evidence and explicit undecided profiles.
- [ ] Publication Contract and physical design reviewed.
- [ ] Acceptance criteria exercised and owner approval recorded.
