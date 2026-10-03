---
ddx:
  id: US-001
  type: user-stories
  activity: frame
  status: draft
  authoring:
    home: repo
  links:
  - id: FEAT-001
    kind: informed_by
  - id: ashlar.prd
    kind: informed_by
---

# US-001: Review a graph mapping

**Feature**: [FEAT-001](../features/FEAT-001-umf-graph-profile.md)
**Feature Requirements**: MODEL-01–MODEL-06
**PRD Requirements**: FR-1
**Priority**: P0
**Status**: Draft

## Story

**As a** Warehouse platform engineer,
**I want** to review a synthetic ontology mapping,
**So that** I can account for every authored construct before trusting generated tables.

## Context

The first milestone uses synthetic domain-neutral data to expose structural mistakes
before real-data adoption. This journey exercises the parent feature's outcome;
exact interface and schema surfaces belong in the forthcoming design Contracts.

## Walkthrough

1. I select the pinned model, synthetic example, and applicable target profile.
2. I perform the review or execution described in the acceptance criteria.
3. I inspect the results and deliberately invalid variants.
4. I accept only outcomes whose preservation, integrity, and execution limits
   match the package's declared contract.

## Acceptance Criteria

- [ ] **US-001-AC1** — Given a pinned model of consumer-defined TypeA/TypeB nodes with A1, B1, isolated A0 and a directed edge carrying confidence, when I review its projection, then every selected identity, type, property, direction and isolated node has an explicit mapping.
- [ ] **US-001-AC2** — Given an unsupported authored rule, when I review the projection report, then the rule remains in the retained source and is explicitly identified as unsupported.
- [ ] **US-001-AC3** — Given a retained-source recovery and a target-only recovery, when I compare their results, then the report identifies any meaning recoverable only with the retained source.
- [ ] **US-001-AC4** — Given an unknown extension affecting graph meaning, when I request a supported projection, then acceptance is blocked with the affected construct identified.

## Edge Cases

Unresolved endpoint type or stale binding: block acceptance and retain source. A label rename must not silently change identity.

## Test Scenarios

| Scenario | AC ID | Input / state | Action | Expected result |
| --- | --- | --- | --- | --- |
| Case 1 | US-001-AC1 | a pinned model of consumer-defined TypeA/TypeB nodes with A1, B1, isolated A0 and a directed edge carrying confidence | I review its projection | every selected identity, type, property, direction and isolated node has an explicit mapping |
| Case 2 | US-001-AC2 | an unsupported authored rule | I review the projection report | the rule remains in the retained source and is explicitly identified as unsupported |
| Case 3 | US-001-AC3 | a retained-source recovery and a target-only recovery | I compare their results | the report identifies any meaning recoverable only with the retained source |
| Case 4 | US-001-AC4 | an unknown extension affecting graph meaning | I request a supported projection | acceptance is blocked with the affected construct identified |

## Dependencies

FEAT-001; PRD FR-1; MODEL-01–MODEL-06. Pin UMF and define the profile Contract after PRD Q1–Q2. Actions and upstream UMF refactoring are excluded.

## Out of Scope

The exclusions in the parent feature apply. This story does not authorize
production data, deployment, or an application facade.

## Review Checklist

- [x] One persona goal, parent feature and PRD requirement, stable criteria IDs.
- [x] Concrete positive/edge scenarios; normative interfaces left to Contracts.
- [ ] Criteria exercised by passing tests citing `@covers US-001-ACm`.
- [ ] Owner review and approval.

Test scenarios are specifications, not executed tests. All criteria are UNTESTED.
