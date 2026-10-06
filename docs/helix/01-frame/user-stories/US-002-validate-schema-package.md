---
ddx:
  id: US-002
  type: user-stories
  activity: frame
  status: draft
  authoring:
    home: repo
  links:
  - id: FEAT-002
    kind: informed_by
  - id: ashlar.prd
    kind: informed_by
---

# US-002: Validate a schema package

**Feature**: [FEAT-002](../features/FEAT-002-databricks-schema-package.md)
**Feature Requirements**: SCHEMA-01–SCHEMA-09
**PRD Requirements**: FR-2
**Priority**: P0
**Status**: Draft

## Story

**As a** Warehouse platform engineer,
**I want** to create and check the synthetic graph tables,
**So that** I can detect invalid data before consumers rely on it.

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

- [ ] **US-002-AC1** — Given the reviewed model and a pinned Databricks target, when I execute the package DDL and load the valid fixture, then all example tables are created and valid records including isolated A0 are retained.
- [ ] **US-002-AC2** — Given separate fixtures with duplicate A1, missing B9 endpoint, and null required identity, when I run the package checks, then each invalid case is identified and cannot be accepted silently.
- [ ] **US-002-AC3** — Given a breaking property type change, when I assess the revised package, then it requires an explicit migration instead of an automatic compatibility claim.
- [ ] **US-002-AC4** — Given an interrupted multi-table update, when I request a valid published snapshot, then the incomplete revision is withheld or explicitly reported inconsistent.
- [ ] **US-002-AC5** — Given informational key declarations and governance tags, when I inspect the package, then they are distinguished from validated or enforced rules with evidence.

- [ ] **US-002-AC6** — Given a Truss-shaped fixture, its canonical and graph-serving representations preserve source IDs, property missing/null distinctions, exact values, typed endpoints, independent edge identities and retained content, or explicitly refuse/report an unsupported projection.
- [ ] **US-002-AC7** — Given the selected native Delta layout, singleton lookup runs without Fabric and meets the reviewed cold/warm budgets on the pinned target; graph adapter mappings preserve isolated nodes and parallel paths with explicit target limits.

## Edge Cases

Late endpoint, replay, deletion, and parallel-edge cases need explicit decisions in the Contract. A valid isolated node must never fail endpoint checks merely because it has no edges.

## Test Scenarios

| Scenario | AC ID | Input / state | Action | Expected result |
| --- | --- | --- | --- | --- |
| Case 1 | US-002-AC1 | the reviewed model and a pinned Databricks target | I execute the package DDL and load the valid fixture | all example tables are created and valid records including isolated A0 are retained |
| Case 2 | US-002-AC2 | separate fixtures with duplicate A1, missing B9 endpoint, and null required identity | I run the package checks | each invalid case is identified and cannot be accepted silently |
| Case 3 | US-002-AC3 | a breaking property type change | I assess the revised package | it requires an explicit migration instead of an automatic compatibility claim |
| Case 4 | US-002-AC4 | an interrupted multi-table update | I request a valid published snapshot | the incomplete revision is withheld or explicitly reported inconsistent |
| Case 5 | US-002-AC5 | informational key declarations and governance tags | I inspect the package | they are distinguished from validated or enforced rules with evidence |

## Dependencies

FEAT-002; PRD FR-2; SCHEMA-01–SCHEMA-09. Depends on US-001. Schema/publication Contract awaits PRD Q2–Q4 and Q7. Live connectors and transactional engines are excluded.

## Out of Scope

The exclusions in the parent feature apply. This story does not authorize
production data, deployment, or an application facade.

## Review Checklist

- [x] One persona goal, parent feature and PRD requirement, stable criteria IDs.
- [x] Concrete positive/edge scenarios; normative interfaces left to Contracts.
- [ ] Criteria exercised by passing tests citing `@covers US-002-ACm`.
- [ ] Owner review and approval.

Test scenarios are specifications, not executed tests. All criteria are UNTESTED.
