---
ddx:
  id: US-003
  type: user-stories
  activity: frame
  status: draft
  authoring:
    home: repo
  links:
  - id: FEAT-003
    kind: informed_by
  - id: ashlar.prd
    kind: informed_by
---

# US-003: Query a typed graph

**Feature**: [FEAT-003](../features/FEAT-003-graph-query-path.md)
**Feature Requirements**: QUERY-01–QUERY-09
**PRD Requirements**: FR-3
**Priority**: P0
**Status**: Draft

## Story

**As a** Ontology analytics engineer,
**I want** to find TypeC nodes reachable from a TypeA node through TypeB nodes,
**So that** I can verify the selected graph query path answers the intended relationship question.

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

- [ ] **US-003-AC1** — Given A1→B1→C1, A1→B1→C2 and unrelated A2→B2→C3 in one published fixture, when I run the fixed two-hop TypeA-to-TypeC query for A1, then the distinct target-node answer is exactly C1 and C2.
- [ ] **US-003-AC2** — Given isolated A0, when I look it up and inspect outgoing relationships, then A0 is found and has zero outgoing matches.
- [ ] **US-003-AC3** — Given two separately identified A1→B1 edges and one B1→C1 edge, when I request edge-distinct two-hop paths, then two paths are returned while the distinct target-node answer contains C1 once.
- [ ] **US-003-AC4** — Given an added B1→A1 cycle, when I run the fixed two-hop pattern, then no paths beyond the specified depth are returned.
- [ ] **US-003-AC5** — Given a candidate query execution path, when I review its feasibility evidence, then the result comparison and workload/environment limits are explicit.

- [ ] **US-003-AC6** — Given a filtered list larger than its result cap, continuation or truncation is explicit; bounded grouped counts match the authorized fixture oracle or report incomplete computation.
- [ ] **US-003-AC7** — Given gold behind a caller’s required source position, the read reports the unmet requirement under its bounded wait policy; after publication catches up, the result identifies satisfying progress.
- [ ] **US-003-AC8** — Given permitted-empty, denied and unavailable synthetic executions, the read distinguishes the outcomes permitted by the disclosure policy; unsupported delegation cannot execute with broader service permissions.
- [ ] **US-003-AC9** — Given a user restricted from a fixture row or property, lookup, list, traversal and counts all follow the declared effective policy; continuation across changed publication or authorization preserves its boundary or requires an explicit restart.
- [ ] **US-003-AC10** — Given retained changes after a source position, changes-since results match the shared corpus with explicit bounds and progress; an expired position reports the retention gap.

## Edge Cases

Absent starting node: empty result. A cap or failed execution must be reported as incomplete, never as a complete graph answer.

## Test Scenarios

| Scenario | AC ID | Input / state | Action | Expected result |
| --- | --- | --- | --- | --- |
| Case 1 | US-003-AC1 | A1→B1→C1, A1→B1→C2 and unrelated A2→B2→C3 in one published fixture | I run the fixed two-hop TypeA-to-TypeC query for A1 | the distinct target-node answer is exactly C1 and C2 |
| Case 2 | US-003-AC2 | isolated A0 | I look it up and inspect outgoing relationships | A0 is found and has zero outgoing matches |
| Case 3 | US-003-AC3 | two separately identified A1→B1 edges and one B1→C1 edge | I request edge-distinct two-hop paths | two paths are returned while the distinct target-node answer contains C1 once |
| Case 4 | US-003-AC4 | an added B1→A1 cycle | I run the fixed two-hop pattern | no paths beyond the specified depth are returned |
| Case 5 | US-003-AC5 | a candidate query execution path | I review its feasibility evidence | the result comparison and workload/environment limits are explicit |

Additional scenarios AC6–AC10 use the shared consumer/publication corpus. They
must specify exact authorized results, progress, caps and refusal outcomes as
fixture data before execution. AC3 remains a preservation test: a single-edge
producer profile may reject parallel edges explicitly, but must not collapse them.

## Dependencies

FEAT-003; PRD FR-3; QUERY-01–QUERY-09. Depends on US-002. Query Contract and target evidence await PRD Q4–Q5. General graph algorithms and new compilers are excluded.

## Out of Scope

The exclusions in the parent feature apply. This story does not authorize
production data, deployment, or an application facade.

## Review Checklist

- [x] One persona goal, parent feature and PRD requirement, stable criteria IDs.
- [x] Concrete positive/edge scenarios; normative interfaces left to Contracts.
- [ ] Criteria exercised by passing tests citing `@covers US-003-ACm`.
- [ ] Owner review and approval.

Test scenarios are specifications, not executed tests. All criteria are UNTESTED.
