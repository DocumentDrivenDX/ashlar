---
ddx:
  id: US-004
  type: user-stories
  activity: frame
  status: draft
  authoring:
    home: repo
  links:
  - id: FEAT-004
    kind: informed_by
  - id: ashlar.prd
    kind: informed_by
---

# US-004: Trust replayed publication

**Feature**: FEAT-004
**Feature Requirements**: PUB-01–PUB-10
**PRD Requirements**: FR-4
**Priority**: P0
**Status**: Draft

## Story

**As a** Warehouse platform engineer,
**I want** to replay a synthetic producer feed and inspect published state,
**So that** consumers can trust deletes, history and reported progress.

## Context

Use synthetic opaque TypeA/A1 identities and symbolic source positions.
Version and cursor encoding belongs in the forthcoming publication Contract.
No UMF model fields or platform implementation are selected by this story.

## Walkthrough

1. Select the declared producer profile and deterministic change sequence.
2. Publish it uninterrupted, then replay it with duplicates and interruptions.
3. Compare current state, retained history and observable progress.
4. Inspect refusal, retention and recovery outcomes before accepting publication.

## Acceptance Criteria

- [ ] **US-004-AC1** — Given create A1 v1 and update A1 v2, duplicate delivery and restart from the create position produce the same current value, retained versions and final progress as uninterrupted publication.
- [ ] **US-004-AC2** — Given accepted v2, a late v1 cannot overwrite it; different content claiming v2 is reported according to the declared conflict policy.
- [ ] **US-004-AC3** — Given delete A1 v3, replay of v1/v2 and an older bootstrap cannot restore A1; deliberate recreation follows the declared policy and preserves deletion evidence.
- [ ] **US-004-AC4** — Given an unsupported revision followed by dependent changes, publication stops at the last complete supported boundary and reports the revision; progress does not skip it.
- [ ] **US-004-AC5** — Given failure between related-table updates, no read claims complete publication through the interrupted change; recovery matches the uninterrupted state.
- [ ] **US-004-AC6** — Given retained v1/v2/v3 and their source positions, historical and changes-since reads match the corpus; requesting expired history reports the retention gap and recovery need.
- [ ] **US-004-AC7** — Given missing endpoints, a feed gap or conflicting producer identities, the chosen reject/quarantine policy is observable and does not hide invalid data behind a complete-state claim.
- [ ] **US-004-AC8** — Given two independent feeds, reported progress preserves both positions; unknown source timestamps do not become fabricated freshness measurements.

## Edge Cases

Partial property changes, deleting a node with incident edges, tombstone expiry
and equal-version conflicts require declared policies in the shared corpus.

## Test Scenarios

AC1–AC8 define the positive, replay, interruption and refusal scenarios. Their
expected state and progress must be encoded as independently reviewable fixture
data before automation; no test implementation is supplied here.

## Dependencies

FEAT-004; FEAT-002; PRD Q2/Q3/Q9/Q10. UMF binding is a later integration gate.

## Out of Scope

Live transport, production datasets, deployment and UMF schema authoring.

## Review Checklist

- [x] Stable criteria IDs and traceable feature requirements.
- [ ] Shared corpus and policies reviewed.
- [ ] Passing tests cite the criteria; all criteria currently UNTESTED.
- [ ] Owner review and approval.
