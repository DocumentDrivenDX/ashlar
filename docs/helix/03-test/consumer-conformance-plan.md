---
ddx:
  id: TP-001
  type: test-plan
  activity: test
  status: draft
  authoring:
    home: repo
  links:
  - id: CONTRACT-001
    kind: informed_by
  - id: CONTRACT-002
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
---

# Consumer conformance test plan

## Current physical-layout milestone

The owner revised acceptance on2026-10-08 to close the design/schema-package
milestone using existing evidence plus a quick local package review. Its scope
and passed checks are recorded in [layout milestone acceptance](../02-design/spikes/SPIKE-001-table-layout/layout-milestone-acceptance.md).
No Spark, Databricks benchmark or enlarged corpus is required to move to build.
The broader consumer-conformance strategy below continues to govern later
behavior/security/production claims; it is not a gate for this bounded handoff.
Original latency/freshness/scale shortfalls remain measured limits, not passes.

## Testing Strategy

Prove deterministic publication and bounded authorized reads using
[consumer-conformance.json](fixtures/consumer-conformance.json). Expected state
is authored fixture data, not derived from a publisher implementation. All
behavior criteria remain UNTESTED until an implementation runs them with cited
acceptance IDs. This plan scopes the consumer work, not all Ashlar acceptance.

| Level | Coverage target | Gate |
| --- | --- | --- |
| Fixture validation | Unique case IDs, resolvable identities, coherent expectations and acceptance references | Before handing off corpus |
| Contract | Every publication/read case; exact logical state and outcome | Before boundary conformance claim |
| Platform integration | Real table publication failure/recovery, policy delegation and progress visibility | Before Databricks support claim |
| Query feasibility | Workload, plans, runtime and measurements for US-003-AC5 | Before performance acceptance |

No language, framework, CI service or runtime is selected. Schema/corpus
validation is distinct from behavioral execution. A passing local simulator
cannot establish Databricks transaction or access-control guarantees.

## Test Data

The synthetic profile uses opaque ASCII node/edge identifiers, revision r1,
feed S/epoch e and integer positions. Graph fixture contains isolated A0,
parallel A1→B1 edges, B1→C1/C2 and a B1→A1 cycle. History fixture contains
create v1, replace v2 and delete v3. A second source tests authority and progress.

`publicationCases` initialize a complete state and apply actions in order.
`expected.state` is the final logical state; compare current/history/tombstone
collections by identity and version, and progress by feed/epoch. Do not compare
wall-clock times or generated publication IDs for replay equivalence. Inspect
intermediate visible state when an action injects failure; later recovery must
not hide an earlier incomplete visibility failure.

Plain batch actions invoke CONTRACT-001. `applyWithFailure` is a harness action
injecting failure at beforeCommit, then inspecting visible state. `bootstrap`
is a harness attempt to replace current state with an older snapshot without
reviewed reconciliation; it must return RECOVERY_REQUIRED. These actions are
not production feed messages. Outcomes for each action must be captured even
when the fixture asserts only the final outcome/state.

`readCases` reference a named publication and simulate the stated policy/runtime
conditions before executing the request. Response publication is its ID, not the
full state. Unspecified setup defaults: operations permitted, no hidden data,
delegation supported, store available, no continuation support, all history
retained. A setup override MUST NOT silently mutate the fixture publication.
`earliestAfter` sets the retained-history lower bound. `cursorContext` injects
an earlier cursor binding. `hiddenIdentities` suppresses incident edges and
`hiddenProperties` forbids filters/counts involving that property.

## Coverage Requirements

Every fixture case must pass with no unexplained differences. Every in-scope
criterion needs a covering executable test with `@covers US-nnn-ACm` and recorded
pass/fail evidence. Fixture metadata `covers` records planned coverage only.

| Acceptance class | Fixture cases | Primary layer | Remaining proof |
| --- | --- | --- | --- |
| US-004-AC1–AC5/AC7/AC8 | PUB-* | Contract | Native adapter, atomic visibility, freshness instrumentation |
| US-004-AC6 | READ-history/changes/expired | Contract | Actual retention and historical policy |
| US-003-AC1–AC4 | READ-two-hop/isolated/parallel/cycle | Contract | Pinned physical query execution |
| US-003-AC5 | No fixture substitute | Platform feasibility | Target/runtime, corpus workload and query evidence |
| US-003-AC6–AC10 | Remaining READ-* | Contract | Full policy enforcement and safe continuation integration |

The existing story requirements remain authoritative. These cases exercise
specific examples; they do not exhaust each criterion. Add cases for publication
revision recovery, position conflict, node-plus-edge atomic deletion, missing/null
count groups, valid continuation across unchanged context, expired publication,
epoch mismatch, source-time measurement and protection in historical reads before
claiming complete boundary coverage. STP-003/STP-004 and technical designs remain
unwritten until execution interfaces and platform mechanics are selected.

## Implementation Order

1. Review the proposed identity, replacement, conflict, retention and disclosure
   policies with the consumer/producer; revise Contracts and fixtures together.
2. Specify an adapter-independent execution harness and story test plans.
3. Exercise publication and read boundaries against an independent implementation.
4. Select and prove physical publication/read mechanisms on a pinned target.
5. Bind exact UMF capabilities after merge, with explicit loss and preservation tests.

## Infrastructure

No platform resources are required to review the corpus. Native policy and
Databricks evidence require a synthetic sandbox, a pinned target, authorized test
principals and failure injection. No production data is authorized by this plan.

## Risks

| Risk | Consequence | Mitigation |
| --- | --- | --- |
| Whole-entity adapter conflicts with producer property events | Lost transactional/version meaning | Require native-to-boundary mapping and batch proof |
| Simulator policy mistaken for user delegation | Unauthorized access | Separate platform integration gate |
| Expected data follows implementation assumptions | Correlated mistakes | Review corpus independently with producer/consumer |
| Retention allows stale resurrection | Incorrect state | Retain tombstones until reconciled expiry is proven |

## Table-design spike evidence

SPIKE-001 locally compares generic bags, shared promoted columns and typed
serving tables with matching query-result digests. Adapter fixture shape checks
retain isolated A0 and four parallel two-hop paths. This is screening evidence,
not acceptance of US-002-AC6/AC7 or US-003-AC5: exact scalar corpus, native
Databricks lookup/query evidence and external engine execution remain required
for the corresponding full support claims, rather than the bounded design handoff.
Native DDL, exact-carrier and version-pinned read probes now pass on dbw-aidev-cus.
Bounded native query results are recorded in the native spike evidence, with the
singleton latency targets still missed or unproved in their recorded scopes. CONTRACT-003 supplies the physical surface. No full platform story criterion is marked covered by a local simulator.

## Build Handoff

The corpus is specification data. No implementation or behavioral test command
exists yet. Contracts are drafts; physical design, platform selection and UMF
integration remain explicit gates. All story checkboxes remain unchecked.


## Native layout evidence disposition — 2026-10-06

The [candidate package](../02-design/spikes/SPIKE-001-table-layout/layout-package-candidate.json) and [local audit](../02-design/spikes/SPIKE-001-table-layout/out/layout-package-audit.json) identify scoped native evidence. They do not mark this consumer corpus executed.

| Scope | Evidence | Disposition |
| --- | --- | --- |
| 0.2 / 0.3 table DDL | r65/r73 | Native executable shapes; no semantic-key enforcement claim |
| Typed identity, exact carriers, structural fixture | r66 | Tiny synthetic correctness; no real producer or billion-node claim |
| Pinned export/release projections | r67/r68 | Exact native projection parity; external engines unexecuted |
| Property tokens and complete uninterpreted raw kinds | r69/r71 | Exact synthetic storage; source semantics unqualified |
| Raw replay and conflict refusal | r72 | Single-table atomic MERGE control; concurrent uniqueness unproved |
| 0.3 cursor/reference publication | r74 | Actual invalid references detected before publication; not crash recovery |
| Native singleton and paged adjacency builders | r75/r76 | Generated bound query correctness; no new latency or service authorization claim |

Mandatory remaining integration evidence includes trusted complete-feed manifest coverage, bootstrap consistency, generation/registration fencing, source acknowledgement custody, concurrency/crash recovery, policy delegation and actual graph-engine releases. Historical performance shortfalls remain recorded measurements under the owner's fixed Unity Catalog Delta direction. No small fixture substitutes for the full corpus or full-scale admission.
