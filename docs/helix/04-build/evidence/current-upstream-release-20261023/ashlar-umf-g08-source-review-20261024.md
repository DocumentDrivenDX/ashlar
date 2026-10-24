---
ddx:
  id: ashlar.evidence.umf-g08-source-review-20261024
  type: evidence
  activity: build
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
    - id: CONTRACT-004
      kind: informed_by
    - id: CONTRACT-005
      kind: informed_by
---

# Ashlar–UMF G08 source compatibility review

**Review date:** 2026-10-24

**Ashlar source:** `c7cdafc13a1a7cee7efb6bad142dee2717b4e6e1`

**UMF source:** `DocumentDrivenDX/umf` commit `2ede4c879bea5db13794a6fa30e67c821306c17f`, `docs/helix/02-design/solution-designs/SD-007-schema-migrations.md`
**Scope:** Read-only source and contract comparison. No native, cloud or scale workload was run. SD-007 is a proposal; this note does not adopt it or qualify support.

## Finding

Ashlar has generic custody and publication components that can support a future adapter, but the reviewed source contains no schema-migration capability that transforms a candidate graph, validates its complete resulting population and activates an immutable successor publication. G08 remains open for Ashlar.

SD-007 G08 (lines 1436–1539) allocates exact before/after source interpretation, identity, absence semantics, dependency intent and versioned capability/refusal descriptions to UMF. Ashlar's backend must supply its native binding, complete population validation, physical strategy, authority, atomicity, publication/journal and recovery. For Ashlar, a supported change must construct a complete successor with exact schema/source and full table-version and progress vectors, apply the authored fill/conversion in the candidate projection, validate nodes, edges, endpoints, keys, multiplicity and retained unknown content, then activate while preserving old versions and decoding/binding custody. Source acceptance, Delta effects, activation and ACK remain separate boundaries with original-attempt recovery. Future-write defaults require an admitted writer or an explicitly selected projection behavior; otherwise Ashlar must refuse them. Date/birthday support cannot be inferred from prior Core 0.7/0.8 helper coverage.

## Source comparison

- [schema.py](../../../../../src/ashlar/schema.py) retains exact source and intake artifact bytes, checks their digest and identity, and records the validator revision. Its module purpose explicitly separates intake custody from target acceptance; it does not produce a migration diff or candidate projection.
- [typed_source_config.py](../../../../../src/ashlar/typed_source_config.py) admits a fixed JSONL typed-source profile tied to UMF 0.8.0. [typed_source_policy.py](../../../../../src/ashlar/typed_source_policy.py) bounds scalar bindings to integer, decimal, boolean and string; this is not date/birthday evolution support.
- [publication.py](../../../../../src/ashlar/publication.py) resolves an exact publication descriptor and pinned table versions through required authorization, descriptor-validation and snapshot-inspection ports, without latest fallback.
- [stored_publisher.py](../../../../../src/ashlar/stored_publisher.py) composes durable attempt phases and requires backend validation, manifest commit/recovery and ACK ports. This is generic publication scaffolding; the native effects and their complete validation remain host obligations.
- [truss_publication.py](../../../../../src/ashlar/truss_publication.py) preserves Truss checkpoint custody while leaving source admission, Delta effects, immutable publication and ACK to the backend.

The governing Ashlar drafts align with these boundaries: [CONTRACT-001](../../../02-design/contracts/CONTRACT-001-publication-boundary.md) separates accepted source progress and immutable publication outcomes; [CONTRACT-002](../../../02-design/contracts/CONTRACT-002-consumer-read-boundary.md) requires old field-definition pins and complete publication vectors for compatible reads; [CONTRACT-003](../../../02-design/contracts/CONTRACT-003-delta-graph-tables.md) says generated DDL does not imply migration and requires revision correspondence and preservation of the prior descriptor after partial effects; [CONTRACT-004](../../../02-design/contracts/CONTRACT-004-publication-resolver.md) requires exact retained snapshots and no latest fallback. [CONTRACT-005](../../../02-design/contracts/CONTRACT-005-schema-intake.md) explicitly separates intake from migration. Their proposed/draft status and existing evidence do not qualify the G08 adapter.

## Residual work before a positive claim

Keep G08 open until an Ashlar-owned contract and test allocation defines a closed, version-qualified adapter profile: exact Core/source/adapter/runtime tuple and retained before/after bytes; supported operations and explicit refusals; stable IDs and missing/null mapping; binding, dependency and loss/default intent; source-acceptance linkage; predecessor incarnation and complete vectors; candidate transform and physical strategy; writer fencing and activation atomicity; complete graph validation; old/new pinned-read evidence; future-write behavior; and original attempt, publication and ACK recovery, including schema-only/zero-graph-effect transactions. Evidence must exercise actual backend API boundaries and complete expected data, not direct SQL that bypasses acceptance. Keep preparation distinct from execution and qualification.

This review changed no contracts, implementation, or release status. It ran no native/cloud/scale activity and establishes no execution, publication, ACK, platform or production-authority qualification.
