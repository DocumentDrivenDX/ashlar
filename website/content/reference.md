---
title: "Contracts & evidence"
description: "Follow each design claim to its exact boundary, version profile and evidence."
eyebrow: "06 / Reference"
composition: model-primary
nextPath: start/
nextLabel: "Inspect the candidate package"
---

## Governing contracts

| Boundary | What it defines | Source |
| --- | --- | --- |
| Publication | Ordering, replay, deletion, progress and visibility | [Publication contract](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-001-publication-boundary.md) |
| Consumer reads | Pinned consistency, access and failure outcomes | [Read contract](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-002-consumer-read-boundary.md) |
| Delta tables | Proposed ashlar-delta/0.3 tables, keys and mappings | [Table contract](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-003-delta-graph-tables.md) |
| Resolver | Proposed ashlar-resolver/0.1 resolution/refusal behavior | [Resolver contract](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-004-publication-resolver.md) |

## Package and implementation

- [SQL package](https://github.com/DocumentDrivenDX/ashlar/tree/main/sql/ashlar-delta-v03): selected baseline and optional DDL, manifest and pinned read templates.
- [Resolver candidate](https://github.com/DocumentDrivenDX/ashlar/blob/main/src/ashlar/README.md): standard-library core and injected native backend ports.
- [Graph adapter evidence](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/adapters/README.md): exact local versions, fixtures and remaining native integration boundaries.

## What the closed milestone establishes

The [2026-10-08 physical-layout acceptance record](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/layout-milestone-acceptance.md) closes the table-design/schema-package milestone for its named synthetic scope. It records concrete definitions, preservation and endpoint evidence, completed serialized synthetic publication, native lookup feasibility and scoped mappings.

Its package review checks hashes and membership offline. Extracted package files were not newly deployed by that review. The original private native CREATE and query evidence has its own version/subset boundaries.

## What remains open

Production source authority, concurrent fencing, authorization delegation, retained snapshot custody, crash recovery and engine-specific deployment require separate evidence. Operational latency/freshness targets remain unmet or unproved for their declared scope. Sustained throughput and 1B-node/5B-edge capacity are not admitted runtime claims.

UMF integration remains intended; the exact graph binding is deferred. License and release distribution remain owner decisions. No installable release is implied by this documentation site.

## Provenance of these pages

Each page is prepared for an Innsigle source seal. The visible colophon identifies declared composition and links the signed attestation and issuer keys. Source signing accounts for authorship and byte integrity; it does not certify the correctness or performance of Ashlar.
