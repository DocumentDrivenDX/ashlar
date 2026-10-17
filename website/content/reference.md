---
title: "Contracts & evidence"
description: "Follow each design claim to its exact boundary, version profile and evidence."
eyebrow: "06 / Reference"
composition: model-primary
nextPath: start/
nextLabel: "Use the toolkit"
---

## Governing contracts

| Boundary | What it defines | Source |
| --- | --- | --- |
| Publication | Ordering, replay, deletion, progress and visibility | [Publication contract](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-001-publication-boundary.md) |
| Consumer reads | Pinned consistency, access and failure outcomes | [Read contract](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-002-consumer-read-boundary.md) |
| Delta tables | Managed Delta tables, keys, UMF models and graph projections | [Table contract](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-003-delta-graph-tables.md) |
| Resolver | Immutable publication resolution and refusal behavior | [Resolver contract](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-004-publication-resolver.md) |

## Package and implementation

- [SQL package](https://github.com/DocumentDrivenDX/ashlar/tree/main/sql/ashlar-delta-v03): selected baseline and optional DDL, manifest and pinned read templates.
- [Toolkit components](https://github.com/DocumentDrivenDX/ashlar/blob/main/src/ashlar/README.md): portable source, schema, publication and guarded-query interfaces with injected native transports.
- [Local setup and queries](https://github.com/DocumentDrivenDX/ashlar/blob/main/examples/end-to-end/README.md): explicit source, runtime, compiler and publication inputs.
- [Canonical UMF models](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/models/ashlar-delta-runtime/README.md): source models, extension meanings and checked DDL/browser generation.

## Preserve meaning across boundaries

UMF owns reusable schema interpretation, validation and DDL. Ashlar bindings must preserve original declared versions, unknown extensions, exact values and separate absent/null/value states. Explicit extensions carry Delta-specific meaning; logical relationships express publisher obligations rather than warehouse-enforced foreign keys.

Weft owns compilation. Ashlar must execute original compiler SQL and every admitted integrity obligation against the entire resolved publication vector. Unsupported meaning, drift, expired snapshots or missing authority must produce an explicit refusal. Engine projections must preserve qualified identities, independent edges and multiplicity, and declare any capability loss.

## Read evidence at its declared scope

Each support claim needs the exact source/model revision, native versions, selected semantic subset and original observations. Preparation, native execution, source acknowledgement and production authority are distinct proofs. A successful local query cannot supply real Truss catalog acceptance or Fabric access.

The [native GraphFrames original-scenario evidence](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/04-build/evidence/original17-graphframes-native-20261009/summary.json) qualifies typed in-memory graph projections and seventeen original relational scenarios over immutable Delta-export provenance. The [ecology DISTINCT evidence](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/04-build/evidence/original-pack-weft-distinct-native-20261009/README.md) records one publication-bound original Weft query; its separate controls have their own authority limits. Consult each receipt before generalizing its result.

The [layout acceptance record](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/layout-milestone-acceptance.md) retains the measured physical-layout scope. Latency and throughput measurements guide tuning of managed Delta; they do not replace identity, preservation or publication requirements or prove billion-node capacity.

## Provenance of these pages

Each page carries an Innsigle source seal. The visible colophon identifies declared composition and links the signed attestation and issuer keys. Source signing accounts for authorship and byte integrity; it does not certify the correctness or performance of Ashlar.
