---
title: One graph, explicit mappings
description: Canonical warehouse meaning can feed different query tools through qualified, versioned projections.
eyebrow: 05 / Ecosystem
composition: model-primary
nextPath: reference/
nextLabel: Follow the contracts and evidence
---

## The ecosystem map

<div class="ecosystem-map" role="group" aria-label="Ecosystem roles">
<div class="ecosystem-input"><strong>Truss / other producers</strong><span>Qualified feed, identity and ordering profile</span></div>
<div class="ecosystem-center"><strong>Ashlar on Unity Catalog Delta</strong><span>Current graph + source/history + publication pins</span></div>
<div class="ecosystem-outputs"><a href="#native-sql"><strong>Native SQL</strong><span>Pinned singleton and adjacency reads</span></a><a href="#graphframes"><strong>GraphFrames</strong><span>Local version-pinned DataFrame mapping</span></a><a href="#puppygraph"><strong>PuppyGraph</strong><span>Local typed carrier mapping</span></a><a href="#fabric-graph"><strong>Fabric Graph</strong><span>Bounded OneLake projection plan</span></a></div>
<div class="ecosystem-future"><strong>UMF / Weft</strong><span>Schema meaning and DDL / query compilation</span></div>
</div>

Arrows in this map represent intended data direction. Each release must identify exact versions, supported values, residual content, endpoint closure, refresh behavior and effective access policy. The map does not assert that every integration is deployed.

## A common example

Start with TypeA A1, TypeB B1, two independently identified relationships E1/E2 from A1 to B1, and isolated TypeA A0. A selected relationship property is `confidence`. Keep the exact canonical property bag and unknown content even when a consumer exposes only a scalar field.

Native IDs travel as decimal strings when needed. Graph keys encode source, type and identity injectively; they are not truncated hashes. An engine mapping must preserve the isolated vertex and edge multiplicity, or explicitly reject the source profile.

## Native SQL

The native read path selects a publication and queries canonical Delta at its pinned version. The singleton template combines a derived hash with full source/type/id predicates. Forward adjacency returns E1 and E2 as separate rows with B1's typed endpoint; bounded traversal uses the same publication context.

**Evidence:** native read-only query-builder and adjacency fixtures recorded in the [adapter evidence](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/adapters/README.md#portable-native-singleton-query-builder). Authorization and continuation custody remain caller-service responsibilities. This path works independently of Fabric; operational service targets are not qualified by these fixtures.

## GraphFrames

Vertices come from node tables, so A0 survives. Edges carry graph `src`/`dst` keys and a separate edge-key attribute, so E1 and E2 remain distinguishable. Prepared projection tables require their own release-version vector; canonical versions cannot substitute for their versions.

An illustrative graph question is a motif `(a)-[e]->(b)` filtered to A1. It has two relationship matches in this example. Distinct destination IDs have one result. Exact raw carrier strings remain available beside selected attributes.

Use `ashlar configure-graphframes-export` to select an immutable export, then `ashlar run-graphframes-export` for the bounded traversal corpus. The [Truss quickstart](https://github.com/DocumentDrivenDX/ashlar/blob/main/examples/truss-native/QUICKSTART.md) connects source ingestion, publication and export. [Parallel-edge execution evidence](https://github.com/DocumentDrivenDX/ashlar/tree/main/docs/helix/04-build/evidence/truss15-parallel-graphframes-20261013) records the actual runtime and supported local subset; direct Unity Catalog access requires its own qualification.

## PuppyGraph

A typed release maps node and edge tables to graph labels and selected scalar properties. Original identity columns may serve engine identity without being returned as ordinary properties, so the executed model includes explicit `carrier_id` and `carrier_key` duplicates for inspectable preservation.

Cypher and Gremlin must preserve independently identified parallel edges, isolated vertices and reuse of an edge in different hop positions. Path multiplicity and distinct destinations answer different questions. Compare complete canonical rows and native identities before and after queries; carrier parity alone cannot prove endpoint preservation.

The [immutable PuppyGraph guide](https://github.com/DocumentDrivenDX/ashlar/blob/main/examples/puppy-immutable-export/README.md) runs either language from retained release/custody files without a running source or Spark process. Its [Cypher](https://github.com/DocumentDrivenDX/ashlar/tree/main/docs/helix/04-build/evidence/truss15-puppy-immutable-cypher-20261013) and [Gremlin](https://github.com/DocumentDrivenDX/ashlar/tree/main/docs/helix/04-build/evidence/truss15-puppy-immutable-gremlin-20261013) evidence qualify the recorded local release, image and clients. Property JSON remains opaque; numeric filters, cloud access and production authority need separate qualification.

## Fabric Graph

A bounded release selects nodes and endpoint-closed edges into OneLake-compatible projections. If A1 and B1 are selected, their included E1/E2 edges retain identity. If a selection excludes a destination, the omitted cross-boundary relationship becomes an explicit residual; no automatic graph federation is assumed.

The mapping plan is an Ashlar design artifact, not a Microsoft deployment API payload. String/value constraints need validation without truncation; the source's exact content remains recoverable through pinned canonical references.

**Evidence:** local mapping/export validation and native Delta projection materialization. Fabric engine deployment is unexecuted. The recorded source research bounds Fabric separately from the full 1B-node/5B-edge planning graph; current platform limits must be rechecked before a deployment.

[Projection plan](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/adapters/fabric-projection.json) · [Release evidence and limits](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/adapters/README.md#concrete-typed-release-plan)

## Truss and UMF

Truss supplies accepted catalogs, typed mutations and committed source events. Its adapter must preserve transaction boundaries, original IDs, replay and source authority. Property events alone cannot replace complete current carriers. Follow the [typed Truss quickstart](https://github.com/DocumentDrivenDX/ashlar/blob/main/examples/truss-native/QUICKSTART.md) for the supported local workflow; local feed progress does not establish a protected production ACK.

UMF owns schema interpretation and reusable Delta DDL generation. Ashlar's [runtime models](https://github.com/DocumentDrivenDX/ashlar/tree/main/docs/helix/02-design/models/ashlar-delta-runtime) define the canonical carriers with versioned Delta extensions. Explore them in the [schema browser](../model/schema-browser/index.html#schema=ashlar-runtime-logical). Preserve each source document's declared version and unknown content rather than relabeling it for a consumer.

Weft owns query compilation. Ashlar binds its original compiled artifacts to one readable publication, executes emitted integrity obligations and preserves exact values and multiplicities. The [query installation guide](https://github.com/DocumentDrivenDX/ashlar/blob/main/examples/end-to-end/WEFT-INSTALLATION.md) distinguishes supported indexed distributions from public wheel interfaces; availability of a compiler for another backend does not establish Ashlar support.
