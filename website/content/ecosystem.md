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
<div class="ecosystem-future"><strong>UMF</strong><span>Intended metadata binding · deferred</span></div>
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

**Evidence:** Spark 3.5.3 / Delta 3.2.1 / GraphFrames 0.12.3 local execution, including isolate, self-loop, parallel-edge and unpublished-append controls. A separate recorded local 4M-vertex/20M-edge integration counts 100M two-hop paths. These results qualify those local fixtures, not direct Unity Catalog protocol compatibility or billion-scale workloads.

[Version-pinned helper](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/adapters/graphframes.py) · [Local execution evidence](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/adapters/README.md#subsequent-actual-graphframes-evidence)

## PuppyGraph

A typed release maps node and edge tables to graph labels and selected scalar properties. Original identity columns may serve engine identity without being returned as ordinary properties, so the executed model includes explicit `carrier_id` and `carrier_key` duplicates for inspectable preservation.

For the common example, a Cypher relationship match returns two edges while a distinct destination query returns one B1. The local conformance fixture also includes A0 and a self-loop. This establishes the mapping behavior for that fixture, not release activation semantics.

**Evidence:** PuppyGraph 1.13.0 local engine, DuckDB carrier fixture and neo4j 5.28.2 Bolt client. The four typed-table checks preserve original IDs, property text including `9007199254740993`, retained content and endpoints. Direct Unity Catalog external-access prerequisites and Delta feature compatibility remain unqualified; an attempted in-place model change was rejected.

[Executed model](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/adapters/puppygraph-r105-model.json) · [Local carrier conformance](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/adapters/README.md#actual-puppygraph-1130-local-carrier-conformance)

## Fabric Graph

A bounded release selects nodes and endpoint-closed edges into OneLake-compatible projections. If A1 and B1 are selected, their included E1/E2 edges retain identity. If a selection excludes a destination, the omitted cross-boundary relationship becomes an explicit residual; no automatic graph federation is assumed.

The mapping plan is an Ashlar design artifact, not a Microsoft deployment API payload. String/value constraints need validation without truncation; the source's exact content remains recoverable through pinned canonical references.

**Evidence:** local mapping/export validation and native Delta projection materialization. Fabric engine deployment is unexecuted. The recorded source research bounds Fabric separately from the full 1B-node/5B-edge planning graph; current platform limits must be rechecked before a deployment.

[Projection plan](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/adapters/fabric-projection.json) · [Release evidence and limits](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/adapters/README.md#concrete-typed-release-plan)

## Truss and UMF

Truss is a related producer design, not a deployed Ashlar ingestion dependency. Its proposed feed adapter preserves exact transaction tuple cursors, complete transaction boundaries, replay origins and source authority. Property events alone may not contain enough structural information to reconstruct complete current carriers.

[Truss mapping candidate](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/truss-feed-mapping-candidate.md) records source-profile gaps and distinguishes committed upstream evidence from later working drafts.

UMF is DocumentDrivenDX's metadata metamodel and schema interchange fabric. Ashlar intends to use it for shared graph definitions while keeping graph-specific meaning in an explicit extension vocabulary. The exact binding, source versions and vocabulary are deferred; no working UMF adapter is claimed.
