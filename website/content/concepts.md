---
title: "The graph model"
description: "Typed identities and property-bearing relationships give different producers a shared structure."
eyebrow: "01 / Concepts"
composition: model-primary
nextPath: schema/
nextLabel: "Explore the physical schema"
---

## Entities have typed identities

A canonical node is identified by `(source_system, type_id, id)`. The same numeric ID can occur in different types or sources without becoming the same node. An isolated node remains in `object_current`; the node inventory is never inferred solely from edges.

Ashlar's candidate table profile uses signed 64-bit integer type and object IDs. A consumer transporting those IDs through JSON uses exact decimal strings where numeric precision could be lost.

## Relationships have identities of their own

An edge is identified by `(source_system, rel_type_id, id)` and carries both typed endpoint tuples. Its properties describe the relationship itself. Two relationships between the same endpoints can remain separate when the producer profile permits them; a producer may also require unique relationship/endpoint combinations.

Example: TypeA A1 relates to TypeB B1 twice, through E1 and E2. A query counting relationships returns two. A query returning distinct destination nodes returns B1 once. Those are different questions.

## Properties preserve their original meaning

`props_json` retains the property-ID map as text. `retained_json` carries source content that a selected consumer projection does not understand. Missing, explicit null, exact decimal tokens and large integers must remain distinguishable. A scalar serving projection exposes only the properties its declared profile can represent.

A projected `group_value` is paired with `group_present` when the consumer must distinguish absent from null. Unsupported conversions are refused or recorded as an explicit projection residual; they are never silently rounded or truncated.

## A publication is a pinned view

Independent Delta tables can advance at different times. Consumers select a validated immutable publication descriptor containing exact table identities, versions, source progress and schema revisions. They read those versions rather than following whichever physical heads happen to be latest.

The publication boundary does not turn independent table writes into a cross-table transaction. It controls which completed, validated vector consumers may select.

## Read the precise rules

[Table contract](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-003-delta-graph-tables.md) · [Consumer read boundary](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-002-consumer-read-boundary.md)
