---
title: "Why this structure"
description: "Canonical carriers preserve meaning. Serving projections make selected graph workloads practical."
eyebrow: "03 / Background"
composition: model-primary
nextPath: ecosystem/
nextLabel: "Explore the ecosystem mappings"
---

## One generic canonical surface

Ashlar's design starts with generic object and edge tables. Consumer-defined types and properties do not require an industry-specific schema in the core. This follows the goal of a domain-independent property-graph toolkit and stays close to Truss's generic object/edge representation.

Typed canonical tables for every domain type would make scalar filtering convenient, but would couple canonical migrations to every type's evolution. Shared promoted columns would create a wide sparse schema across unrelated types. The proposed compromise keeps exact generic carriers and adds selected scalar projections.

## Exact text before convenient scalars

Warehouse scalar types cannot represent every source value or distinction. Exact JSON carriers preserve lexical numbers, explicit null, missing properties and unknown extension content. A useful scalar projection may be incomplete; its release must name what is omitted and retain a pinned canonical recovery path.

The property journal is a separate history role. Using property-event rows as the only current representation would require reassembly and additional joins for ordinary object reads. The candidate uses current tables for current state and a journal for accepted history.

## Identity lookup and traversal are different workloads

`lookup_hash` is derived from the exact identity tuple. Identity-oriented liquid clustering is the proposed physical candidate for singleton lookup. The native tuple remains in the query predicate: the hash is neither semantic identity nor uniqueness enforcement.

Forward adjacency groups a narrow structural projection around the source endpoint. Reverse adjacency is a separately selected projection. Edge identity remains present in both so endpoint-parallel relationships do not collapse.

Physical tuning is independent of logical meaning. The initial 64 MiB file target and unpartitioned hash clustering are candidates informed by recorded experiments, not promised file sizes or a universal performance result.

## Raw deliveries, events and deletion each have a role

`source_record` retains the original envelope, cursor and digest. `property_journal` records accepted property transitions and presence flags. `tombstone` preserves typed deletion identity and version. Parsing a raw envelope into current fields does not replace the raw record; a deletion marker alone does not prove protection against stale resurrection.

Producer ordering, authority, replay conflict rules, transaction completeness and retention need an explicitly qualified source profile.

## Visibility follows a validated publication

A manifest holds an immutable table-version vector. Readers pin this vector while physical maintenance may advance table heads independently. Publication occurs only after the required role content, endpoint integrity, revisions and custody have been validated.

Production writer fencing, crash recovery and effective caller policy remain integration obligations. The descriptor representation provides a boundary to implement; it is not proof that every production mechanism already exists.

## Trade-offs and current limits

Extra raw/history and serving roles cost storage and validation work. Optional projections need synchronization and their own release versions. More projections should be selected because a workload needs them, with explicit coverage and preservation checks.

Unity Catalog Delta is the owner-selected storage architecture. Measured latency and freshness misses inform tuning; the design milestone's closure does not convert them into passed operational targets. Billion-node capacity remains unproved, and UMF binding is deferred.

Read the [physical-layout decision](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/adr/ADR-001-delta-canonical-and-serving-layout.md) and [table handoff](https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/table-design-handoff.md) for the candidate's alternatives and evidence.
