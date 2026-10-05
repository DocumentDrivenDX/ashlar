# Discovery input: publication from a hot transactional store

Proposed 2026-10-05, for the owner's review. This is an input, not a decision: it
proposes answers to open questions Q2, Q3 and Q5 and adds requirements that a
hot transactional store feeding Ashlar and an application reading Ashlar need.
It names no particular consuming application.

## Context

The build brief puts a Postgres/Lakebase layer under Truss as the hot store,
makes its changelog both the audit trail and the replication feed into Delta,
and gets low-latency visibility of a change from the hot store, not from a batch
rewrite of gold. Truss now specifies that feed (its `CONTRACT-006`) and a layout
whose identity rules are stated. An application that reads one ontology through
two stores, a fresh hot store and gold, needs Ashlar to state what it publishes
and how current it is. These requirements come from drafting twelve concrete
interactions for such an application: browse and filter, open a record with its
related records, aggregate counts, a bounded two-hop pattern, "what changed since
I last looked", reads on behalf of a signed-in user, and behavior when a store is
unavailable.

## Proposed requirements

### P1. Publication semantics (answers part of Q3; refines FR-2)

- Ashlar consumes an ordered, replayable change feed from a hot store. A change is
  applied idempotently by `(entity kind, entity id, version, property)`; applying
  a record twice changes nothing; restarting from an earlier feed position
  reproduces the same gold state.
- A delete is a first-class change. It removes the entity from current state and
  is kept as a tombstone with its version, so a later feed record or bulk load
  cannot silently bring it back.
- A schema revision is applied before the first change that uses it. A revision
  Ashlar cannot support stops publication at that revision and is reported; it is
  never skipped.
- A late record is applied by version, not arrival order: an older version never
  overwrites a newer one.

### P2. Identity (answers Q2)

- A node's identity is its UMF type and its primary key value, not a generated
  surrogate. The same entity in the hot store and in gold has the same identity.
- An edge's identity is its relationship, its source identity and its target
  identity. At most one edge exists for that triple. A model that needs several
  distinguishable links of one kind between the same two entities declares an
  association entity or a second relationship.
- This is what the hot store enforces, so gold can rely on it rather than detect
  duplicates after the fact.

### P3. Current state and history (answers the rest of Q3)

- Gold holds current state and, per entity, the versions it has been through, each
  with the feed position and time at which it became current, so a reader can ask
  for the current value, the value at a version, or the changes since a position.
- Retention of history is a stated policy, not an accident of storage.

### P4. Publication position and freshness (adds to FR-3's consistency limits)

- Gold exposes, per source feed, the position it reflects and when that was
  recorded. A reader can state "this answer reflects the source through position
  P", and ask for an answer that reflects at least a given position.
- Freshness is measured and reported, not assumed. How quickly a change reaches
  gold depends on the transport, for example a managed synchronization from the
  hot store, and a low figure should be stated only after it has been measured.
- A read never silently mixes feed positions across tables; when a mixed answer is
  possible it is labeled.

### P5. Read workload shapes (supplies input to Q5)

The shapes a first consuming application needs are: lookup by identity or key;
a bounded filtered list; a bounded one-hop and a fixed multi-hop pattern with a
depth limit and a truncation marker; a small set of bounded aggregates (count by a
property, count by a related entity's property); and "changes since a position".
Every shape is bounded, and every result carries its feed position. Latency
budgets are the owner's and the pilot's to set. The application's own needs are
interactive for lookup and list, and a few seconds for aggregates and multi-hop.

### P6. Access on behalf of a user (supports Q6)

- A read can be made on behalf of a named end user, with Unity Catalog row and
  column policy applied to that user, not only to a service identity.
- A caller can tell three outcomes apart: the data is empty, the user is not
  permitted, and the store is unavailable. A refusal and an outage are never
  confused, because a caller that fails closed treats them differently.

### P7. Shared conformance

A producer, Ashlar and a consumer share a corpus: for a given feed, the gold state,
the identity rules, the delete behavior and the reported position are specified as
data, so any producer or any gold implementation can be tested against it.

## Questions this input leaves open

- Which feed transport the first pilot uses, and its measured end-to-end freshness.
- Whether gold keeps all versions or a bounded window.
- Which aggregates are required, if any, beyond counts.
- Whether Ashlar publishes type definitions as UMF so a reader can discover the
  ontology's types without a separate registry.

## Where this should land

If the owner agrees, P1 to P4 become requirements on FR-2 and FR-3 and answers to
Q2 and Q3; P5 and P6 inform Q5 and Q6; P7 extends the shared conformance corpus
named in the vision's north star.
