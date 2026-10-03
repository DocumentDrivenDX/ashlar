# Generalized build brief

Technical starting context adapted to the owner's current direction: Ashlar is
an open-source, domain-independent warehouse ontology toolkit. Domain-specific
examples from the original input have been removed. This is an adapted brief,
not a verbatim archive; the focused PRD governs the first milestone.

## What we are building

A metadata-driven property-graph toolkit for user-defined ontologies across
domains. Scale aspiration is up to a couple of billion nodes, with skewed
connectivity and many isolated nodes. Databricks is the first warehouse target:
gold Delta tables are the official ontology for reads and analytics. Hot
transactional systems of record remain external. Curated entities and links may
be owned transactionally; other relationships arrive from high-change sources.
No application-domain types are built into Ashlar.

The portable artifact is UMF (Universal Metadata Format), at `github.com/DocumentDrivenDX/UMF` on `master`. It is a machine-readable metamodel and schema interchange fabric: logical, semantic, and physical representations, with projections that retain the source and report losses. It is experimental (core around 0.6), TypeScript on Bun, with native oracles and Chromium checks. TableSpec is the production proof that generative pipelines beat handwritten ones; UMF grew out of it. Axon and Truss are the two storage designs that consume it. They are not the same engine.

## Model

Labeled property graph: typed entities (nodes) and typed relationships (edges),
both with properties. RDF triples, SPARQL, and OWL runtime are excluded.
External vocabulary identifiers and tags may be retained for interoperability.
Relationships can carry dependency and provenance information, including source
system, timestamp, and confidence. Domain vocabularies are supplied by consumers.

Vocabulary in UMF should move from table/column to entity/property. They are the same concepts; the storage metaphor is the wrong one for an ontology. Relationship inclusion in a graph projection is a hint on the property or relationship, not a separate schema.

## Physical split

Official ontology: gold Delta tables. Optimize read latency there. Non-batched updates do not land directly in Delta.

Hot transactional layer: Postgres, preferably Lakebase (managed Postgres inside Databricks: branching, scale-to-zero, Unity Catalog). Do not put Datomic under this. The datom model is attractive for audit and time travel, but a transactor plus Postgres-as-dumb-log is worse operationally than staying in Postgres. Apache AGE is a Cypher-to-SQL extension on vertex/edge tables; it does not give index-free adjacency, and Lakebase will not take custom extensions. Skip it.

Two designs already exist:

- Axon: single record, JSON property bag, changelog and rollback first. Currently on its own EMF fork of UMF.
- Truss: EAV, separate property tables, indexes, less audit emphasis. This is the engine to implement against a cleaned-up UMF.

Hybrid that both should converge on: typed indexed columns for hot fields, JSONB for the long tail. Pure EAV is fine for point lookup and bad for joins; a pure bag is easy to read and hard to index.

Changelog rows in Postgres are both the audit trail and the replication feed into Delta. Low-latency visibility of changes on the gold side comes from Lakebase, not from waiting on a batch rewrite of the whole ontology.

## What UMF must own

Already in flight: Field, nullability, cardinality, facets, keys, relationships, constraints (TableSpec’s Great Expectations–style rule language), storage hints (hybrid column, index direction), and projection policies. Round-trip oracles are the point: map out, map back, name the exact semantic loss. There are already on the order of a thousand tests. That harness is what makes agent iteration safe.

Still missing, and in scope for the spec rather than a side project:

- Governance. Visibility and access as metadata: public, private, group, high-visibility. Core or extension, but declared in UMF.
- Actions. A declarative action type: name, agent-facing description, typed parameters, return or handle, preconditions, effects (which properties change, which entities or links are created or deleted), side effects. This is the CQRS payload. Execution stays in Truss. Palantir’s action types are the reference shape: rules for simple edits, function-backed only when rules are not enough, submission criteria, read/write authorization bounds, and a tool description meant for agents.
- Compensation, not just idempotent retry. Optional per action or per step: if this fails, undo that. MassTransit-style sagas are the prior art for long-running work: submit, get a handle, track state, compensate in reverse. Synchronous call-and-block is the special case.
- Provenance and version on values and edges.

Do not pretend the spec captures domain meaning. "Assign employee" is not the same kind of write as "touch last-login." The implementation remains the accurate semantics; UMF is the contract and the loss report.

## Query and agent surface

Not Hasura. Hasura introspects physical Postgres tables. Here the logical schema is declared once in UMF, Truss owns the physical layout, and a compiler emits queries and mutations from the UMF schema. Closest prior art is Salesforce Force.com: SOQL/SOSL over a hybrid EAV store with per-type index tables, virtual schema compiled to physical SQL. UMF is the open version of that declaration.

GraphQL is a generated API over Truss: queries from entities and relationships, mutations from UMF actions, changelog as the write log. A later UMF-to-GQL compiler targets Microsoft Fabric Graph (ISO GQL, public preview, Cypher-like patterns) and similar engines: gold projection plus the graph type definition, with a hint for which fields and relationships belong in that projection. Neo4j remains a possible projection, not the system of record.

MCP and a Django-admin-style walker are consumers of the same metadata: fields, display hints (currency prefix, rounding), actions as tools. Palantir’s Object Explorer / Object Views / Ontology MCP are the product analog, not the architecture to copy.

## Explicitly out of the first cut

NetworkX and GraphFrames are compute libraries, not stores. RDF/OWL/SHACL/Turtle stay as interchange (UMF already has fixtures), not the runtime. Multi-hop graph algorithms, saga orchestration inside the schema, and a full Palantir kinetics layer (notifications, external writeback webhooks, scenario merge) wait until actions and Truss writes are real.

## Sequencing

1. Clean UMF: drop single-use TableSpec weirdness, lock entity, property, relationship, constraint, storage-hint, and action contracts. Forks in Axon and TableSpec are a divergence tax; converge them onto this spec.
2. One storage engine: Truss on Postgres/Lakebase, UMF-driven DDL and DML, hybrid columns plus JSONB, changelog as audit and Delta feed.
3. Generated GraphQL (or equivalent) whose mutations are UMF actions.
4. UMF-to-GQL / Fabric projection for the gold graph.
5. Agent and MCP surface on top of the stable contracts.

Query-compiler and index tuning are ongoing and agent-friendly once those boundaries are stable. TableSpec already showed that generative pipelines can beat handwritten ones in production; that is the bet, not a new ontology product competing with Salesforce or Palantir. Open contracts, Postgres, Databricks, and a spec with lossy round-trips are the wedge. The risk is facade-before-engine, not scope.