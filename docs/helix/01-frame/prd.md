---
ddx:
  id: ashlar.prd
  type: prd
  activity: frame
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.product-vision
    kind: informed_by
kind: data
---

# Product Requirements Document

## Summary

Ashlar is an open-source, domain-independent toolkit for building and using
property-graph ontologies on data warehouses. Databricks is its first target.
The first milestone gives Databricks platform engineers a reviewed path from
DocumentDrivenDX's [UMF metamodel](../00-discover/resources/umf.md) to gold
Delta tables representing a labeled property graph, then to bounded graph
queries. Nodes and edges have types and properties; RDF triples and an RDF
runtime are excluded. Delta is the warehouse storage format selected by the owner.

The deliverable is an end-to-end toolkit: UMF definitions drive admitted graph
schemas and UMF-generated Delta DDL; users ingest Truss and additional sources,
evolve schemas and query immutable publications. Domain-pack examples must show
exact typed values, relationships and source lineage through Weft and bounded
releases to Microsoft Fabric Graph, GraphFrames and PuppyGraph. A source or engine
that lacks a usable external interface must not prevent independent lanes.

Success requires zero unexplained mapping loss, valid table creation on a pinned
Databricks target, correct replay/deletes/history/publication boundaries and exact
expected results for the supported query corpus. Real Truss acceptance/feed/ACK
and native engine execution require their own evidence; a local database,
simulator, export or model validation cannot substitute for those workflows.

Latency, freshness, throughput and the 1B-node/assumed 5B-edge planning scale are
operational qualification targets, not architecture gates. Reuse existing
measurements and keep functional checks small. Explicitly requested scale trials
require dedicated compute, bounded stages and an approved resource/cost budget.
Production source authority, fencing,
authorization and retention requirements remain mandatory for the corresponding
support claim. Implementation status and results belong in the
[execution plan](../04-build/end-to-end-plan.md) and build evidence.

## Problem and Goals

### Problem

Source systems describe entities, properties, and relationships independently.
Without shared identity, endpoint, and property rules, consumers can join the
wrong records, lose isolated nodes, or trust declared constraints that are not
enforced. This is a cross-domain integration failure mode; its frequency and cost
have not been measured with a first user.

### Goals

1. Engineers can explain how each selected logical type becomes warehouse data.
2. Engineers can create tables with explicit preservation and enforcement limits.
3. Analytics engineers can answer a bounded relationship question using an
   evidenced Databricks-compatible execution path.

### Success Metrics

Required acceptance targets; observations and support limits are recorded in build evidence.

| Metric | Target | Baseline | Measurement Method | Cadence |
| --- | --- | --- | --- | --- |
| Semantic accountability | 100% selected constructs mapped, rejected, or explicitly reported; zero unexplained losses | Evidence scoped per profile | Reviewed source/target/recovery corpus | Each profile revision |
| Schema usability | 100% example DDL creates and loads valid fixtures on one pinned target | Evidence scoped per target | Databricks execution records | Each schema revision |
| Query correctness | 100% expected result identities and multiplicities match | Evidence scoped per query subset | Synthetic result-set oracle | Each query/layout revision |
| Invalid-data detection | 100% authored integrity counterexamples detected | Evidence scoped per validation subset | Negative corpus | Each validation revision |

Latency, freshness, throughput and cost support claims require explicit
budgets and measured evidence agreed with the pilot owner. The owner selected a 1B-node planning target with more edges on 2026-10-05 and
asked for fast singleton lookup directly on Databricks. Provisional experiment
budgets are recorded in SPIKE-001: warm native engine p95 100 ms and caller p95
250 ms for singleton lookup, with cold-data caller p95 1 s on running compute.
They are design targets; measured results and hardware/cost limits must be reported separately.
The planning graph uses 5B edges as an explicit assumption, not measured input.

### Open-source scope

Ashlar must be usable across application domains. Domain types and relationship
vocabularies come from consumer-authored UMF models; the core must not require
any industry-specific schema. The intended open-source deliverable includes
source, schema specifications, synthetic examples, and reproducible validation
instructions. License and publication details require a separate decision before
release; this draft does not select a license or claim public availability.

### Non-Goals

Implementing transactional engines, production Postgres/Lakebase hosting,
Truss/Axon convergence, unrelated UMF core cleanup, action execution,
compensation/sagas, generated GraphQL, MCP, admin UI, production graph-service
operation, RDF/SPARQL/OWL runtime and automated entity resolution are deferred.
Standing up the selected Truss runtime and bounded graph-engine integration
experiments are required toolkit workflows. Broad graph-algorithm service
guarantees remain outside this scope. Exact future boundaries and
re-entry conditions are recorded in the scope disposition in `research.md`
under Discover. A full ontology product is not required to prove the schemas.

## Users and Scope

### Data Consumers

| Consumer / persona | Use case | Freshness / latency | Key dimensions | Access |
| --- | --- | --- | --- | --- |
| Warehouse platform engineer | Review logical types and create trustworthy gold tables | Per schema release; budget open | Type, identity, source, schema revision | Synthetic sandbox initially |
| Ontology analytics engineer | Follow TypeA–TypeB–TypeC and typed one-hop connections | Ad hoc; budget open | Node/edge identity, direction, provenance | Synthetic sandbox initially |

These are roles from the brief, not validated named users. Platform and analytics
owners must be named before production adoption.

### Data Sources

| Source | Data | Owner | Update frequency | Quality baseline |
| --- | --- | --- | --- | --- |
| Synthetic domain-neutral fixture | TypeA, TypeB, TypeC and typed links | Ashlar design author | Deterministic revisions | Known expected results and counterexamples |
| Future operational systems | User-defined entities and relationships | Owner to identify | High change rate in brief; unmeasured | Unknown |
| Future curated source | User-defined curated types and relationships | Transactional owner to identify | Unknown | Unknown |

Use bounded synthetic fixtures, pinned UMF domain-pack datasets and documented
additional-source adapters for reproducible proof. Retain original model/data
bytes, hashes, notices and semantic mapping decisions. Existing source systems
remain the write owners. Ashlar must implement incremental publication, replay,
deletion and visibility semantics without implementing a transactional engine.

## Requirements

P0 consists of FR-1 through FR-4 below. The supported Weft query path and bounded
Fabric Graph, GraphFrames and PuppyGraph integration experiments are required.
An unavailable engine must have an explicit lane-specific prerequisite and must
not block the remaining source/schema/query work.

## Functional Requirements

### Subsystem: UMF graph profile

- **FR-1 (P0)** — Engineers must define typed nodes and property-bearing typed
  relationships from a pinned UMF subset, retaining logical identity, source
  metadata, and explicit projection losses. Graph-specific semantics must be
  isolated in a versioned extension when existing UMF concepts are insufficient.
  Governed by FEAT-001 and US-001.

### Subsystem: Databricks schema package

- **FR-2 (P0)** — Engineers must obtain a reviewed gold Delta schema package
  sufficient to create tables, represent isolated nodes and typed edges, validate
  integrity, and explain evolution and incremental publication semantics.
  Every constraint must identify its actual enforcement boundary. The physical
  model must be authored as UMF with explicit versioned extensions for Delta
  meaning; UMF must generate its DDL and supply the microsite diagram model.
  Governed by FEAT-002 and US-002.

### Subsystem: Graph query path

- **FR-3 (P0)** — Analytics engineers must have a reproducible query path over
  the selected schema for node lookup, filtered one-hop traversal, and a fixed
  two-hop pattern, with explicit direction, multiplicity, consistency, and
  supported-target limits. The potential-consumer workload also requires bounded
  filtered lists, counts, changes-since reads, publication progress and explicit
  access/failure outcomes. Evaluate existing tools before adding a compiler.
  Governed by FEAT-003 and US-003.

### Subsystem: Incremental gold publication

- **FR-4 (P0)** — Platform engineers must specify reproducible publication from
  an external replayable feed, including version ordering, deletion, revision
  barriers, retained history, and observable publication progress. Consumers
  must be able to assess whether a result includes their required source
  position. Governed by FEAT-004 and US-004.

### Consumer input and specification boundary

The [potential-consumer input](../00-discover/hot-store-publication-input.md)
(P1–P7) informs this draft. Its merge records discovery evidence, not approval
of every proposed identity or enforcement rule. Ashlar publication and read
behavior can be specified using opaque logical identities, revisions and source
positions while UMF (DocumentDrivenDX’s metamodel and schema interchange fabric)
evolves. Exact UMF constructs, extension vocabulary, serialized feed formats,
physical columns and platform mechanisms are governed by pinned downstream
contracts and compatibility profiles; unsupported meaning requires explicit
retention/loss or refusal.

P2’s single-edge triple is a candidate producer profile. It does not establish a
universal Ashlar identity rule or permit loss of distinguishable parallel edges.
Producer enforcement requires evidence and validation at the publication boundary.

### Data Quality Requirements

| Dimension | P0 threshold | P1 threshold | Measurement | Enforcement requirement |
| --- | --- | --- | --- | --- |
| Identity completeness | Zero missing required identities in accepted fixtures | No weaker tier | Required-value checks | Block acceptance of invalid snapshot |
| Uniqueness | Zero duplicate canonical node or edge identities | No weaker tier | Duplicate grouping | Detect before acceptance; do not rely on informational keys |
| Endpoint integrity | Zero unexplained dangling/wrong-type endpoints in accepted fixtures | No weaker tier | Endpoint/type reconciliation | Reject or quarantine with explicit publication policy |
| Preservation | Zero unreported property/provenance losses | No weaker tier | Source/target comparison | Block unsupported silent coercion |
| Timeliness | Every example identifies its source and published revision | No weaker tier | Snapshot lineage review | No cross-table consistent-read claim without proof |

Exact checks and publication rules belong in the subsequent schema contract
and data-quality expectations; no executable enforcement is claimed here.

## Acceptance Test Sketches

| Requirement | Scenario / input | Expected outcome |
| --- | --- | --- |
| FR-1 | TypeA A1, TypeB B1, TypeA A0 isolated, directed A1→B1 edge with source and confidence; unsupported authored rule | All identities, types, values and unsupported meaning remain accounted for; source-only recovery distinguished from target recovery |
| FR-2 | Create and load that model; inject duplicate A1, missing B9 endpoint, null identity, and a breaking property type change | Valid fixture accepted; each invalid case identified; breaking change requires explicit migration; keys not represented as enforced without evidence |
| FR-4 | Publish create, update, duplicate replay, older update, delete, interrupted publication and unsupported revision | Replaying yields the same state; old records do not overwrite or resurrect; progress advances only through complete supported publication; history and gaps are explicit |
| FR-3 | A1→B1→C1 and A1→B1→C2, unrelated A2→B2→C3, isolated A0 | Two-hop target-node query yields C1 and C2 only; A0 remains directly queryable; parallel-edge and cyclic variants have declared results |

## Physical design direction

Owner direction (2026-10-05): keep tables as close as practical to Truss while
providing straightforward mappings to PuppyGraph, GraphFrames and Microsoft
Fabric Graph. Native Databricks singleton reads must work independently of Fabric,
using measured partitioning/Z-order or liquid-clustering settings. Mapping
experiments are in scope; full production graph deployments remain deferred.

The full 1B-node graph and a bounded external projection are separate support
claims. A bounded Fabric release cannot establish
support for the complete 1B-node graph plus more edges. Source meaning and omitted cross-scope
relationships must be accounted for in every projection.

## Technical Context

Databricks gold Delta and UMF integration are owner constraints; physical layout
and execution libraries remain design choices. [Current research](../00-discover/research.md)
records SQL, recursive SQL, and GraphFrames candidates and source limits.

Every qualified deployment must explicitly pin cloud, catalog/schema names,
Unity Catalog policy configuration, SQL warehouse or cluster tier, runtime,
Delta protocol/features, language and budget. Bronze/silver ingestion and hot
transactional writes are upstream.
Gold publication may be incremental; no event-to-gold freshness promise follows
from selecting a hot database. Retention and audit duration remain open.

## Constraints, Assumptions, Dependencies

- **Constraints:** property graph; UMF semantic preservation; Databricks gold
  Delta; synthetic data; no unqualified version or scale claims.
- **Assumptions:** one synthetic domain-neutral slice can expose structural tradeoffs;
  bounded traversal is useful before graph algorithms; typed properties can
  carry the main analytic filters. Validate in design and the pilot.
- **Dependencies:** a pinned UMF core/binding subset, a selected Databricks
  environment for proof, and owner decisions on identity and publication policy.
  Real Truss integration is required for its source lane; other adapters and
  engine releases must proceed independently of Truss availability.

## Risks

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| UMF drift or duplicate semantics | High | High | Pin exact source/packages; map existing concepts before defining an extension |
| Referential/key assumptions mistaken for enforcement | High | High | Classify each rule; demonstrate negative-data detection separately |
| High-degree nodes cause query explosion | Medium | High | Test selective starts, skew, edge multiplicity, property width and result limits |
| Cross-table publication yields mixed revisions | Medium | High | Design an explicit publication/read policy and test interrupted updates |
| Visibility/provenance metadata mistaken for protection | Medium | High | Preserve metadata; require policy enforcement evidence before real data |
| External service unavailable | High | Medium | Isolate source/engine lanes; finish independent work and record exact access prerequisites |

## Open Questions

| ID | Question | Decision owner | Blocks |
| --- | --- | --- | --- |
| Q1 | Which UMF commit/core/extension subset is consumed? | Ashlar technical lead with UMF maintainer | Exact graph profile |
| Q2 | What scopes node identity, reconciles source keys, and identifies parallel edges? | Ontology/data owner | Schema contract and deduplication |
| Q3 | Current state versus historical values/edges; deletes, late events, retention, and snapshot visibility? | Data owner/platform lead | Publication and evolution contract |
| Q4 | Which cloud, compute/runtime, catalog and Delta feature profile? | Databricks platform owner | Executable compatibility evidence |
| Q5 | Which query workload, node/edge distribution, change rate, latency and cost budget? | Pilot analytics owner | Physical layout and performance acceptance |
| Q6 | Which real-data access rules, residency, retention, and provenance requirements? | Data governance owner | Production data adoption |
| Q8 | Which open-source license and distribution model? | Project owner | Public release and licensing terms |
| Q7 | Which typed/shared/hybrid layout best meets this workload? | Technical lead after evidence | Physical-design ADR |

## Consumer decisions remaining open

| ID | Question | Decision owner | Blocks |
| --- | --- | --- | --- |
| Q9 | Feed ordering, transaction boundaries, version scope, conflict policy and resumable cursor guarantees? | Producer and Ashlar leads | Publication Contract |
| Q10 | History window, tombstone lifetime and bootstrap reconciliation after feed expiry? | Data owner | History and recovery design |
| Q11 | Which bounded lists/counts, authorization disclosure policy and minimum-position wait policy? | Pilot consumer and governance owners | Consumer read Contract |

Q2 retains identity scope and parallel-edge decisions; Q3 retains history and
publication decisions. Consumer proposals inform those questions without closing
them. Each deployment must resolve Q1 through a pinned supported UMF profile; an
unavailable source/engine must not block independent model/query work.

## Success Criteria

The toolkit delivery is complete when FR-1–FR-4 have traceable contracts, reviewed
design decisions and runnable source/schema/publication/query workflows. Domain
pack data must be ingested and queried through evidenced Weft paths and bounded
engine releases, with exact values, original node/edge identities, multiplicity,
updates/deletes, replay and schema evolution verified. Actual Truss acceptance
and streaming remain required for that lane. External prerequisites must be
reported explicitly; they are not fabricated execution passes.
Every support statement names platform/runtime, model/format revision, subset,
and evidence. A draft spec or local simulation alone cannot satisfy that gate.

## Review Requirements

Review must confirm FR-1–FR-4 traceability, decision ownership, supported target
versions and schema contracts, cited execution of US-001–US-004 criteria, and
explicit owner approval. No unchecked status box in this requirement document
substitutes for the current evidence ledger. The
[pre-cleanup PRD](../04-build/evidence/documentation-history-20261009/docs/helix/01-frame/prd.original.txt)
preserves its historical milestone/status text verbatim.
