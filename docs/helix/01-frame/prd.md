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

The immediate deliverable is a designed schema package: pinned logical profile,
physical table contract and data definition language (DDL), integrity rules,
synthetic examples, and a query feasibility decision. This framing pass defines
those outcomes; it does not claim that the schemas or query tools exist yet.
Success requires zero unexplained mapping loss, valid table creation on one
pinned Databricks target, and exact expected results for the agreed query corpus.

## Current milestone acceptance — owner revision2026-10-08

Close the physical-layout/schema-package milestone using the existing bounded
synthetic evidence and local package review. Its required outcomes are concrete
Delta definitions, native structural evidence, exact identity/value/unknown-content
preservation, explicit integrity/enforcement responsibilities, pinned publication
and native lookup evidence, and scoped graph-tool mappings. UMF binding is deferred.
The [milestone acceptance record](../02-design/spikes/SPIKE-001-table-layout/layout-milestone-acceptance.md)
identifies the required evidence and its limits.

Provisional latency, publication freshness, sustained/burst throughput and1B-node/
5B-edge capacity are operational qualification targets, not gates for this design
milestone or prerequisites to implementation. Record misses and unknowns without
asserting performance support. No further scale benchmark is required. Production
source authority, concurrency fencing, authorization delegation, retention and
external engine deployment remain requirements for their later delivery scopes;
closing this milestone does not declare those scopes complete.

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

Proposed milestone acceptance targets; none is an observed result.

| Metric | Target | Baseline | Measurement Method | Cadence |
| --- | --- | --- | --- | --- |
| Semantic accountability | 100% selected constructs mapped, rejected, or explicitly reported; zero unexplained losses | No Ashlar mapping | Reviewed source/target/recovery corpus | Each profile revision |
| Schema usability | 100% example DDL creates and loads valid fixtures on one pinned target | No Ashlar DDL | Databricks execution records | Each schema revision |
| Query correctness | 100% expected result identities and multiplicities match | No query proof | Synthetic result-set oracle | Each query/layout revision |
| Invalid-data detection | 100% authored integrity counterexamples detected | No checks | Negative corpus | Each validation revision |

Latency, freshness, throughput, and cost budgets must be selected with the pilot
owner before performance acceptance. The owner selected a 1B-node planning target with more edges on 2026-10-05 and
asked for fast singleton lookup directly on Databricks. Provisional experiment
budgets are recorded in SPIKE-001: warm native engine p95 100 ms and caller p95
250 ms for singleton lookup, with cold-data caller p95 1 s on running compute.
They are design targets pending native evidence and hardware/cost review.
The planning graph uses 5B edges as an explicit assumption, not measured input.

### Open-source scope

Ashlar must be usable across application domains. Domain types and relationship
vocabularies come from consumer-authored UMF models; the core must not require
any industry-specific schema. The intended open-source deliverable includes
source, schema specifications, synthetic examples, and reproducible validation
instructions. License and publication details require a separate decision before
release; this draft does not select a license or claim public availability.

### Non-Goals

Transactional engines, Postgres/Lakebase deployment, Truss/Axon convergence,
UMF core cleanup, action execution, compensation/sagas, generated GraphQL,
MCP, admin UI, production Fabric/Neo4j projection deployment, RDF/SPARQL/OWL runtime, graph algorithms,
and automated entity resolution are deferred. Exact future boundaries and
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

Only the synthetic source is in the initial proof. A replayable synthetic change feed
represents the potential consumer’s hot-store publication scenario. Existing systems remain the
write owners. Ashlar must describe incremental publication, replay, deletion,
and visibility semantics without implementing a transactional engine.

## Requirements

P0 consists of FR-1 through FR-4 below. No P1/P2 product surfaces are required
for this milestone. Optional query tools may be rejected after evaluation;
the supported query path itself is mandatory.

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
  Every constraint must identify its actual enforcement boundary.
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
physical columns, and platform mechanisms remain downstream decisions.

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
claims. Fabric’s current approximate 2B total-elements limit cannot establish
support for 1B nodes plus more edges. Source meaning and omitted cross-scope
relationships must be accounted for in every projection.

## Technical Context

Databricks gold Delta and UMF integration are owner constraints; physical layout
and execution libraries remain design choices. [Current research](../00-discover/research.md)
records SQL, recursive SQL, and GraphFrames candidates and source limits.

Cloud, catalog/schema names, Unity Catalog policy configuration, SQL warehouse
or cluster tier, runtime, Delta protocol/features, language, and budget are
unselected. Bronze/silver ingestion and hot transactional writes are upstream.
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
  Related projects are context, not mandatory runtime dependencies.

## Risks

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| UMF drift or duplicate semantics | High | High | Pin exact source/packages; map existing concepts before defining an extension |
| Referential/key assumptions mistaken for enforcement | High | High | Classify each rule; demonstrate negative-data detection separately |
| High-degree nodes cause query explosion | Medium | High | Test selective starts, skew, edge multiplicity, property width and result limits |
| Cross-table publication yields mixed revisions | Medium | High | Design an explicit publication/read policy and test interrupted updates |
| Visibility/provenance metadata mistaken for protection | Medium | High | Preserve metadata; require policy enforcement evidence before real data |
| Broad brief diverts work into engine/UI | High | High | Only four capabilities in this milestone; reopen deferred scope explicitly |

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
them. Q1 blocks UMF binding, not model-independent behavior specifications.

## Success Criteria

The milestone is complete when FR-1–FR-4 have traceable contracts, recorded
design decisions, reproducible schema/query evidence and reviewed limits.
Every support statement names platform/runtime, model/format revision, subset,
and evidence. A draft spec or local simulation alone cannot satisfy that gate.

## Review Checklist

- [x] Four scoped capabilities, acceptance sketches, and upstream traceability.
- [x] Open decisions have owners and consequences; no fabricated benchmarks.
- [ ] Target versions, schema contracts, and design decisions finalized.
- [ ] US-001–US-004 acceptance criteria exercised by cited tests.
- [ ] Owner review/approval.
