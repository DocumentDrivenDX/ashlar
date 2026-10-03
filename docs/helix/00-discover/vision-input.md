# Discovery input

Captured 2026-10-03 from the owner's naming and bootstrap discussion.

## Confirmed direction

- Intent: “design a standard structure for graph nodes on databricks.”
- Name: Ashlar, selected by the owner after two naming research rounds.
- Destination: DocumentDrivenDX/ashlar; bootstrap with HELIX.
- The broader description “a standard graph data model for Databricks” is the
  working tagline. Whether the first release standardizes edges is still open.

## Naming rationale

Ashlar evokes precisely shaped stones that fit together. It fits the structural
metaphor of truss while retaining its own project identity. The owner chose it
with existing software-name overlap disclosed. Package, domain, and trademark
availability have not been cleared; do not publish a package based on the repo name.

## Assumptions to validate

- A1: Initial users are data platform engineers maintaining graph-shaped datasets
  on Databricks for more than one producer or consumer. No first user is identified.
- A2: A reusable contract reduces repeated mapping work compared with individually
  defined tables. No customer interviews or measured baseline establish this yet.
- A3: GraphFrames is a useful candidate consumer and GraphAr a useful candidate
  format. Neither integration is selected or tested.
- A4: Identity, properties, and evolution need shared rules; their actual semantics
  must come from a real use case during framing.

## Proposed scope boundaries

For the initial framing discussion, keep a new graph database engine, query
language, graph visualization application, and automated entity resolution out
of scope. These are proposed non-goals, not owner-approved product requirements.
Databricks is the stated foundation; portability to other platforms is undecided.

## Open questions

1. Who are the first producer and consumer, and what concrete graph do they share?
2. Does the first standard include edges, labels/types, and edge properties?
3. What defines node identity across sources, tenants, and time?
4. Is Ashlar a logical model, physical storage convention, validation library, or
   a combination? Which existing format or model should it adopt?
5. Should it consume UMF, and how does that divide responsibility with tablespec?
6. Which Databricks cloud, runtime, catalog, and table format must be supported?
7. Which rules need write-time enforcement versus validation and reporting?
8. What history, provenance, deletion, access control, and schema evolution do
   actual users need? Is any sensitive data involved?
9. Which implementation language, distribution package name, and license fit
   the agreed deliverable? None has been selected.
