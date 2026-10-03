# Discovery input

Updated 2026-10-03. The owner selected Ashlar and authorized the repository and
HELIX bootstrap. The current request and supplied build brief supersede the
initial uncertainty about UMF, edges, graph model, and gold storage.

## Confirmed direction

- Ashlar is an open-source, domain-independent toolkit to build and use
  property-graph ontologies on data warehouses. Databricks is the first target.
- Integrate with UMF to define node types; preserve its existing semantics.
- Use a labeled property graph: typed nodes and typed edges, both with properties.
- Databricks gold Delta tables are the official ontology for reads and analytics.
- First milestone: well-supported table schemas and a path to graph queries,
  potentially using existing Databricks-compatible tools.
- Domain types are user-defined. Workloads may include high-change imported
  relationships, curated types maintained elsewhere, and many isolated nodes.
- A couple of billion nodes is a workload aspiration, not demonstrated capacity.

## Authority and source handling

The narrower accompanying request governs Ashlar's first milestone. The
[generalized build brief](build-brief.md) retains the technical starting context, not
as an approved architecture or an implementation instruction for every project.
Its external product/version assertions require independent verification.

The brief's proposed entity/property vocabulary is a consumer-language request,
not authorization to rename or redefine UMF core. Reuse UMF concepts and physical
bindings; put graph-only meaning in a versioned Ashlar extension when necessary.
The brief's whole-ecosystem sequence does not make Truss a prerequisite for
Ashlar's synthetic schema and query proof.

## Naming rationale

Ashlar evokes precisely shaped stones that fit together. The owner selected it
with existing name overlap disclosed. Package, domain, and trademark availability
have not been cleared; naming does not authorize publication.

## Open-source direction

Ashlar will be open source. License selection and release/distribution details
remain open; no license has been selected or publication performed by this pass.
Domain definitions belong to consumer-authored UMF models, not built-in industry
schemas. Synthetic TypeA/TypeB/TypeC examples demonstrate structural behavior.

## Assumptions and unresolved choices

- First producer and consumer teams remain unidentified; synthetic domain-neutral examples
  are proposed design fixtures, not approved production datasets.
- Global identity, source-key reconciliation, tenant scope, edge multiplicity,
  temporal semantics, and deletion behavior need recorded design decisions.
- UMF revision and supported subset, Databricks cloud/runtime/compute/catalog,
  performance budgets, retention, and enforcement policies remain open.
- Runtime language is undecided. Truss's Postgres and TypeScript choices do not
  govern Ashlar. Related tools are integration candidates, not inherited code.

See the [PRD](../01-frame/prd.md) for scope, ownership of open questions, and
acceptance targets. All derived artifacts remain drafts.
