# Current framing research and scope disposition

Updated 2026-10-03. Owner direction now selects a domain-independent, open-source
property-graph toolkit, UMF integration,
and Databricks gold Delta. Earlier statements below that these were unselected
are historical bootstrap context. No Databricks execution or performance tests
were performed in this framing pass.

## Query path to evaluate

| Candidate | Evidence | Proposed evaluation and limits |
| --- | --- | --- |
| Fixed-depth SQL joins | Relational baseline; feasibility to prove on selected target | First test lookup, one-hop filters, and two-hop TypeA–TypeB–TypeC joins over the same schema. No new language required. |
| GraphFrames motif queries | [Official motif guide](https://graphframes.io/04-user-guide/04-motif-finding.html), read 2026-10-03 | Documented structural patterns use vertex/edge DataFrames. Compare identity and multiplicity with SQL; pin library, Spark and Databricks compatibility before selection. Compute library, not store. |
| Recursive SQL | [Databricks AWS CTE reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-cte), read 2026-10-03 | Documentation places recursion at Runtime 17.0+; default depth 100 and result limit 1,000,000. Runtime 17.2+ documents LIMIT ALL. Optional bounded experiment; limits and execution still need verification on the selected target. |
| GraphAr | Existing [resource note](resources/graphar.md) | Secondary export/layout candidate. Do not replace owner-selected gold Delta merely to adopt another format. |

Recommendation for the design experiment, not a recorded final architecture:
compare shared node/edge tables, typed tables, and a hybrid using identical
logical examples and query answers. Begin with ordinary joins. Try GraphFrames
only as a consumer of that representation; record a rejection if setup or
semantics make it unsuitable. Separate fixed-depth queries from graph algorithms.

Use a small correctness fixture first, then a reproducible workload with reported
node/edge counts, property widths, high-degree hubs, isolated nodes, selective
starts, and update churn. Select larger scale steps and latency/cost budgets with
the owner; never imply billion-node support from a small proof.

## UMF and enforcement evidence

[UMF revision evidence](resources/umf.md) identifies existing logical/physical
separation and stable relationship binding. Inspect the pinned core and binding
contracts before defining the graph extension. Full Delta table generation is
not established by the inspected upstream evidence.

[Databricks constraints](resources/databricks-constraints.md), rechecked against
[official AWS documentation](https://docs.databricks.com/aws/en/tables/constraints)
on 2026-10-03, distinguishes enforced NOT NULL/CHECK from informational keys.
The schema package must cover uniqueness and endpoint validation explicitly.

## Scope disposition of the owner brief

| Brief topic | Ashlar disposition | Re-entry condition |
| --- | --- | --- |
| Property graph, UMF, gold Delta | First milestone, FR-1–FR-3 | Current scope |
| Identity, typed properties, edge provenance/version, governance declarations | Preserve and define necessary schema semantics | Exact policies in design Contracts; real-data governance review before adoption |
| UMF vocabulary cleanup, forks, actions, compensation | Related upstream work; no core rewrite here | Proven graph-profile gap and separately scoped upstream change |
| Truss/Postgres/Lakebase, hybrid JSONB, audit feed | External transactional context; no inherited engine or ADR | A concrete producer contract and requested integration |
| Hot-to-gold freshness | Open operational requirement | Measured publication mechanism and budget; Lakebase alone proves no gold visibility |
| Generated GraphQL, action tools, MCP, admin walker | Deferred consumers | Stable schema/query contracts and explicit scope expansion |
| Fabric GQL / Neo4j | Deferred projections | Stable gold profile and a real consumer requirement |
| RDF/OWL/SHACL/Turtle | Interchange context only | Separately requested projection; never implicit graph runtime |
| General graph algorithms, sagas, kinetics/writeback | Deferred | Validated schema/query milestone plus explicit owner request |

The brief's product claims about Lakebase extensions, Fabric preview state,
TableSpec production results, and test counts are owner-supplied context, not
verified Ashlar compatibility evidence. No such dependency is selected here.

---

# Historical bootstrap research (superseded where noted below)

Desk research completed 2026-10-03 before drafting the vision. No platform,
performance, compatibility, or customer validation was performed.

| Candidate | Evidence | Implication for Ashlar |
| --- | --- | --- |
| GraphFrames | [Vertex/edge DataFrame inputs](resources/graphframes.md) | Assess as a consumer; avoid claiming that column names alone settle the shared data contract. |
| Apache GraphAr | [Graph storage format and Spark libraries](resources/graphar.md) | Evaluate adoption or a mapping before creating a new physical format. |
| Databricks constraints | [Enforced versus informational rules](resources/databricks-constraints.md) | Frame explicit responsibility for integrity checks; a declared key is not proof that it is enforced. |
| UMF | [Current README](https://github.com/DocumentDrivenDX/umf) describes an experimental machine-readable metamodel and schema interchange fabric with scoped preservation and validation | Evaluate as a schema input; no dependency or graph mapping is selected. |
| tablespec | [Project README](https://github.com/DocumentDrivenDX/tablespec) describes table-schema tooling and Spark integration | Clarify responsibility for schema generation versus graph conventions before duplicating tooling. |

The defensible gap is a hypothesis: a shared contract may remove recurring
producer/consumer mapping decisions that existing graph-processing APIs and
storage formats leave to each team. Neither a customer study nor a complete
standards comparison has established that gap. Framing should test whether
an existing model plus a narrow Databricks profile is sufficient.

Databricks remains the owner-selected foundation. The survey supplies plausible
integration points, but does not yet prove fitness for a particular workload.
No physical layout, language, dependency version, or performance target is chosen.

Related projects remain independent: truss is a planned property-oriented graph
engine on SQL databases; Axon is document-oriented. Their design choices do not
govern Ashlar. Integration with either remains an open product question.

Next research: choose a real producer/consumer scenario, compare it against the
GraphFrames and GraphAr contracts, assess UMF reuse, and pin versions for a small
conformance experiment. Detailed resource summaries for UMF and tablespec should
follow if either is selected as a dependency.
