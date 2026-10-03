# Initial prior-art research

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
