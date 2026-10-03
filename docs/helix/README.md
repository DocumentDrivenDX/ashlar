# Ashlar project documentation

**State (2026-10-03):** Focused HELIX framing drafts exist. Owner direction is a
open-source, domain-independent property-graph toolkit integrating UMF, with
Databricks gold Delta as its first target. The first
milestone is a well-supported schema package and an evidenced graph query path.
No physical schema, implementation, or compatibility result is approved.

| Activity | State | Entry point |
| --- | --- | --- |
| 00 Discover | Updated direction and evidence | [Vision](00-discover/product-vision.md), [input](00-discover/vision-input.md), [generalized build brief](00-discover/build-brief.md), [research/scope disposition](00-discover/research.md) |
| 01 Frame | Draft PRD, three features, three stories | [PRD](01-frame/prd.md), [concerns](01-frame/concerns.md) |
| 02 Design | Next; no physical choices recorded yet | Exact UMF graph profile, Delta schema contract, layout/publication ADRs, query feasibility experiment |
| 03 Test | Acceptance scenarios specified; all untested | Story criteria below; no platform tests run |
| 04 Build | Not started | No implementation |
| 05 Deploy | Not started | No resources provisioned |
| 06 Iterate | Not started | No release |

| Requirement | Feature | User story |
| --- | --- | --- |
| FR-1 | [FEAT-001: UMF graph profile](01-frame/features/FEAT-001-umf-graph-profile.md) | [US-001: Review mapping](01-frame/user-stories/US-001-review-graph-mapping.md) |
| FR-2 | [FEAT-002: Databricks schema package](01-frame/features/FEAT-002-databricks-schema-package.md) | [US-002: Validate package](01-frame/user-stories/US-002-validate-schema-package.md) |
| FR-3 | [FEAT-003: Graph query path](01-frame/features/FEAT-003-graph-query-path.md) | [US-003: Query typed graph](01-frame/user-stories/US-003-query-typed-graph.md) |

**Next action: design.** Review framing and resolve PRD Q1–Q4/Q7 sufficiently to
write a concrete schema Contract and physical design. Compare shared, typed,
and hybrid table layouts; record identity, edge multiplicity, property encoding,
history, and publication decisions. Use one synthetic TypeA–TypeB–TypeC
slice with an isolated node. Prove fixed-depth SQL queries first and evaluate
GraphFrames as an optional compute consumer. Q5 determines performance budgets;
Q6 blocks real-data adoption.

Drafting design can proceed with explicit proposals, but drafts do not constitute
owner approval or justify production/compatibility claims. Tests must cite stable
story AC IDs and actually exercise them before criteria can be marked satisfied.
Transactional engines, action execution, GraphQL, MCP, and UI are deferred.

HELIX catalog: installed plugin 0.14.1 `workflows/graph.yml`; `.helix.yml` binds
only project artifacts. No methodology catalog is copied into this repository.
