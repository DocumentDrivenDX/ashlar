# Working in ashlar

This repository uses HELIX. Start with `docs/helix/README.md`, read `.helix.yml`,
and engage the installed `helix` skill for governed work.

- Keep project artifacts under `docs/helix/` in the matching activity.
- Resolve the graph, templates, prompts, and concern library from the installed
  HELIX plugin. Do not copy the methodology catalog into this repository.
- Read governing upstream artifacts before writing downstream documents.
- Preserve artifact IDs, frontmatter, and deliberate `ddx.links` traceability.
- Record unknowns as open questions, assumptions, or risks. Drafts are not approvals.
- Implementation must trace to framed requirements and recorded design decisions.

## Project context

Ashlar defines domain-independent property graphs on Unity Catalog managed Delta
tables. Native singleton reads must work independently of Fabric Graph. Read the
product requirements and publication, consumer, table and resolver contracts
before extending a boundary; do not infer requirements from implementation status.

UMF owns reusable schema interpretation, validation and DDL generation. Preserve
declared schema versions, native semantics and unknown extensions. Put explicit
Ashlar meanings in versioned extension vocabularies. Repair demonstrated reusable
domain-pack gaps in UMF rather than introducing a second interpretation in Ashlar.

Weft owns query compilation; Ashlar owns publication-bound execution adapters.
Do not repair compiler SQL in an adapter. Truss owns its accepted source boundary;
do not inherit its PostgreSQL or TypeScript ADRs or substitute development fixtures
for actual Truss acceptance and feed evidence.

Qualify support claims by platform/runtime version, model or format version,
supported subset and exact evidence. Distinguish declared constraints from enforced
constraints, preparation from native execution, and development source authority
from production authority. Preserve identity, presence, exact values, multiplicity,
full publication vectors and source/ACK custody throughout each workflow.

Specs describe desired behavior. Keep implementation status short in the README
and retain execution results under build evidence. Keep native checks small and
sequential; use local systems or an explicitly dedicated Databricks endpoint,
never the shared default cluster. Do not renew scale benchmarks or disable
predictive optimization. Land tested, independently reviewed iterations on main
and push to origin.
