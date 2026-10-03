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

Ashlar's owner requested a standard structure for graph nodes on Databricks,
selected the name Ashlar, and authorized this repository and HELIX bootstrap.
Discovery drafts exist; PRD, feature specifications, architecture, and code do not.
The next action is `frame`.

Databricks is the stated target. Runtime language, storage format, graph model,
node identity, relationship scope, enforcement mechanisms, supported versions,
and first users remain undecided. Do not inherit truss's PostgreSQL or TypeScript
ADRs: they belong to a separate project.

UMF (DocumentDrivenDX's machine-readable metamodel and schema interchange fabric),
tablespec, truss, and Axon are related projects, not dependencies selected here.
If Ashlar consumes UMF, preserve its semantics and put Ashlar-specific concepts
in an explicit extension vocabulary rather than redefining UMF meaning.

Qualify future support claims by platform/runtime version, selected model or
format version, supported subset, and evidence. Distinguish declared constraints
from constraints actually enforced. Use synthetic examples until a real dataset
and its governance requirements are explicitly supplied.
