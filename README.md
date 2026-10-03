# ashlar

A standard graph data model for Databricks.

Ashlar starts with a shared structure for graph nodes on Databricks. Its name
comes from precisely shaped stones that fit together into a larger structure.

**Status:** discovery drafts only. No schema, library, Databricks integration,
or compatibility guarantee has been implemented. The first users, node identity
rules, relationship scope, and physical representation remain open.

Start with the [project documentation](docs/helix/README.md) and
[product vision](docs/helix/00-discover/product-vision.md).

## Working with HELIX

This repository uses [HELIX](https://github.com/DocumentDrivenDX/helix).
Read `AGENTS.md` and `.helix.yml`, then invoke the installed `helix` skill.
The bootstrap used HELIX 0.14.1. Resolve its graph, templates, and prompts from
the installed plugin; the methodology catalog is not vendored here.

**Next action:** `frame` — review the discovery assumptions, choose the first
producer/consumer scenario, and draft the PRD and feature specifications.

## Naming

The owner selected Ashlar on 2026-10-03. Existing software uses include
[Ashlar-Vellum](https://ashlar.com/) and the unrelated
[ASHLAR imaging package](https://pypi.org/project/ashlar/).
Repository naming does not establish package, domain, or trademark availability.
See the [discovery input](docs/helix/00-discover/vision-input.md).
