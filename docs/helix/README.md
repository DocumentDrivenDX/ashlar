# Ashlar project documentation

**State (2026-10-03):** HELIX genesis completed; discovery and concern documents
are drafts. The owner's confirmed intent is a standard structure for graph nodes
on Databricks. No product requirements or implementation have been approved.

| Activity | State | Entry point |
| --- | --- | --- |
| 00 Discover | Drafts | [Vision](00-discover/product-vision.md), [input and open questions](00-discover/vision-input.md), [prior-art research](00-discover/research.md) |
| 01 Frame | Initial concerns only | [Concerns](01-frame/concerns.md) |
| 02 Design | Not started | No architecture or ADRs |
| 03 Test | Not started | No conformance corpus or platform tests |
| 04 Build | Not started | No code or work tracker |
| 05 Deploy | Not started | No Databricks resources provisioned |
| 06 Iterate | Not started | No released product |

**Next action: `frame`.** Review the vision, validate the proposed first audience,
choose one producer/consumer example, and write the PRD and feature specifications.
Resolve node identity, relationship scope, UMF's role, and evidence needed for the
first Databricks compatibility claim before committing to a physical layout.

HELIX catalog source: installed plugin, bootstrap version 0.14.1. `.helix.yml`
binds only this project's artifacts. Resource summaries live under
`00-discover/resources/` to keep all project artifacts in the activity scope.
