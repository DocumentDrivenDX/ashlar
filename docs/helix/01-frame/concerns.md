---
ddx:
  id: ashlar.concerns
  type: concerns
  activity: frame
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.resource.databricks-constraints
    kind: informed_by
---

# Project Concerns

Updated from the owner's 2026-10-03 request. Existing concerns remain active;
platform, format, and UMF integration now have explicit owner direction.

## Active Concerns

| Concern | Source | Areas | Why Active | Key Practices |
| --- | --- | --- | --- | --- |
| scope-discipline | library; owner-requested focus | all | Schema and query milestone must stay bounded | Preserve deferred brief context; do not implement transactional or agent surfaces. |
| databricks-target | project-local; operator-override | `area:data`, `area:interop` | Databricks gold Delta selected | Pin cloud, compute/runtime, catalog, protocol, subset, and evidence before support claims. |
| semantic-preservation | project-local; operator-override | `area:model`, `area:interop` | UMF integration selected | Preserve source; reuse existing vocabulary; report losses and distinguish target-only recovery. |
| enforcement-transparency | project-local; existing assumption supported by platform docs | `area:model`, `area:data` | Keys are not automatically enforced | Identify declared, checked, enforced, and unsupported rules separately. |
| graph-workload | project-local; owner brief | `area:data`, `area:query` | Skew and isolated nodes at large scale | Assess degree skew, property width, edge count, selective traversals, and incremental churn separately. |
| governance-provenance | project-local; owner brief | `area:model`, `area:data`, `area:query` | Graph data requires lineage and access semantics | Synthetic data only; preserve metadata; no security claim from metadata alone. |

## Generality and open-source scope

Owner override: the toolkit is domain-independent and open source. Downstream
schemas and tooling must accept consumer-defined types without built-in industry
semantics. Examples use synthetic TypeA/TypeB/TypeC nodes. Review features and
stories for domain assumptions; record license choice before public release.

## Project Overrides

No library practice overrides. No `concerns.local.yml` exists. Needed slots:

| Slot | Filler | Source |
| --- | --- | --- |
| datastore | Gold Delta tables (project-local) | operator-override, supplied brief and focused request |
| deploy-target | Databricks (project-local; environment unselected) | operator-override |

Language-runtime and architecture-style remain design questions, not needed
selections for this specification bootstrap. Frontend, browser e2e, and auth
provider slots do not apply to this milestone. No shipped application defaults
are inherited. Physical layout and target versions need ADRs before build.

## Area Labels

- `area:model` — ontology semantics and UMF profile.
- `area:data` — physical schemas, integrity, and governance.
- `area:interop` — mappings, versioning, and loss reports.
- `area:query` — query semantics and feasibility evidence.
- `area:docs` — governed specifications.

## Concern Conflicts

| Conflict | Resolution |
| --- | --- |
| Broad ecosystem versus first milestone | UMF profile, Delta schema, query path only; preserve other work as deferred. |
| Entity/property wording versus UMF source meaning | Use ontology-facing terminology without changing UMF semantics; extension for graph-specific meaning. |
| Billions of nodes versus initial proof | Require representative skew and a scaling experiment design; never extrapolate a small test into a support claim. |
| Governance tags versus enforcement | Preserve declarations and record enforcement gaps; real-data adoption requires platform policy evidence. |
