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

Concern authority: HELIX 0.15.4. Project overrides preserve Ashlar's portable library and
explicitly qualified runtime profiles.

## Active Concerns

| Concern | Source | Areas | Why Active | Key Practices |
| --- | --- | --- | --- | --- |
| modularity-and-encapsulation | baseline-policy; HELIX 0.15.4 | all | Handwritten Python core and integration tooling require enforceable ownership | ADR-001 Module Boundaries; exact existing edge debt only; new forbidden imports fail the project checker. |
| python-uv | library; owner-requested configuration guidance | all | Runtime endpoint, identity and policy configuration affects authority | One typed entrypoint configuration object with explicit precedence, secret types and injection; portable-library override below. |
| o11y-otel | library; owner-requested diagnostics guidance | all | Publication, ACK and held reads need attributable failure evidence | Structured bounded diagnostics, OTel boundary mapping, redaction before every sink; no telemetry-as-commit evidence. |
| formal-methods | library; owner-requested formal analysis | all | Crash/replay and independent feed progress are safety-critical | Precise reviewed publication/ACK specification with implementation witnesses; bounded executable analysis is a separately evidenced increment; ADR-001 and TP-001 own scope/correspondence. |
| scope-discipline | library; owner-requested focus | all | End-to-end toolkit scope must stay bounded | Deliver admitted source ingestion, immutable publication/recovery and qualified reads/engine mappings; no unrelated agent product. |
| databricks-target | project-local; operator-override | `area:data`, `area:interop` | Databricks gold Delta selected | Pin cloud, compute/runtime, catalog, protocol, subset, and evidence before support claims. |
| semantic-preservation | project-local; operator-override | `area:model`, `area:interop` | UMF integration selected | Preserve source; reuse existing vocabulary; report losses and distinguish target-only recovery. |
| enforcement-transparency | project-local; existing assumption supported by platform docs | `area:model`, `area:data` | Keys are not automatically enforced | Identify declared, checked, enforced, and unsupported rules separately. |
| graph-workload | project-local; owner brief | `area:data`, `area:query` | Skew and isolated nodes at large scale | Assess degree skew, property width, edge count, selective traversals, and incremental churn separately. |
| governance-provenance | project-local; owner brief | `area:model`, `area:data`, `area:query` | Graph data requires lineage and access semantics | Use synthetic controls and original pinned domain packs under explicit source profiles; preserve notices/metadata; no security claim from metadata alone. |

## Generality and open-source scope

Owner override: the toolkit is domain-independent and open source. Downstream
schemas and tooling must accept consumer-defined types without built-in industry
semantics. Examples use synthetic TypeA/TypeB/TypeC nodes. Review features and
stories for domain assumptions; record license choice before public release.

## Project Overrides

### Portable Python library and configuration

Ashlar retains Python 3.9+ and its dependency-free core. Python 3.12+, uv and
pydantic-settings v2 are the preferred application-host configuration route;
this does not impose SDK/runtime dependencies on portable core users. Host
configuration owns typed validation and injects immutable values/ports; the core
must not read ambient environment or discover credentials. Existing runtime
profiles remain individually qualified. Review this override when the supported
Python floor or packaged application dependencies change.

### Browser boundary

Browser configuration uses a browser-safe typed boundary (Zod or an equivalently
reviewed closed validator), without Node environment/secret APIs. Public schema
browser assets carry no credentials. UMF retains semantic validation ownership.

### Formal-analysis scope

Formal specification applies to publication visibility, immutable vectors,
per-feed ACK/replay and original-attempt recovery. Engine query semantics,
layout performance and UI behavior are outside this model; their conformance
uses independent semantic/native tests. A model pass is not a production fence
or native retention/transaction guarantee.

### Selected storage and deployment

The datastore is Unity Catalog managed gold Delta tables; the deployment target
is Databricks. Local Spark/DuckDB and private graph-engine fixtures qualify their
explicit subsets separately and cannot replace this selected production scope.

## Area Labels

- `area:model` — ontology semantics and UMF profile.
- `area:data` — physical schemas, integrity, and governance.
- `area:interop` — mappings, versioning, and loss reports.
- `area:query` — query semantics and feasibility evidence.
- `area:docs` — governed specifications.

## Concern Conflicts

| Conflict | Resolution |
| --- | --- |
| Broad ecosystem versus first milestone | End-to-end source/schema/ingest/publication/read paths; external dependencies block their own qualification only. |
| Entity/property wording versus UMF source meaning | Use ontology-facing terminology without changing UMF semantics; extension for graph-specific meaning. |
| Billions of nodes versus initial proof | Reuse retained layout measurements and small functional checks; no renewed scale benchmark or extrapolated billion-node support claim. |
| Governance tags versus enforcement | Preserve declarations and record enforcement gaps; real-data adoption requires platform policy evidence. |
