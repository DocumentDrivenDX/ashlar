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

Initial selections for a specification-first discovery project. Source labels
record why each concern is active; they do not approve product requirements.

## Active Concerns

| Concern | Source | Areas | Why Active | Key Practices |
| --- | --- | --- | --- | --- |
| scope-discipline | library; assumption from bootstrap scope | all | Genesis precedes requirements and implementation | Bound changes to the requested artifact; leave speculative capabilities as questions. |
| databricks-target | project-local; operator-override | `area:data`, `area:interop` | Owner explicitly selected Databricks | Record cloud, runtime, catalog, format, and evidence with support claims. |
| semantic-preservation | project-local; assumption | `area:model`, `area:interop` | Mapping conventions can change meaning | Surface unmapped information and distinguish loss from intended conversion during framing. |
| enforcement-transparency | project-local; assumption grounded in platform docs | `area:model`, `area:data` | Declared keys do not establish enforcement | Separate declared, validated, and write-enforced rules in downstream designs and evidence. |

## Project Overrides

No library practice overrides. No implementation slot is needed for this
documentation bootstrap: language/runtime, datastore, architecture, and tooling
remain undecided. Databricks is an operator-selected platform concern; it does
not itself select Delta Lake, Unity Catalog, Python, TypeScript, or a deployment.
Resolve needed exclusive slots during frame using the installed HELIX catalog.
There is no frontend, authentication provider, or browser test framework to select.

## Area Labels

- `area:model` — graph contract and schema semantics.
- `area:data` — platform representation, integrity, and data governance.
- `area:interop` — producer/consumer mappings and compatibility evidence.
- `area:docs` — governed specifications and user guidance.

## Concern Conflicts

| Conflict | Resolution |
| --- | --- |
| Broad interoperability versus initial scope | Frame one producer/consumer use case before committing to more formats or platforms. |
| Platform declarations versus integrity expectations | Derive claims from observed checks, retaining unsupported rules as explicit limitations. |
