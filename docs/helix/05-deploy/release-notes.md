---
ddx:
  id: ashlar.release-notes-20261103
  type: release-notes
  activity: 05-deploy
  status: released
  authoring:
    home: repo
  links:
    - id: ashlar.release-closure-20261101
      kind: informed_by
---

# Release Notes — 0.1.0

> Published as an immutable GitHub release on 2026-11-03. The annotated tag
> points to the reviewed source commit; the release API target commitish is
> `main`. Downloaded wheel and sdist payloads were verified against the tag.

## Release Scope

- Release identifier or version: `0.1.0`.
- Release date: 2026-11-03.
- Rollout window or environment: selected `aidev-cus` Databricks serverless profile, Spark 4.2.0 / environment 6 / Unity Catalog managed Delta.
- Release owner: Erik Labianca.
- Distribution: [immutable GitHub `v0.1.0` release](https://github.com/DocumentDrivenDX/ashlar/releases/tag/v0.1.0); Truss `v0.0.55` is published at its [immutable release](https://github.com/DocumentDrivenDX/truss/releases/tag/v0.0.55).

## Audience and Channels

| Audience | Why they care | Delivery channel |
|----------|---------------|------------------|
| Operators and support | Install the Ashlar wheel from the GitHub `v0.1.0` release assets | GitHub Releases |
| Maintainers | Verify the wheel hash against the release manifest before installation | GitHub release manifest |

## Highlights

Qualified scope and release limits:

- Define property graphs on versioned Delta tables with native singleton reads independent of Fabric Graph.
- Preserve source, publication-vector and acknowledgement custody through the selected local Truss workflow.
- Export canonical graph bytes for a bounded GraphFrames action while preserving parallel edges and isolated nodes.
- Exercise the selected Databricks serverless Spark 4.2.0 / Unity Catalog managed Delta path with exact four-role versioned observations; the Delta engine version was not separately observed.

## Required Actions Summary

- Install the wheel from the GitHub `v0.1.0` release assets and verify its SHA-256 against `release-manifest.json`.
- Use the managed profile only within the qualified Spark 4.2.0 / environment 6 scope described here.
- No in-place production migration is qualified; use a fresh installation and retain the previous deployment for rollback. The managed cost envelope is `$89.56` modeled including reserve; billing was not observed.

## Changes and Fixes

### New or Improved

| Area | What changed | Who is affected |
|------|--------------|-----------------|
| Delta graph publication | The local R61 qualification exercised the `.4-xxhash64` carrier with Truss 0.0.55, UMF SDK 0.10.2 / Core 0.10.6 and model 0.10.0. | The local R61 evidence is a separate local-development qualification and does not establish managed runtime support. |
| Source custody | A local attempt14 recovery independently compared four native Delta role bags to the source oracle before reconciling one A1 ACK. | Qualification reviewers; recovery records a source-hold discontinuity. |
| Managed serverless continuation | R23 completed A0 native verification, a three-point predictive-optimization observation and matched protected ACK. R25 resumed the retained A1 source transaction without replay, independently verified 14 native rows, observed predictive optimization enabled at all three checks, reconciled the protected ACK, and completed close and cleanup. | Operators using the selected `aidev-cus` serverless Spark 4.2.0 / environment 6 / Unity Catalog managed Delta profile. The source-hold gap remains recorded; no Databricks Delta engine version is claimed. |
| Runtime package bindings | The host controller/source-holder uses Ashlar `0.1.0rc10`; remote task payloads remain bound to the original Ashlar `0.1.0rc7` wheel. | The remote Spark tasks used RC7; RC10 was the host controller and source-holder. |

### Fixes

| Issue or symptom | Resolution | User or operator impact |
|------------------|------------|-------------------------|
| The local A1 prefix builder had used the admission cursor where the original genesis cursor was required. | The builder now keeps original genesis at cursor zero while the continuation admission bundle remains separately bound to its live cursor. | Prevents rebinding the original source prefix to a later checkpoint. |
| Truss install transactions could exceed the default operation window on the selected local setup path. | Truss 0.0.55 exposes a bounded install-only operation timeout override; general operation and statement caps remain unchanged. | Local setup can select a longer bound for that install operation. |

## Breaking Changes and Required Actions

The qualification supports the selected managed serverless profile and the separate recorded local R61 profile. It does not qualify in-place migration or rollback automation. Classic Jobs, Fabric and scale are outside scope.

| Change | Impact | Required action | Deadline or trigger |
|--------|--------|-----------------|---------------------|
| In-place migration and automated rollback | Existing deployments | Use a fresh installation; retain the prior deployment and its rollback procedure | Before upgrading an existing deployment |

## Migration or Rollback Guidance

### Upgrade or Migration

No in-place migration is qualified. Install as a new deployment and retain the existing deployment until the new one is accepted.

### Rollback or Hold Guidance

- Pause rollout if the installed wheel hash differs from the GitHub release manifest or the deployment is outside the qualified profile.
- Roll back by following the existing deployment runbook; automated rollback was not qualified.
- Ask for help in: the project release review.

## Known Issues and Support

| Issue | Who is affected | Workaround or next step |
|------|------------------|-------------------------|
| The C17 classic Spark Jobs CREATE attempt was refused because the organization has no associated worker environments; no cluster or workload was created. | Operators using the classic route | The classic Jobs route is outside this release scope. See the C17 refusal evidence. |
| Fabric integration is unfinished; the 1B-node / 5B-edge scale trial remains held | Fabric and large-scale users | Do not claim those capabilities or performance. |
| R61's Weft result values were observed, but a durable result hash was not persisted in the full worker closeout | Qualification reviewers | See the evidence limitation in the R61 closeout. |

## References

- Release gate: [Ashlar release closure](../04-build/release-closure-20261101.md)
- Local R61 evidence: R61 qualification closeout
- Local A1 recovery: attempt14 recovery closeout
- Publication: v0.1.0 publication closeout
- Managed R23/R25 evidence: final closeout provenance, raw R25 qualification receipt, and R23 A0 / RC10 source probe
- Support or escalation path: project release review
