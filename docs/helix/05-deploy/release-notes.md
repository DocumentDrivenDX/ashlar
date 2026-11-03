---
ddx:
  id: ashlar.release-notes-20261103
  type: release-notes
  activity: 05-deploy
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.release-closure-20261101
      kind: informed_by
---

# Release Notes — 0.1.0rc1 candidate (draft, not for distribution)

> No 0.1.0rc1 package, tag, or release has been created. These notes are a
> candidate summary for review, not a description of shipped software.

## Release Scope

- Release identifier or version: proposed `0.1.0rc1`; package metadata remains `0.1.0.dev1`.
- Release date: not set.
- Rollout window or environment: provider-cache candidate only, pending qualification; no rollout is scheduled.
- Release owner: not assigned in this draft.
- Release version/build: not built as `0.1.0rc1`.

## Audience and Channels

| Audience | Why they care | Delivery channel |
|----------|---------------|------------------|
| Internal reviewers | Confirm exact candidate scope and unresolved gates | This draft and linked evidence |
| Operators and support | No operational rollout is authorized | None until release approval |
| End users | No released change is available | None |

## Highlights

Proposed candidate highlights, subject to the remaining managed and classic Spark gates:

- Define property graphs on versioned Delta tables with native singleton reads independent of Fabric Graph.
- Preserve source, publication-vector and acknowledgement custody through the selected local Truss workflow.
- Export canonical graph bytes for a bounded GraphFrames action while preserving parallel edges and isolated nodes.

## Required Actions Summary

- Users: no action; no release is available.
- Operators: do not deploy this candidate. The managed target gate remains pending.
- Support: no customer-facing announcement or support commitment is authorized.

## Changes and Fixes

### New or Improved

| Area | What changed | Who is affected |
|------|--------------|-----------------|
| Delta graph publication | The local R61 qualification exercised the `.4-xxhash64` carrier with Truss 0.0.55, UMF SDK 0.10.2 / Core 0.10.6 and model 0.10.0. | Reviewers of this candidate only; local-development evidence does not establish managed target support. |
| Source custody | A local attempt14 recovery independently compared four native Delta role bags to the source oracle before reconciling one pending A1 ACK. | Qualification reviewers; recovery records a source-hold discontinuity. |

### Fixes

| Issue or symptom | Resolution | User or operator impact |
|------------------|------------|-------------------------|
| The local A1 prefix builder had used the admission cursor where the original genesis cursor was required. | The builder now keeps original genesis at cursor zero while the continuation admission bundle remains separately bound to its live cursor. | Prevents rebinding the original source prefix to a later checkpoint. |
| Truss install transactions could exceed the default operation window on the selected local setup path. | Truss 0.0.55 exposes a bounded install-only operation timeout override; general operation and statement caps remain unchanged. | Local setup can select a longer bound for that install operation. |

## Breaking Changes and Required Actions

No compatibility or migration conclusion is ready for release. The full managed and classic target gates remain open; do not infer that no breaking changes exist from this draft.

| Change | Impact | Required action | Deadline or trigger |
|--------|--------|-----------------|---------------------|
| Candidate version and final compatibility scope are not frozen | All adopters | Wait for final release notes and exact artifact pins | Before any installation |

## Migration or Rollback Guidance

### Upgrade or Migration

No upgrade path is authorized by this draft. Do not migrate a production installation using this candidate.

### Rollback or Hold Guidance

- Pause rollout when: the managed target, classic Spark, or release artifact gate is incomplete.
- Roll back using: no candidate rollout exists; follow the deployed release's existing runbook.
- Ask for help in: the project release review.

## Known Issues and Support

| Issue | Who is affected | Workaround or next step |
|------|------------------|-------------------------|
| Managed target qualification remains pending | Operators considering managed deployment | Do not deploy; await the exact managed gate closeout. |
| The dedicated managed classic Spark Jobs trial is pending | Operators using the classic route | Do not claim managed classic Jobs support before that trial closes. |
| Fabric integration is unfinished; the 1B-node / 5B-edge scale trial remains held | Fabric and large-scale users | Do not claim those capabilities or performance. |
| R61's Weft result values were observed, but a durable result hash was not persisted in the full worker closeout | Qualification reviewers | See the evidence limitation in the [R61 closeout](../04-build/evidence/current-upstream-release-20261102/r61-fresh-truss055-attempt13-continuation/qualification-closeout.json). |

## References

- Release gate: [Ashlar release closure](../04-build/release-closure-20261101.md)
- Local R61 evidence: [R61 qualification closeout](../04-build/evidence/current-upstream-release-20261102/r61-fresh-truss055-attempt13-continuation/qualification-closeout.json)
- Local A1 recovery: [attempt14 recovery closeout](../04-build/evidence/current-upstream-release-20261102/managed-current-attempt14-a1-recovery/closeout.json)
- Support or escalation path: project release review
