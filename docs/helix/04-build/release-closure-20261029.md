---
ddx:
  id: ashlar.release-closure-20261029
  type: implementation-plan
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.prd
      kind: informed_by
    - id: ashlar.toolkit-delivery-plan
      kind: refines
    - id: TP-001
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: ashlar.evidence.truss051-r19-pre-effect-failure-20261031
      kind: informed_by
    - id: ashlar.evidence.truss051-r20-runtime-cohort-20261031
      kind: informed_by
    - id: ashlar.evidence.managed-a1-readonly-refresh-20261031
      kind: informed_by
    - id: ashlar.evidence.source-holder-a1-live-port-gap-20261024
      kind: informed_by
    - id: ashlar.evidence.r05-production-hardware-napkin-20261031
      kind: informed_by
    - id: ashlar.evidence.g03-sdk010-source-admission-gap-20261031
      kind: informed_by
    - id: ashlar.evidence.g03-sdk010-source-admission-candidate-20261031
      kind: informed_by
---

# Release closure sequence — 2026-10-29

This is the current execution order for the existing P01–P11/R01–R06 delivery
obligations. It changes no product requirement or contract approval. An evidence
receipt qualifies only its exact source, runtime, model and supported subset.

| Gate | Work and acceptance | Current disposition |
| --- | --- | --- |
| 1. Current public cohort | Select immutable UMF SDK 0.10.0 / semantic Core 0.10.0, Python Core 0.10.4, Access dev8, Weft 0.4.56 and Truss 0.0.51 by source and digest. Focused package, schema and compiler checks precede native use. | Exact SDK tarball and macOS Core wheel digests match their published assets. Core 0.10.4 installed with Access dev8 and Weft 0.4.56; selected imports and `pip check` passed. SDK 0.10.0 source-backed Delta 4 bridge matched all six frozen proposals under MiniRacer, and an offline wheel/sdist build contained the exact pinned resources. The Linux Core wheel digest is release-supplied but the asset was not downloaded or executed. No native or managed publication is qualified by these checks. [Latest adoption](evidence/current-upstream-release-20261031/README.md), [SDK 0.10 runtime](evidence/current-upstream-release-20261031/sdk010-delta4-runtime-integration/README.md). |
| 2. Local current-source lifecycle | Run one reviewed Truss .51 local source lifecycle: .48→.51 migration, SDK 0.10.0 `.4` genesis and protected ACK, seven-operation seed, verified continuation and protected ACK, then the eight ACK-gated A/B slots on admitted sources. | R16 and R17 approvals are spent; their failed results and retained recovery custody remain recorded. The approved R19 one-shot failed before native effects when its isolated child could not import the current SDK bridge ([failure receipt and logs](evidence/current-upstream-release-20261031/truss051-r19-failure-evidence-20261031/README.md)). The fresh [R20 packet](evidence/current-upstream-release-20261031/truss051-r20-runtime-cohort/README.md) pins a clean current-source archive and passes 37 focused effect-free tests plus isolated source/runtime preflight; R20 is proposed, unapproved, and has not run. Native source acceptance, feed/ACK evidence and A/B slots remain open. |
| 3. Current-core native consumer | On a publication-pinned `.4-xxhash64` graph, run the original Weft 0.4.56 Backend03 grouped and Backend02 page/continuation artifacts and native singleton under exact source oracle. Preserve .53/.50 historical gates as historical. Verify query identity, values, multiplicity and complete publication vectors. | `.56` package/compiler checks pass; no `.56` `.4` native query execution. R1 stopped at ZSTD admission and r2 at six-role alias preflight; both are consumed. SDK 0.5.8 passes effect-free source correspondence and its six proposals match the historical bridge. Its explicit `/0.2` cutover ran in one bounded local r3 attempt: all six Delta roles initialized, then transaction 1 reached the 450-second deadline after source admission and metadata reservation but before the first metadata operation. The journal has `prepared` and `applying` but no operation; six Delta heads remain at initialization v1. The consumed r3 attempt did not produce a graph commit, `applied`, manifest, realized vector, consumer, or ACK. R3 has no causal timing trace; the effect-free checker and custody costs are low. SDK 0.10.0 source admission remains unwired in main, with an [isolated focused-tested candidate](evidence/current-upstream-release-20261029/g03-sdk058-txn1-timeout-r3-20261030/sdk010-source-admission-candidate-20261031.md) pending merge after R20. A fresh bounded native packet and complete transaction result remain required before claiming the current cohort. [G03 candidate](evidence/current-upstream-release-20261029/g03-weft056-delta4-consumer-candidate/README.md), [SDK comparison](evidence/current-upstream-release-20261029/g03-sdk058-delta4-realization-candidate-20261030/README.md), [source correspondence](evidence/current-upstream-release-20261029/g03-sdk058-source-correspondence-20261030/README.md), [local cutover](evidence/current-upstream-release-20261029/g03-sdk058-local-cutover-candidate-20261030/README.md), [r1 refusal](evidence/current-upstream-release-20261029/g03-weft056-delta4-consumer-candidate/transaction1-zstd-preflight-failure-20261030.md), [r2 refusal](evidence/current-upstream-release-20261029/g03-weft056-delta4-consumer-candidate/transaction1-role-alias-preflight-failure-20261030.md), [r3 timeout](evidence/current-upstream-release-20261029/g03-sdk058-txn1-timeout-r3-20261030/README.md). |
| 4. Managed production target | Admit one dedicated `client_dev` trial with exact runtime, UC mode, source/ACK custody and a total cost below $100, aiming below $10. Keep predictive optimization enabled and delete only the owned schema after evidence capture. | The owner selected the [15-minute operational-retention profile](evidence/managed-a1-operational-retention-profile-proposal-20261031.md), with exact four-role reads at open, pre-ACK and close and explicit administrator/storage exclusions. The [read-only A1 refresh](evidence/managed-a1-readonly-refresh-20261031.md) confirms catalog grants and the visible Personal Compute policy only; it does not admit a dedicated Jobs cluster, runtime/task attachment or cost. The [A1 live read/pin seam review](evidence/source-holder-a1-live-port-gap-20261024.md) documents the missing read-only versioned-publication and native-pin custody adapters. Source/ACK holder, runtime/access mode and total cost remain unadmitted. No paid compute has started. |
| 5. Scale and graph export | Redesign the end-to-end approach before reconsidering one full 1B/5B load, two point lookups, two persisted scans and one graph-engine export. Preserve exact Delta semantics, identity, values, multiplicity and both full-scan accounting. | Owner decision (2026-10-29): **“Hold and redesign first.”** The current sample does not support the provisional five-minute goal: writes alone project to 14.18 minutes at 80% of observed throughput, and edge-only CPU scales to about 8.0 minutes at 80% utilization of 18 cores. The 28.21-minute measured bundle covers setup, writes and one combined row/value and endpoint-formula scan; the second scan and other phases remain unmeasured. The 2026-10-31 [production-hardware napkin](evidence/r05-production-hardware-napkin-20261031.md) estimates 256 executor cores and about 1.0 GB/s sustained aggregate storage throughput as a planning candidate under linear scaling assumptions; this is not a production benchmark or capacity qualification. The former 45M/225M phase pilot is behind a small 5M-edge mechanism discriminator for per-column Parquet statistics; that micro-pair is unrun and unapproved, not a scale gate. No pilot or full 1B/5B run is authorized. [R05 redesign napkin](evidence/r05-redesign-napkin-math-20261029.md), [R05 packet](evidence/r05-one-shot-full-scale-run-packet-20261029.md), [pilot scope](evidence/r05-phase-pilot-candidate-20261029/README.md). |
| 6. Consumer and release handoff | Qualify only versioned engine subsets supported by native evidence. Build/install stable wheel and sdist from reviewed main, verify selected resources and licenses, then tag and publish the exact source and artifacts locally. | The 0.1.0.dev1 development artifacts passed focused inspection. The [local GitHub publisher](pypi-publishing.md) and five focused tests are ready; immutable releases are enabled. Stable version, remaining native/managed gates, exact tag and published release remain open. |

Gate 3's recorded r1–r3 results belong to the earlier SDK 0.5.8 / Core 0.10.3
cohort. The source-backed SDK 0.10.0 bridge has passed six-role proposal parity.
R19 selected it for actual local Delta DDL but its approved one-shot failed before
native effects at isolated child import admission; the receipt and logs remain
preserved. The fresh R20 packet passes focused effect-free tests and source/runtime
preflight, but remains unapproved and unrun. No Truss source acceptance, native
query, publication or ACK is qualified by these steps. Gate 4's 2026-10-31
read-only refresh confirmed `client_dev` schema grants and effective predictive
optimization `ENABLE`, but showed only a Personal Compute policy to the
authenticated principal. The owner selected the
[bounded operational-retention profile](evidence/managed-a1-operational-retention-profile-proposal-20261031.md)
for A1: exact original-version reads at open, pre-ACK and close within 900
seconds, with explicit administrator/storage exclusions. The [read-only seam
review](evidence/source-holder-a1-live-port-gap-20261024.md) still identifies
missing production publication-read and native-pin custody adapters. The observed
policy suggests one D4ds_v5 driver and zero workers; API creation, task attachment,
runtime/UC mode, live source/ACK custody and total cost remain unadmitted. No
managed run has started. [Read-only refresh](evidence/managed-a1-readonly-refresh-20261031.md).
The R05 production-hardware napkin is only a conditional planning sensitivity;
it does not qualify production capacity or authorize a scale trial
([hardware napkin](evidence/r05-production-hardware-napkin-20261031.md)).

Gate 6 now has a [local GitHub release publisher](pypi-publishing.md) with five
focused passing tests. It enforces exact reviewed `origin/main`, a clean stable
tag and the repository's enabled immutable-release policy, then builds and
verifies wheel/sdist assets before draft staging and publication. The project
remains `0.1.0.dev1`; no stable tag or GitHub release has been created.

Run one focused check for each changed boundary and one production-target trial
for its accepted subset. Do not repeat historical platform matrices or use the
shared default cluster. Preserve failed/uncertain source, ACK and native custody
for reconciliation rather than retrying a possible effect. A missing external
engine, authority, or cost input remains a release gate; it is never a simulated
pass.
