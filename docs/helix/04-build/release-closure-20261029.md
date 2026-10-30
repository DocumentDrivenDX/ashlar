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
---

# Release closure sequence — 2026-10-29

This is the current execution order for the existing P01–P11/R01–R06 delivery
obligations. It changes no product requirement or contract approval. An evidence
receipt qualifies only its exact source, runtime, model and supported subset.

| Gate | Work and acceptance | Current disposition |
| --- | --- | --- |
| 1. Current public cohort | Track the immutable UMF SDK 0.5.8 release and select Ashlar's exact Core 0.10.3, Access dev8 and Weft 0.4.56 package assets plus Truss 0.0.51 by source/digest. Focused package, schema and compiler checks precede native use. | The SDK 0.5.8 release body selects Access dev8, Codegen 0.1.0a5, Core distribution 0.10.3 / semantic Core 0.10.0, and Weft 0.4.56. Ashlar's Access URL now names SDK 0.5.8; its asset digest and size match the previously checked .5.7 Access wheel. Existing macOS checks remain scoped to identical bytes; Linux/native execution remains open. [R01 receipt](evidence/current-upstream-release-20261029/r01-umf-weft-pins-20261029/README.md), [R16 candidate](evidence/current-upstream-release-20261024/truss051-r16-effectfree-candidate-20261029/README.md). |
| 2. Local current-source lifecycle | Resolve the retained PostgreSQL state discrepancy without overwriting either tree. Freeze a runnable .51 worker/supervisor, expected source/consumer/seed custody and staged post-start observations; independently review exact one-run scope before any restore/start. Require real .48→.51 route, full genesis verification, protected ACK, one seed, actual continuation and protected ACK. Then exercise the eight ACK-gated A/B slots on their own admitted sources. | R16 restored the cold backup but refused before PostgreSQL start after 305.595 seconds; its approval is spent. The corrected inventory check and fresh R17 scope are inert-tested with a 900-second one-shot cap, but R17 has no approval or native run. [R16 failure evidence](evidence/current-upstream-release-20261024/truss051-r16-native-candidate-20261029/r16-failure-evidence.json); [R17 proposal](evidence/current-upstream-release-20261024/truss051-r17-native-candidate-20261029/README.md). |
| 3. Current-core native consumer | On a publication-pinned `.4-xxhash64` graph, run the original Weft 0.4.56 Backend03 grouped and Backend02 page/continuation artifacts and native singleton under exact source oracle. Preserve .53/.50 historical gates as historical. Verify query identity, values, multiplicity and complete publication vectors. | `.56` package/compiler checks pass; no `.56` `.4` native query execution. A frozen small source/oracle and five-read scope exist. The `.4` transaction-1 coordinator gate is complete on fake transport: it retains exact source/plan custody, grants only the three original operation IDs, verifies clean or committed-prefix recovery, quarantines uncertain submissions, validates receipts and complete role bags, and advances closing heads. Sol reviewed the focused 36-test `.4`/`.3` suite, including prepared-tail tamper refusal. All checks are inert. No Spark/native effect or snapshot comparison has run, so there is no release success claim. Native publication/manifest readback, realized vector, independent native consumer, `.56` bound artifacts and host adapter remain. [G03 candidate](evidence/current-upstream-release-20261029/g03-weft056-delta4-consumer-candidate/README.md), [coordinator finding](evidence/current-upstream-release-20261029/g03-weft056-delta4-consumer-candidate/transaction1-coordinator-boundary-20261029.md). |
| 4. Managed production target | Admit one dedicated, policy-conformant Jobs cluster and a new owned `client_dev` schema, with actual runtime/UC mode/permissions and conservative total cost under $100, preferably $10. Keep predictive optimization enabled. Use a separate verifier session and a live external source/read/pin holder through protected ACK; reconcile and close independently. Delete only the owned schema after evidence capture. | The `aidev-cus` workspace has a read-only two-node Jobs candidate; the usual `ashlar-dev` workspace exposed neither `client_dev` nor a dedicated Jobs policy to this principal. Exact runtime, permissions, source holder and total cost remain open. The holder needs independently admitted retention and destination-registration/ACK-scope custody. Current Databricks retention settings provide a normal window but no independently held pin across authorized cleanup. A proposed time-bounded operational profile would require an explicit weaker threat model and contract revision; deep images also need a retention provider. Neither is admitted. [A1 packet](evidence/managed-a1-jobs-admission-packet-20261029.md), [fresh candidate read](evidence/managed-a1-fresh-readonly-admission-20261029.md), [usual workspace read](evidence/managed-a1-ashlar-dev-readonly-discovery-20261029.md), [holder owner proposal](evidence/managed-a1-live-holder-owner-proposal-20261029.md), [retention assessment](evidence/managed-a1-retention-provider-assessment-20261029.md), [profile alternatives](evidence/managed-a1-retained-image-proposal-20261029.md). |
| 5. Scale and graph export | Redesign the end-to-end approach before reconsidering one full 1B/5B load, two point lookups, two persisted scans and one graph-engine export. Preserve exact Delta semantics, identity, values, multiplicity and both full-scan accounting. | Owner decision (2026-10-29): **“Hold and redesign first.”** Sample projects 28.21 minutes for setup, writes and one combined row/value and endpoint-formula scan; the second scan remains unmeasured. A provisional five-minute total would require at least 5.64× speedup of that measured path before remaining phases. A bounded, phase-resolved local pilot is scoped but has no executable runner or approval; no full 1B/5B run is authorized. [R05 packet](evidence/r05-one-shot-full-scale-run-packet-20261029.md), [pilot scope](evidence/r05-phase-pilot-candidate-20261029/README.md). |
| 6. Consumer and release handoff | Qualify each engine only on its available, versioned subset; close Fabric/GraphFrames/PuppyGraph gaps required for the claimed release scope. Build/install the stable `umf-ashlar` wheel and sdist from reviewed main, verify their selected resources and licenses, reconcile requirement-to-evidence claims, then tag/publish the exact reviewed source and artifacts. | The current-source 0.1.0.dev1 wheel and sdist were built from committed package inputs. Both artifact metadata records contain the Access 0.5.8 release URL and exact digest, and both passed name/version, license and selected-resource byte inspection. Optional-extra installation, native checks, stable tag and production claim remain open. [Current-source artifact receipt](evidence/ashlar-current-source-development-artifacts-20261029.md); [historical artifact receipt](evidence/ashlar-current-main-development-artifacts-20261029.md). |

Run one focused check for each changed boundary and one production-target trial
for its accepted subset. Do not repeat historical platform matrices or use the
shared default cluster. Preserve failed/uncertain source, ACK and native custody
for reconciliation rather than retrying a possible effect. A missing external
engine, authority, or cost input remains a release gate; it is never a simulated
pass.
