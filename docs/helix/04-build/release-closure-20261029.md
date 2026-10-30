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
| 1. Current public cohort | Pin official UMF Core 0.10.3, Access dev8, Weft 0.4.56 and Truss 0.0.51 assets by source/digest. Focused package, schema and compiler checks precede native use. | Candidate pins and macOS checks are on main; Linux/native execution remains open. [R01 receipt](evidence/current-upstream-release-20261029/r01-umf-weft-pins-20261029/README.md), [R16 candidate](evidence/current-upstream-release-20261024/truss051-r16-effectfree-candidate-20261029/README.md). |
| 2. Local current-source lifecycle | Resolve the retained PostgreSQL state discrepancy without overwriting either tree. Freeze a runnable .51 worker/supervisor, expected source/consumer/seed custody and staged post-start observations; independently review exact one-run scope before any restore/start. Require real .48→.51 route, full genesis verification, protected ACK, one seed, actual continuation and protected ACK. Then exercise the eight ACK-gated A/B slots on their own admitted sources. | R15 approval is spent. R16 is effect-free preflight only; no .51 native effect is qualified. |
| 3. Current-core native consumer | On a publication-pinned `.4-xxhash64` graph, run the original Weft 0.4.56 Backend03 grouped and Backend02 page/continuation artifacts and native singleton under exact source oracle. Preserve .53/.50 historical gates as historical. Verify query identity, values, multiplicity and complete publication vectors. | .56 package/compiler checks pass; no .56 `.4` native query execution. |
| 4. Managed production target | Admit one dedicated, policy-conformant Jobs cluster and a new owned `client_dev` schema, with actual runtime/UC mode/permissions and conservative total cost under $100, preferably $10. Keep predictive optimization enabled. Use a separate verifier session and a live external source/read/pin holder through protected ACK; reconcile and close independently. Delete only the owned schema after evidence capture. | Read-only two-node candidate exists; source holder and actual cost/runtime admission remain open. [A1 packet](evidence/managed-a1-jobs-admission-packet-20261029.md). |
| 5. Scale and graph export | Redesign the end-to-end approach before reconsidering one full 1B/5B load, two point lookups, two persisted scans and one graph-engine export. Preserve exact Delta semantics, identity, values, multiplicity and both full-scan accounting. | Owner decision (2026-10-29): **“Hold and redesign first.”** Sample projects 28.21 minutes for setup, writes and one combined row/value and endpoint-formula scan; the second scan remains unmeasured. A five-minute total would require at least 5.64× speedup before remaining phases. No full run is authorized. [R05 packet](evidence/r05-one-shot-full-scale-run-packet-20261029.md). |
| 6. Consumer and release handoff | Qualify each engine only on its available, versioned subset; close Fabric/GraphFrames/PuppyGraph gaps required for the claimed release scope. Build/install the stable `umf-ashlar` wheel and sdist from reviewed main, verify their selected resources and licenses, reconcile requirement-to-evidence claims, then tag/publish the exact reviewed source and artifacts. | Current package is 0.1.0.dev1; stable tag and production claim remain open. |

Run one focused check for each changed boundary and one production-target trial
for its accepted subset. Do not repeat historical platform matrices or use the
shared default cluster. Preserve failed/uncertain source, ACK and native custody
for reconciliation rather than retrying a possible effect. A missing external
engine, authority, or cost input remains a release gate; it is never a simulated
pass.
