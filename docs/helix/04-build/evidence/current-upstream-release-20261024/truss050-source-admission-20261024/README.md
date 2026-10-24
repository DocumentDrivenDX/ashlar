---
ddx:
  id: ashlar.evidence.truss050-source-admission-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.toolkit-delivery-plan
      kind: informed_by
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: ashlar.evidence.truss050-package-cohort-qualification-20261024
      kind: informed_by
---

# Truss 0.0.50 versioned source-admission implementation

Status: **versioned Ashlar admission and downstream binding support is implemented and focused effect-free tests pass. This is not direct .50 source/feed lifecycle evidence, an R03 completion, or execution approval.**

R03 calls for adding the official Truss 0.0.50 release as a new versioned provider/admission profile while preserving prior release profiles and the accepted .48 source/consumer custody. This change gives .50 its own exact wheel and runtime-manifest pins, catalog evidence profile `/0.5`, and graph-admission profile `/0.5`. Its legacy `ashlar-delta/0.3` managed-core carrier uses binding profile `/0.4`; the `ashlar-delta/0.4-xxhash64` carrier deliberately retains physical binding profile `/0.3`, with the .50 source and runtime pins still bound independently. The existing .49 profiles and the historical R12 packet are retained. A separate .50 R04 feed adapter verifies the installed .50 profile before using the shared bounded read-only page-shape validator, with release-correct refusal messages.

The adapter continues to apply Ashlar's accepted-source report and complete-view correspondence rules. The .50 package/cohort receipt establishes wheel and import/API-surface coexistence only. Direct .50 catalog method behavior, source/feed lifecycle, two-edge migration, publication, independent verification, ACK custody, and production authority remain unqualified. CONTRACT-001's source and publication boundaries and CONTRACT-003's managed Delta table/publication-vector contract remain controlling; this admission change does not grant source registration, ACK, publication, or execution authority.

Focused tests exercised exact .50 admission and refusal, catalog-reader provenance, managed-core and Weft binding pins, SQL/Spark receipt profile parsing, and the version-specific bounded feed adapter. The seven selected unittest modules passed (73 tests) using the effect-free .50 runtime cohort. `compileall` passed for all changed Python modules. The .50 distribution reported version `0.0.50`, wheel SHA-256 `7b0483bb0c624c98515cf38ec30bbbf1ca43c1250e8880e5d00d520b4b7eec00`, and catalog runtime manifest SHA-256 `288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4`.

No PostgreSQL, Spark, migration, source, publication, ACK, or native action was started or invoked. The historical R13 packet remains blocked and unchanged; a later lifecycle packet must bind the post-code Ashlar source commit and perform its own gates.
