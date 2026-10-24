---
ddx:
  id: ashlar.commerce-fabric-candidate-path-preflight-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.toolkit-delivery-plan
      kind: informed_by
---
# Fabric candidate-path preflight addendum (2026-10-24)

**Result:** the four exact frozen candidate paths each returned HTTP 404 with
`x-ms-error-code=PathNotFound` in the root's escalated, read-only run of
[head_candidates.py](head_candidates.py). The script exited 0 in 7.845 seconds.
These are point-in-time observations only.

An earlier attempt in this sandbox failed before sending requests: Azure CLI
token acquisition for the Fabric API exited 1 at `2026-10-24T05:22:42Z`. That
attempt is distinct from the successful root run and yielded no service result.
The root's successful GET observations were: workspace GET for
`0a4dbd9c-e1a7-4ec8-8ce9-aa51991ff793`, assigned to capacity
`c69c0329-778f-4446-b529-d43358c73251` with assignment progress `Completed` in
West US 3; capacity GET reported SKU `FTL4`, state `Active`, West US 3. The
raw HTTP statuses for those GETs were not retained. Sanitized method-level
observations are retained in [result.json](result.json).

The same read-only GraphModel listing returned successfully with no
continuation. Its five names were `Ashlar_R1`, `Ashlar_R2`,
`Ashlar_Archaeology_Typed_20261016b`, `Ashlar_Ecology_Typed_20261016b`, and
`Ashlar_Commerce_Publication1_20261016a`. The two frozen candidate names in the
[fresh functional plan](../commerce-fabric-fresh-functional-plan-20261023/README.md)
were absent from that listing at observation time. The raw HTTP status was not
retained. This does not reserve either name; repeat the read-only check before
any create.

The helper's SHA-256 is
`a81dac79cdd17652a32f74fc41bb17721a32345f4a17e0bc627dc58f674328cb`. Its
Azure Storage token stayed in process memory. Output omitted tokens and all
response headers except the selected `x-ms-error-code` values. No create,
upload, compute, capacity change, activation, source action, or ACK occurred.

The 404 observations do not guarantee future path absence. They establish no
all-in $0 cost boundary and do not re-admit R1 source/ACK authority or establish
an accepted R2 source transaction. The fresh functional plan remains held and
this is not launch approval.
