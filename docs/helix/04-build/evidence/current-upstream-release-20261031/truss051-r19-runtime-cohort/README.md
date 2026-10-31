---
ddx:
  id: ashlar.evidence.truss051-r19-runtime-cohort-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss051-r18-native-candidate-20261031
      kind: informed_by
    - id: ashlar.evidence.sdk010-delta4-runtime-integration-20261031
      kind: informed_by
    - id: ashlar.evidence.umf010-upstream-adoption-20261031
      kind: informed_by
    - id: ashlar.evidence.truss051-r16-native-candidate-20261029
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# Truss 0.0.51 R19 runtime cohort candidate

**Status: proposed, not approved; no native action.** R19 is a fresh local
full-lifecycle candidate derived from R18. Its migration worker binds the
versioned SDK 0.10.0 source bridge and exact source-policy/resource hashes;
its isolated Python runtime binds UMF Python Core 0.10.4. R18 remains the
historical SDK 0.5.8 cohort. The R19 scope reuses the exact recovered R16
selected and cold-backup inputs and receipts, and assigns new attempt,
destination, output, and supervisor paths.

The serialized [R19 scope](full-lifecycle-scope-proposal-r19.json) has SHA-256
`bfe4f9623efc3abe130b05f9f6188e95b681b3d446c44ce418d4591898c59c62`. It pins
custody-prefix worker `6ba8526a1ecb0c32e906e08652e54ed940a27d859400f4fb7e034a1d776d0f1a`,
migration/readback worker `385a761893f533c5efb952b05393edd2431d4063c235e7b0ce4f2d47c910a4ac`,
and supervisor `25d1519e2fee2c8802b0f3a3ee5c544160726cf90f2a4f60979c97a181f0b147`.
R19 passes `umf_sdk_version` from its scope into the public `publish_graph_pages`
route. That version selects the SDK 0.10.0 six-role table creator for actual
Delta DDL; the default and SDK 0.5.8 creator remain explicit separate routes.
The resulting versioned installation profile is admitted by the local journal
and can be rechecked by the `.4` profile verifier. Continuation reopens that
saved profile and verifies it with the same SDK 0.10.0 bridge before reading
the next page; it does not recreate the tables. The bridge profile is
`ashlar-local-current-delta4-umf-source-sdk010/0.1`, with SDK archive SHA-256
`2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c`, bridge
bundle SHA-256 `4d0ce3902368d0d1a73312626ec153125992327ee812cd579282445514067735`,
resource manifest SHA-256 `59ea8c9ac104c7ecc328699b8f752c5ae42a2c33bee49eace5616be050df8356`,
and source-policy SHA-256
`ebdaa3c27e917fc2ed91459fbd4eb3f36e2d57489e75921cb03c9b67490d017f`.

The isolated R19 environment was copied from the retained R16 Truss runtime,
then only Core was replaced with the official macOS arm64 Core 0.10.4 wheel
(SHA-256 `a3a4a68fcc4dfb637d7e9229ac7f6cbf029e464442304e6e49f8fc0c2467845e`).
Truss 0.0.51, Weft 0.4.56, PySpark 4.2.0, Delta Spark 4.4.1, and MiniRacer
0.14.1 remain pinned. The R19 validator checks the installed Core distribution
version and direct wheel digest before it can admit a populated scope.

All 35 focused effect-free tests pass, including the real inert scope validator,
recovered R16 receipt/tree custody, runtime distribution checks, and a local
MiniRacer six-role SDK 0.10.0 bridge probe. The candidate retains R18's 900-second
total ceiling, 2 GiB process-group RSS cap, 2 GiB retained-plus-attempt cap,
512 MiB attempt-root cap, sequential local execution, zero retries, and no cloud
resources. The proposal has `approval_artifact_created: false`; no approval
file, R19 output root, or supervisor output directory exists. No PostgreSQL
start, migration, Spark, Delta write, publication, ACK, seed, continuation, or
cloud action was run. Before any separately authorized use, the recovered input
trees, ephemeral R19 runtime path, wheel pins, source pins, code hashes, and
fresh immediate pre-start process inventory still require revalidation.

Run the focused checks with:

```sh
PYTHONPATH=.:/Users/erik/Projects/ashlar/src \
  /private/tmp/ashlar-r19-truss051-runtime-cohort-20261031/bin/python \
  -m unittest -v test_r19_supervisor test_r19_pre_migration_custody \
  test_r19_post_migration_admission
```
