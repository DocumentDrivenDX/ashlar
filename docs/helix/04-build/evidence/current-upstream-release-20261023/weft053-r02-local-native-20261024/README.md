# R02 public-runtime local qualification plan

Status: **effect-free plan; no native operation has run in this follow-up**.
The public Weft 0.4.53 target-matched compiler replay is recorded in the sibling
[`weft053-r02-spark42-public-target-20261024`](../weft053-r02-spark42-public-target-20261024/README.md)
receipt. It closes compiler compatibility only.

Run `python3 preflight.py` from this directory for the effect-free static
readiness check. It verifies the pinned UMF model, configured source, 13-event /
three-batch fixture and public Weft receipt targets. It does not inspect process
state or start Spark/Java. The result is `prepared_native_held` until R12 releases
the slot, the approved process inventory is obtained, and fresh request/artifact
bytes are bound to the new `.4-xxhash64` publication. The retained Weft replay
uses `.3` binding examples, so its response bytes cannot be reused as R02 native
evidence. This packet does not yet contain a native executor: the existing
configured-local command requires an operator-owned per-invocation configuration
and the fresh publication vector, both of which must be created after the slot
release gate without changing the frozen source tuple.

The prior `current-native-query-and-sdk-pages-20261021.json` receipt remains
historical evidence: it used a private Weft build and an `ashlar-delta/0.3`
publication, and it does not retain a reopenable R02 table path. Reopening that
fixture alone would not qualify R02. The R02 row in the
[toolkit delivery plan](../../../toolkit-delivery-plan.md) requires the selected
public UMF 0.10.1 distribution, a fresh small create/update/delete publication,
exact replay, and publication-bound native singleton plus both Weft paths.
UMF's model semantics remain pinned to core 0.10.0; 0.10.1 is the selected
public Python distribution, as recorded in the
[UMF adoption receipt](../umf-0.10.1-ashlar-adoption-20261024/README.md).

## Bounded execution sequence after R12 slot release

1. Establish that R12 has released the local slot. Then obtain an observable
   process inventory using the approved escalated process-inspection route and
   record that no competing Spark/Java workload owns the slot. The earlier
   sandbox observations (`ps` EPERM and `pgrep` sysmon unavailable) are a
   tooling limitation, not proof of an occupied slot or a permanent blocker.
2. Use a new, uniquely named, operator-owned directory under `/private/tmp`
   for the installation, Delta database/warehouse, source snapshots, journals
   and receipts. Admit the selected public UMF 0.10.1 package and validate the
   unchanged core 0.10.0 model from
   [`examples/end-to-end/typed-graph-model.umf.json`](../../../../../../examples/end-to-end/typed-graph-model.umf.json).
   Use UMF's checked schema/DDL outputs. Use the Truss-independent local source
   and binding in
   [`examples/end-to-end`](../../../../../../examples/end-to-end/typed-graph-README.md)
   (`configured-typed-graph-source.json`, `typed-graph-source.jsonl`, and
   `typed-graph-binding.json`); preserve their exact bytes and hashes. This is
   a bounded local development source profile and makes no Truss acceptance,
   feed, ACK or production-authority claim.
3. Select `ashlar-delta/0.4-xxhash64` explicitly and run the configured-local
   profile on macOS arm64/Python 3.12, Spark 4.2.0, Delta 4.4.1 and JDK 21
   with the exact hashed Delta 4.4.1 Spark 4.2/Scala 2.13 JARs. Use one serial
   `local[2]` Spark session, at most a 2 GiB driver
   heap, and a 10-minute outer wall-clock deadline. Freeze the complete runtime
   and JAR identity before the first effect. Create a fresh installation,
   publish the source's create, update/replace and delete batches in order,
   and repeat the last completed request byte-for-byte to establish exact
   replay with no additional graph or manifest commit. Retain each publication's
   full version vector and the original operation/source custody.
4. Against the fresh first publication, perform the native singleton read and
   the Backend03 grouped graph query plus Backend02 SDK page and continuation.
   Use public Weft 0.4.53; preserve the R02 SQL and request semantics from the
   [`managed-current-uc-program-parity-v0.4.50.json`](../../current-upstream-release-20261021/local-current/managed-current-uc-program-parity-v0.4.50.json)
   source. Bind the requests to this fresh publication's complete vector and
   compile them through the public wheel; keep each exact response artifact
   unchanged through native execution. The previous compiler-only receipt's
   response bytes are not publication-bound artifacts for the new directory.
   Compare the exact singleton values, grouped keys/counts/path multiplicities,
   page boundary, continuation identity and complete result bag against an
   independent oracle computed from the preserved source bytes. Verify the
   later update/delete publications and exact replay against that same
   source-derived state oracle. Recheck runtime, source custody and vector at
   close; release no partial result after any failure.

Use the existing fixture's bounded 13 source events and three batches; do not
repeat the platform matrix, rebuild the historical publication, alter compiler
SQL, edit Ashlar adapters or `pyproject.toml`, or change the frozen R12 source
tuple. Native effects remain gated on the observed R12 slot release and an
exclusive process inventory. Record exact package/source/JAR hashes, operation
and vector receipts, Weft request/artifact/response hashes, native reports,
oracle comparison, process inventory and elapsed/resource observations under
this evidence directory. A missing prerequisite or a close-time check failure
ends the attempt without returning public results.

## Governing sources

- [R02 release row](../../../toolkit-delivery-plan.md)
- [Configured-local current-core runtime and Weft read contracts](../../../../02-design/contracts/CONTRACT-002-consumer-read-boundary.md)
  and [resolver/runtime identity](../../../../02-design/contracts/CONTRACT-004-publication-resolver.md)
- [Configured typed-graph source and publication workflow](../../../../../../examples/end-to-end/typed-graph-README.md)
- [Public Weft 0.4.53 R02 target replay](../weft053-r02-spark42-public-target-20261024/README.md)
- [Public UMF 0.10.1 adoption and qualification limits](../umf-0.10.1-ashlar-adoption-20261024/README.md)
