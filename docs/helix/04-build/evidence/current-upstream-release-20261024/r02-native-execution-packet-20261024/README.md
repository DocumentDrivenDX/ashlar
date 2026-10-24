# R02 current-core local native execution packet

Status: **prepared for independent review; no Spark, Java, publication, native
read, cloud, or source/ACK action was run for this packet**. This packet fixes
the evidence inputs, oracle, execution order, and attempt limits. Sol must
review it and explicitly direct execution before any native action starts.

## Scope and limits

One attempt, one serial `local[2]` Spark session, macOS arm64 / CPython 3.12,
Spark 4.2.0, Delta 4.4.1, JDK 21, and the public UMF core 0.10.1 distribution
with model semantics fixed at 0.10.0. Use the two exact Delta 4.4.1
Scala 2.13/Spark 4.2 JARs declared by the checked local Delta profile and
record their byte hashes in the runtime admission receipt before initialization.
The driver heap limit is 2 GiB; total wall time is 300 seconds from the first
runtime invocation; retained packet output is capped at 16 MiB total, with each
individual request/response or command receipt capped at 1 MiB. Stop on the
first refusal, timeout, mismatch, hash drift, or failed closing check. Do not
retry or run a platform matrix. These ceilings fit inside the configured-local
CLI limits and are lower than its 600-second maximum.

The current local Spark slot was reported free after the bounded R05 action.
Before the attempt, record the approved process inventory and confirm there is
no competing Spark/Java workload. Freeze the Python executable, JDK, Spark and
Delta identities, both JAR hashes, public UMF wheel hash, public Weft wheel
hash, source inputs, and generated invocation bytes before the first effect.
The precise selected UMF wheel is the macOS arm64 asset SHA-256
`49459bb2dbc37ac9b62b269e9664bac1483439c7d4c79f0b07855e9c8546c823`; its
public install and package integrity evidence is the UMF adoption receipt.
The Weft 0.4.53 macOS arm64 wheel SHA-256 is
`4c8281945b852af02cae8602747fb7b1b652ab3a1369d7650efce62ae6d35c6c`.

## Frozen inputs and compiler artifacts

The effect-free preflight at
[`weft053-r02-local-native-20261024`](../../current-upstream-release-20261023/weft053-r02-local-native-20261024/README.md)
checks these unchanged source bytes and confirms 13 events in three batches:

| Input | SHA-256 |
|---|---|
| `examples/end-to-end/typed-graph-model.umf.json` | `19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b` |
| `examples/end-to-end/configured-typed-graph-source.json` | `040545b8cc96701a158b8936750c89f97ce991df9652ceeacf6c66fb9d9fc168` |
| `examples/end-to-end/typed-graph-source.jsonl` | `9edb7566f82324fadb1e7cf1ba0eaf22691096a26209e938c83730d096d4882b` |
| `examples/end-to-end/typed-graph-binding.json` | `1ab1416d5a36a082c4d5be38dc061377bdae22bb5395fb8cfbec8cee80910863` |
| Original parity source `managed-current-uc-program-parity-v0.4.50.json` | `dd5cef72e28b120f6a81d79d941068cbc3b099f4276dcc386c631bb06f721887` |

The public Weft target replay contains the exact original Backend03 request
`db32c552f3de31fc8f4f11f0b1a7edfb0645026ddbe2a72b6b3f4ea6ac2e74d5` and
Backend02 request `3206e3c16f7721699a61974e0419b015bb0728a4d6b697e734f68a3e873ae729`.
Its retained compiler responses are preserved as original comparison inputs:

| Artifact | Gzip bytes SHA-256 | Response SHA-256 |
|---|---|---|
| Backend03 grouped query | `ab28bf7dc03af01e49d538eb0a5b04af0039550333c33a30c88f74d3d3968490` | `be2e32908d9175c5206aa9a4a9c5383b8aa6eaee282b295bd9d9e890c5d4e7c9` |
| Backend02 page 1 | `cec44462b53279533e89a22b1d264501f87272161baba26b61bc147ce1201ca4` | `879af9e863ef97016106a33ec95bb80a35095340cb1aa0cb2bfb5e68c2e3cdcb` |
| Backend02 continuation | `b5c06a8021779ba6ae6da6ca0a742281249d3c31d201926bfbdb3c16e234f76b` | `d96f63e9786f4b8d4e495708f9799edd8b9c7a66efbcbc88e0fee5eab89e98c4` |

These compiler response bytes bind `ashlar-delta/0.3`; they are not sent to the
native executor. After the fresh `.4-xxhash64` publication is complete, compile
the same two source requests with public Weft 0.4.53 against the complete fresh
publication vector, and create the Backend02 size-2 page and continuation
(`after=["person-2"]`). Hash and preserve each exact new request, binding,
artifact, and response before executing those unchanged bytes. A binding that
does not name every table UUID/version in the captured vector is a refusal.

## Independent expected results

Compute expected values only from the frozen original model, source JSONL, and
binding bytes, without using SQL output or the native result. Use the current
source-derived projection oracle in
`src/ashlar_host/configured_typed_graph_weft_projection.py`
(SHA-256 `e1fbaea90fc697e69dd7977f0e0cabccd2e149d5fbb916a58a4682cd44e050ea`)
for grouped path keys/counts, distinct terminal counts, duplicate path
multiplicity, and exact grouped result bag. Use
`src/ashlar_host/configured_sdk_page_oracle.py` (SHA-256
`26470d6536d473e17fd42aefdb58e56fd302cd19aad47d2433cd843eb8a51ffc`) for
original SDK page metadata and continuation. Independently derive singleton
identity/type/value/presence expectations from the same source events. Compare
the exact result bags, not sorted or deduplicated approximations; require the
page boundary and continuation identity to match the authored-key oracle.
Check each of the create, replace/update, delete, and replay states against the
source-derived identities and values. Exact replay must add no graph or
manifest commit.

## Serial execution order

1. Re-run `python3 preflight.py` from the existing R02 preflight directory and
   retain its JSON. Confirm all four input digests and all three public compiler
   response hashes. Capture the approved process inventory and the full runtime
   and JAR identities. A missing identity or competing process stops the run.
2. In a fresh private directory, admit the public UMF 0.10.1 package and
   validate the frozen UMF 0.10.0 model through UMF's public validation and DDL
   outputs. Freeze the exact output digests. Initialize a fresh
   `ashlar-delta/0.4-xxhash64` installation with the configured-local runner.
3. Using one unchanged source/configuration/binding, publish batches 1, 2 and 3
   sequentially; repeat batch 3 byte-for-byte with a fresh invocation output.
   Retain full publication vectors at each ordinal and prove replay adds no
   table or manifest commit.
4. At the first publication, issue one native singleton read, then the
   publication-bound Backend03 grouped query and Backend02 page 1 followed by
   continuation. Each invocation consumes only its frozen exact config and
   compiler response. Compare each result with the independent oracle above.
5. Recheck complete native vector, file/runtime hashes, source/config custody,
   and process ownership before returning success. Any failed closing check
   invalidates the attempt and suppresses partial results.

The runner entrypoint remains
`python -m ashlar run-configured-local --configuration <absolute invocation.json>`;
the operation-specific invocation must be created only in the private
attempt directory and its exact bytes/digest recorded before each invocation.
Use one fresh output path per operation. Configuration must select an explicit
Spark 4.2.0 / Delta 4.4.1 runtime, `local[2]`, 2 GiB driver memory, bounded
stream/receipt sizes, and `ashlar-delta/0.4-xxhash64`; never use a shared
Databricks cluster.

## Focused preflight outcome and open evidence

The existing effect-free `preflight.py` completed successfully with status
`prepared_native_held`: all pinned source bytes, 13 events/3 batches, the
original parity source, and all three Weft 0.4.53 target/response pairs matched.
It specifically does not admit UMF at runtime, inspect processes, or establish
Spark/Delta execution. The historical R02 runtime receipt
`current-native-query-and-sdk-pages-20261021.json` (SHA-256
`37dd8c330a5e65ad5395827e1bc839976e0a76f1e77a1726eae552edaa54cf1d`) records
private Weft and `.3` bindings and is not current native evidence.

Before execution, this packet still requires an approved current process
inventory, exact downloaded JAR hashes, a fresh UMF validation output, and the
new `.4` publication-bound compiler responses. This packet makes no Truss,
production source, protected ACK, Unity Catalog, or cloud authority claim.

## Governing artifacts

- [R02 release requirement](../../../toolkit-delivery-plan.md)
- [Product requirements](../../../../01-frame/prd.md)
- [Publication boundary](../../../../02-design/contracts/CONTRACT-001-publication-boundary.md)
- [Consumer read boundary](../../../../02-design/contracts/CONTRACT-002-consumer-read-boundary.md)
- [Delta graph tables](../../../../02-design/contracts/CONTRACT-003-delta-graph-tables.md)
- [Publication resolver](../../../../02-design/contracts/CONTRACT-004-publication-resolver.md)
- [Typed graph source workflow](../../../../../../examples/end-to-end/typed-graph-README.md)
- [Public UMF 0.10.1 adoption](../../current-upstream-release-20261023/umf-0.10.1-ashlar-adoption-20261024/README.md)
- [Public Weft R02 target replay](../../current-upstream-release-20261023/weft053-r02-spark42-public-target-20261024/README.md)
