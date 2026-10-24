# R02 current-core local native execution packet

Status: **prepared for independent review; no Spark, Java, publication, native
read, cloud, or source/ACK action was run for this packet**. This packet fixes
the evidence inputs, oracle, execution order, and attempt limits. Sol must
review it and explicitly direct execution before any native action starts.

## Scope and limits

One attempt, one serial `local[2]` Spark session, macOS arm64 / CPython 3.12,
Spark 4.2.0, Delta 4.4.1, JDK 21, and the public UMF core 0.10.1 distribution
with model semantics fixed at 0.10.0. Reuse the exact R05 Spark/Delta runtime
JAR bytes listed below and record their byte hashes in the runtime admission
receipt before initialization. The driver heap limit is 2 GiB; total wall time
is 300 seconds from the first runtime invocation; the private attempt root is
capped at 512 MiB and retained receipts at 16 MiB total, with each individual
request/response or command receipt capped at 1 MiB. Stop on the
first refusal, timeout, mismatch, hash drift, or failed closing check. Do not
retry or run a platform matrix. These ceilings fit inside the configured-local
CLI limits and are lower than its 600-second maximum.

The current local Spark slot was reported free after the bounded R05 action.
Before the attempt, record the approved process inventory and confirm there is
no competing Spark/Java workload. Freeze the Python executable, JDK, Spark and
Delta identities, all five listed JAR hashes, public UMF wheel hash, public Weft wheel
hash, source inputs, and generated invocation bytes before the first effect.
The precise selected UMF wheel is the macOS arm64 asset SHA-256
`49459bb2dbc37ac9b62b269e9664bac1483439c7d4c79f0b07855e9c8546c823`; its
public install and package integrity evidence is the UMF adoption receipt.
The Weft 0.4.53 macOS arm64 wheel SHA-256 is
`4c8281945b852af02cae8602747fb7b1b652ab3a1369d7650efce62ae6d35c6c`.

The exact Spark 4.2.0/Delta 4.4.1 candidate runtime was reused from the
successful R05 R14 run. Its retained runtime inventory SHA-256 is
`973d63580aec4a5f7f396befef68d19823fcf669811e3c6957346f746e6e3505` and its
R05 native result SHA-256 is
`c7d627be28af5a71192b0bb7c14edfda9d564e5084db4473de68921234b5c4ee`. JAR bytes
were independently rehashed from the R05 paths:

| Runtime JAR | SHA-256 |
|---|---|
| `delta-spark_4.2_2.13-4.4.1.jar` | `2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be` |
| `delta-storage-4.4.1.jar` | `f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f` |
| `spark-core_2.13-4.2.0.jar` | `8e0a03b66338a4a38c9089d9f798ab6a6c6583aefe93f910311704a5ec90ffc4` |
| `spark-sql-api_2.13-4.2.0.jar` | `d789409a56db600c5160300fcd2c9651bf6f32dd8db1035a9bda17b67543b372` |
| `spark-sql_2.13-4.2.0.jar` | `ec0a0d699c6c57300ee47ba5d54b4d1f71d3e5401fb90a2bcdcd83b942084dbf` |

[`umf-validation.json`](umf-validation.json) records public UMF core 0.10.1
validation: the frozen 0.10.0 model validated complete with no diagnostics,
and UMF serialization round-tripped the exact 4,082 input bytes with the same
SHA-256. [`source-oracle.json`](source-oracle.json) fixes first-publication
singleton, edge, grouped-query and page expectations from the exact source and
binding hashes. The exact hash-pinned original Backend03 request uses
`COUNT(*) > 0` and returns the source-derived row `["person-1", 2, 1]`. A
separately labeled, effect-free Weft 0.4.53 compilation uses the same request
source and binding with only the threshold changed to `COUNT(*) > 1`; its
independently derived expected bag is the same row. Its retained response is
compiler-only, bound to `.3`, and must be freshly compiled against the actual
`.4` publication vector, frozen, and independently reviewed before SQL.

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
(`after=["person-2"]`). This cannot be frozen before publication because its
exact table UUID/version vector does not yet exist. Hash and preserve each new
request, binding, artifact, and response immediately after compilation and
before executing any SQL. An independent reviewer must verify each binding
against the complete vector. A missing or mismatched table UUID/version is a
refusal; never invent a vector or reuse a `.3` response as native evidence.

## Independent expected results

Expected values are frozen in [`source-oracle.json`](source-oracle.json) and
were derived only from the original model, source JSONL, and binding bytes,
without SQL output or native results. Use the current
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
   retain its JSON. It verifies all four input digests, all three original
   public compiler responses, the public UMF receipt, source oracle, R05
   runtime/result receipt hashes, and current local bytes for the five
   Spark/Delta JARs listed above. Before native execution, capture the approved
   process inventory and recheck the full runtime and JAR identities. A missing
   identity or competing process stops the run.
2. The public UMF 0.10.1 validation is already frozen in this packet. In a fresh
   private directory, initialize a fresh
   `ashlar-delta/0.4-xxhash64` installation with the configured-local runner.
3. Using one unchanged source/configuration/binding, publish batches 1, 2 and 3
   sequentially; repeat batch 3 byte-for-byte with a fresh invocation output.
   Retain full publication vectors at each ordinal and prove replay adds no
   table or manifest commit.
4. At the first publication, issue one native singleton read. Bind the original
   Backend03 and Backend02 requests, plus the separately labeled positive
   Backend03 request, to the complete first-publication vector and
   compile with public Weft 0.4.53. Freeze and hash each exact request, binding,
   artifact, and response; have the independent reviewer verify the complete
   vector pins before any SQL runs. Then execute the unchanged grouped query,
   Backend02 page 1, and continuation serially, comparing each result with the
   independent oracle above. Never execute SQL from an unfrozen compiler reply.
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

## Generated invocation packet

[`r02_packet_generator.py`](r02_packet_generator.py) creates one fresh
mode-0700 attempt directory under `/private/tmp/ashlar-r02-attempts`, stages
the exact pinned source/configuration/binding bytes and the two Delta JARs,
and builds a clean sparse UMF producer worktree at commit
`2ede4c879bea5db13794a6fa30e67c821306c17f`. The worktree contains the
committed `src/` and `spec/` closure used by UMF's public `src/index.ts`, plus
the Bun dependencies needed to resolve those imports. The generator checks
staged files against the Git tree and refuses staging above 384 MiB, leaving
at least 128 MiB under the 512 MiB limit for native output and logs. This is
an effect-free local preparation step; it does not invoke Bun, Spark, Java or
the configured-local CLI.

The attempt contains five invocation JSON templates in fixed order:
initialize, publish batch 1, publish batch 2, publish batch 3, and replay batch
3. The initialize invocation has the exact `null` installation digest. The
four later templates carry the explicit `@from-initialization@` marker because
the registry and its digest do not exist until initialization. The reviewed
manifest routes each step through
[`r02_invocation_worker.py`](r02_invocation_worker.py), which resolves and
retains each final invocation's exact bytes before calling `run-configured-local`.
After batch-3 replay, the worker checks the result manifest against all six
initialized target identities, requires exactly the four graph table roles
and their versions, then writes the vector candidate to
`publication-vector.json`. Continuation SQL and native reads remain unavailable
from this phase.

All absolute source/config/output paths are inside the attempt root. The
selected Python, JDK, Bun and Git executables are external runtime inputs and
are version/hash checked by the generator and supervisor. The R05 native
Python cohort contains Spark 4.2.0, Delta 4.4.1 and Weft 0.4.48;
prepublication does not invoke Weft. Public UMF core 0.10.1 validation and
Weft 0.4.53 compilation use their separately verified environments. The
packet does not claim a unified Python package cohort.

## One-shot supervisor gate

[`r02_supervisor.py`](r02_supervisor.py) accepts the generated JSON manifest
and a separate Sol review receipt whose SHA-256 names those exact manifest
bytes. It checks the exact current Git revision, four source hashes, seven
installed Ashlar runtime-source hashes, packet implementation hashes, all
five R05 JAR hashes, and the selected Python/JDK/UMF/Weft identities. It
adopts only the fresh mode-0700 user-owned attempt root named in that manifest
and runs its argv steps serially. It kills the process group on the 300-second
total deadline or a byte-cap breach, and leaves durable attempt state so a
failed attempt cannot be silently repeated. Command output is capped at 1 MiB
each, private attempt data at 512 MiB, and retained receipts at 16 MiB.

The prepublication phase allows exactly initialization, batches 1–3, and the
byte-identical batch-3 replay. It retains the complete final publication
vector and records its value and digest in an fsynced state file before exiting
as `complete-held-after-publication`. It cannot run native reads or queries.
A separate `continuation` manifest must include the actual complete
publication vector with exactly the `object_current`, `edge_current`,
`tombstone`, and `whole_source_history` roles, the hash of that state file, and
the exact frozen Weft request/response files for the original and positive
Backend03 requests and Backend02 page and continuation. Each binding must
equal the complete vector and each compiler response must preserve its binding
hash. Sol's review receipt must bind the exact continuation manifest hash and
approve those vector-bound artifacts. Only then can the separate invocation
run the singleton and four query steps.

The generator and supervisor require the exact Ashlar Git HEAD and selected
source hashes at manifest creation and execution; regenerate after any code or
source change. The generated manifest and invocation templates live inside
the private attempt root. No Sol review receipt exists, the actual `.4`
publication vector has not been created, and its fresh Weft responses
therefore cannot yet be frozen. These are the remaining execution blockers;
the supervisor has not been used to invoke Spark or native operations.
After Sol supplies the reviewed manifest and review receipt, use the exact
paths printed by the generator for a single phase invocation.

The inert phase/refusal tests and focused invocation/vector checks run with:

```sh
python3 -m unittest discover -s docs/helix/04-build/evidence/current-upstream-release-20261024/r02-native-execution-packet-20261024 -p 'test_r02_*.py' -v
```

The continuation invocation is unavailable until the first phase has
completed, the actual vector and freshly compiled artifacts exist, and their
exact manifest bytes have separate review. Each manifest must name the exact
current Git HEAD and all pinned source/runtime hashes.

## Focused preflight outcome and open evidence

The focused effect-free `preflight.py` completed successfully with status
`prepared_native_held`: all pinned source bytes, 13 events/3 batches, original
parity requests, all three Weft 0.4.53 target/response pairs, public UMF
validation, source oracle, and R05 runtime/JAR hashes matched. It does not
inspect current process state or establish R02 Spark/Delta execution. The
historical R02 runtime receipt
`current-native-query-and-sdk-pages-20261021.json` (SHA-256
`37dd8c330a5e65ad5395827e1bc839976e0a76f1e77a1726eae552edaa54cf1d`) records
private Weft and `.3` bindings and is not current native evidence.

Before execution, this packet still requires an approved current process
inventory and new `.4` publication-bound compiler responses after their vector
exists. This packet makes no Truss,
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
