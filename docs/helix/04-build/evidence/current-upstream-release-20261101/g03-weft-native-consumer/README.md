---
ddx:
  id: ashlar.evidence.g03-weft-native-consumer-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.prd
      kind: informed_by
    - id: FEAT-003
      kind: governed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: ashlar.evidence.g03-weft056-delta4-consumer-candidate-20261029
      kind: informed_by
---

# G03 Weft 0.4.56 native consumer qualification

Status: **r12 local native qualification complete**. This packet targets the completed
SDK 0.10.1 / Core 0.10.5 configured-local Delta `ashlar-delta/0.4-xxhash64`
installation and its independently retained R5 manifest/vector. It qualifies
only the bounded local source and consumer boundary described here.

The frozen source oracle is the reviewed G03 query packet from 2026-10-29.
`prepare.py prepare` admits the exact transaction-1 receipt, R5 completion
receipt, six-role inventory, four data-role versions, source/model/configuration
inputs, Weft 0.4.56 wheel and freshly compiled `.4` request/response artifacts.
It prepares the native attempt without opening Spark or executing SQL. The
native stage compares the direct singleton's exact JSON text, two grouped
Backend03 artifacts, Backend02 page one and the source-derived continuation
to the original query oracle. It also checks the retained manifest, each Delta
identity, schema, pinned version and opening/closing full six-role vector.

A single local Spark 4.2.0 / Delta 4.4.1 `local[1]` read interval is bounded
by a deadline and exclusive local installation locks. Temporary table aliases
exist only in the private attempt metastore. Publication data, source custody,
ACK and cloud state are not written. No Truss, managed Unity Catalog or
production-source authority is qualified. The exact installation, source
oracle, Weft wheel, requests, compiled responses and result hashes are retained
in the private attempt record; `native-result.json` is emitted only after
successful comparisons and cleanup.

The R5 manifest continuation completed with zero graph operations, no ACK and
no cloud activity. Its exact completion receipt is pinned in `prepare.py`.

Two one-shot dispatches failed before reaching SQL. The first failed because a
fresh Python process omitted the isolated checkout's `src/` path, so the
`ashlar_host.configured_local_runtime_profile` import failed. The corrected r6
dispatch then failed at PySpark gateway startup because `JAVA_HOME` was absent;
the exact `PySparkRuntimeError` and traceback are retained in
[`reconciliation-r6/native-failure.json`](reconciliation-r6/native-failure.json).
All three installation/journal locks were released. Read-only reconciliation
matched all six Delta log-head versions and UUIDs to the pinned installation;
the failures occurred before Spark session creation, Delta reads or SQL. See
[`reconciliation-r6/reconciliation.json`](reconciliation-r6/reconciliation.json).
Neither one-shot namespace is reused.

R7 passed the Java and worker preflight, then reached Spark/Delta and failed
when checking a temporary native alias. The guard compared
`Path(detail['location'])` directly, which does not normalize a Delta `file:`
URI. Existing local Ashlar code parses file URIs and percent encoding before
path comparison (`src/ashlar_host/medical_read_observation.py`). The r7 failure
record did not retain the `DESCRIBE DETAIL` UUID/location values, so available
evidence cannot determine whether UUID or location caused this mismatch. The
failure and read-only reconciliation are retained in
[`reconciliation-r7/`](reconciliation-r7/); all six Delta head hashes and UUIDs
match r6 and the pinned installation. Spark and local aliases were cleaned up,
and all three locks were released. No Weft user query ran and no Delta log was
mutated. The r7 namespace is not reused.

R8 reached the grouped query and failed with `TABLE_OR_VIEW_NOT_FOUND`. The
reconciliation identified a concrete adapter namespace mismatch: Weft's
binding and compiled SQL used `spark_catalog.ashlar_g03_b1d3d387fb77`, derived
from the realized publication ID, while the runner created and validated all
four aliases under `spark_catalog.ashlar_g03_38b126893d31`, derived from the
installation ID. The catalog and casing matched. `DESCRIBE DETAIL` therefore
validated a different database from the one queried. This is not evidence of a
Weft SQL defect or unsupported time travel; the SQL retains Delta's documented
`FROM table_name VERSION AS OF version` form ([Delta batch reads](https://docs.delta.io/delta-batch/)).
The minimal fix is in the Ashlar adapter: derive the alias database from the
publication ID already used by the Weft binding. The exact mismatch, cleanup,
and six unchanged Delta heads are recorded in
[`reconciliation-r8/`](reconciliation-r8/).

The r9 candidate applies that adapter fix and guards that compare compiled
grouped/page relation roles with prepared aliases before any native read. Its
generated compiler request/response bytes are identical to r8. It uses strict
`urlsplit`/`unquote` handling for local file URIs
and checks UUID separately. Any future alias mismatch retains actual/expected
UUIDs and raw/normalized/expected locations in the bounded failure record. Four
focused effect-free tests cover `file:/`, `file:///`, percent encoding, UUID
mismatch, path mismatch and refusal of remote/query-bearing locations. The r8
candidate also repeats the pinned Java and worker import preflight; its exact
requests/responses, prepared vector, source binding and hashes are in
[`candidate-r9/`](candidate-r9/) for `/private/tmp/ashlar-g03-weft-native-attempt-r9-20261101`.

R9 then completed the native singleton, both grouped-query oracle comparisons,
and the first two Backend02 page-row comparisons. It failed at the lookahead
guard because the harness compared the lookahead's scalar first column with
the complete source-oracle row. The bounded failure and read-only six-role
reconciliation are in [`reconciliation-r9/`](reconciliation-r9/); all four
aliases, the temporary database, Spark, and all three locks were cleaned up,
and all six table UUIDs, versions, and log-head hashes match r8. The r10
effect-free fix compares the complete lookahead row and requires exactly one
lookahead row, while preserving exact page and continuation comparisons. A
focused regression test covers matching and mismatching rows, missing or extra
lookahead rows, and continuation-key drift. The remaining assertions and
result assembly were audited; continuation rows still compare to the full
source-oracle page-two list before the result is assembled. The fresh r10
namespace is `/private/tmp/ashlar-g03-weft-native-attempt-r10-20261101`; the
effect-free prepared packet, compiler artifacts, source binding, Java/worker
preflight, and test snapshot are retained in [`candidate-r10/`](candidate-r10/)
and bind to harness commit `f4bab9360f165f0753287e741e9b96373d758660`.

Two effect-free exception-recorder tests also pass for module-name retention
and the 16 KiB traceback bound. On a future native failure, cleanup runs first
and a bounded `native-failure.json` retains the exception while withholding the
success result; a failure record blocks namespace reuse. The r10 candidate
passed pinned Java/worker preflight and thirteen focused effect-free tests and
was dispatched once after Sol approval. Its first invocation was refused by
the effect-free gate because `PATH` did not select the pinned Java executable;
no native effects occurred. The corrected one-shot invocation selected pinned
Java, then failed before Spark session creation because the Java gateway could
not bind `127.0.0.1:0` (`Operation not permitted`). The result was withheld,
all three locks were released, and all six Delta UUIDs, versions and log-head
hashes remain unchanged. The raw failure, stderr, gate refusal and
reconciliation are in [`reconciliation-r10/`](reconciliation-r10/); the
updated candidate is in [`candidate-r10/`](candidate-r10/). A separate
effect-free loopback probe confirmed this is the default sandbox restriction;
the same bind succeeds under approved escalation. R11 is prepared in a fresh
namespace for one parent-run attempt with escalation. It retains the same
read-only checks, pinned runtime/source bindings, lookahead regression gate,
and Java/worker preflight. R11 artifacts are in [`candidate-r11/`](candidate-r11/).
No consumer qualification is claimed.

R11's escalated native run passed the singleton, both grouped queries, page,
and continuation comparisons. It then withheld the result because the closing
comparison mistakenly expected `database` and `applicationId` in the six-role
closing map, although those fields are kept in the opening Spark identity
record. All cleanup operations succeeded and all six Delta UUIDs, versions,
and log-head hashes remain unchanged; see
[`reconciliation-r11/`](reconciliation-r11/). R12 compares exactly the six
role-head entries and retains database/application checks as a separate guard.
Result assembly now emits only role heads in the `openingSixRoleHeads` and
`closingSixRoleHeads` fields, with database and application ID under `runtime`.
A focused regression test covers unchanged heads, a changed role head,
unexpected runtime metadata in the role map, and database/application drift.

R12 then completed one bounded local Spark 4.2.0 / Delta 4.4.1 read-only
native run against the SDK 0.10.1 / Core 0.10.5 `.4-xxhash64` installation.
The singleton exact JSON/identity check, both grouped query oracle checks,
Backend02 page rows and one-row lookahead, source-derived continuation and
full page-two check all passed. Opening and closing six-role UUID/version/log
heads matched. The attempt cleaned all four aliases, its temporary database,
Spark and three locks; publication mutation, ACK and cloud activity are false.
The exact full result and concise digest summary are retained in
[`candidate-r12/`](candidate-r12/). This qualifies only this local runtime,
installation, source, model/configuration, Weft version and observed query
subset. It does not qualify Truss, managed Unity Catalog, protected ACK, or
production source authority.
