---
ddx:
  id: TD-009
  type: technical-design
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: US-004
    kind: informed_by
  - id: ashlar.prd
    kind: informed_by
  - id: ADR-001
    kind: informed_by
  - id: CONTRACT-001
    kind: informed_by
  - id: CONTRACT-002
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
  - id: CONTRACT-004
    kind: informed_by
  - id: TD-006
    kind: informed_by
---

# Proposed managed Spark receipt verification amendment

## Scope and authority

This draft proposes a smaller compute arrangement for the production-target
A1 pilot and R04 continuation in `toolkit-delivery-plan.md`. It is not an
approved replacement of that plan, execution authorization, an available-runtime
claim, a price quote, a budget guarantee or production qualification. No compute
has been launched for this design. It realizes [[US-004]], especially
US-004-AC1, US-004-AC5, US-004-AC6 and US-004-AC8, under the existing publication,
consumer, physical-table and resolver contracts. There is no separate parent
solution-design instance; ADR-001 and those contracts govern this bounded change.

The existing `ManagedCurrentSQLReceiptReader` requires an actual dedicated
serverless, current-channel SQL warehouse. A fixed classic SQL warehouse cost
sketch cannot price that implementation. Keep its existing profile and admission
unchanged. The proposed Spark path requires a separately versioned read profile,
contract amendment and focused implementation before any pilot. It cannot be
selected by passing a Spark transport to the SQL reader or by weakening its
runtime check. The current eight-serverless-task-plus-SQL plan remains the
recorded baseline until this amendment is reviewed and adopted.

## Technical approach

Propose one explicitly dedicated, single-node Jobs cluster for a bounded pilot
run: a publisher task followed by a distinct verifier task/process with its own
Spark session on the same cluster. Sharing compute reduces a second endpoint's
startup and billing surface; it does not establish process isolation, authority
or independence. Confirm that the selected cloud/workspace policy, UC access
mode and exact runtime support this arrangement before launch. If they do not,
stop this candidate; do not silently introduce a platform matrix or new endpoint.

The verifier must read durable UC state afresh through its own native owner. It
must not reuse publisher DataFrames, cached temporary views, mutable Python
objects, a publisher-owned resolver or publisher assertions as proof. Clear or
avoid session caches and qualify this behavior with an adversarial stale-state
check. Publisher output is an untrusted comparison input. The independent oracle
comes from frozen accepted source/request evidence, not observed destination
rows or the publisher's expected-state calculation. A process split is not an
independent-principal or adversarial security fence; record the actual principal
and effective grants and the resulting development-authority limitation.

One slot remains one source transaction. After the publisher and verifier both
terminate successfully and closing admission finishes, the external Truss source
holder independently admits the durable receipt and current source/checkpoint
custody, submits the protected destination receipt ACK, commits it and reads back
its exact correspondence. Neither Jobs task receives the source ACK credential
or moves a source cursor. A downloaded verifier report is evidence, not a durable
pin holder: the source holder still needs a reviewed live native read/pin renewal
port through its ACK interval, including closing. Designing that remote renewal
handoff is a prerequisite, not a permission inferred from a verifier success.

For later R04 slots keep A1, B1, A2, B2, A3, B3, A4, B4. No next slot may begin
source mutation until the preceding slot's publication, independent verification
and source-specific protected ACK finish. One cluster may be reused only while
its admitted wall/cost envelope and custody remain valid; idle time while the
external holder ACKs counts. Do not combine A/B publication before A's ACK.
Before A1 retain actual independent accepted A/B installations, feed/epoch,
namespace, consumer and destination ACK registrations, the complete frozen
semantic schedule and oracle. Bind each exact original slot request from verified
predecessors before that slot's mutation; bind actual feed-page bytes at its
ACK-gated step as already recorded in R04. Preparation does not supply those facts.

## Invariants and proposed acceptance criteria

The following are review gates for the new path, not newly granted authority:

| Gate | Required observable result |
| --- | --- |
| Installation | Opening, renewal and independent closing observe exactly six managed roles: `object_current`, `edge_current`, `tombstone`, `whole_source_history`, `publication_attempt_phase`, `publication_manifest`. Check three-part names, six distinct UUIDs, owners, complete ordered columns/types/nullability, admitted Delta protocol/features and actual heads against retained publisher inventory. No omitted attempt or manifest role. |
| Publication | Independently reopen exactly one immutable original manifest and committed attempt custody. Compare exact original request and manifest bytes/digests, schema revisions, publication identity, operation/effect receipts and source binding. Resolve all four distinct data-role UUID/version pins and compare the complete destination receipt and compiler-binding inputs; retain the separate six-role installation inventory. A counts-only or current-head reconstruction cannot pass. |
| Exact data | At the manifest's explicit native literal versions in the selected three-part namespace, compare complete bounded four-role row bags to the independent source oracle, preserving source-qualified identities, absence versus null, integer/decimal tokens, exact payload/history bytes and delivery digests, tombstones and parallel-edge multiplicity. Reject over-budget reads rather than truncating. A catalog-only page still advances source progress through a manifest. |
| Both sources | Preserve the complete progress vector including unchanged entries. A and B overlapping local IDs stay distinct; each source retains its own epoch, reader, destination receipt and ACK scope. Compare exact feed-page inventory and checkpoint to the frozen slot request. |
| Runtime and close | Observe actual job/run/task, cluster identity, runtime/Spark/Delta build, carrier/model/Truss versions, principal, namespace and protocol at opening and closing. Fail closed on replacement, runtime/principal drift, unavailable pinned versions or lost exclusion. Attempt closing for every acquired interval after body failure, cancellation and deadline, preserving the primary error. Release no success/ACK authorization on closing failure. |
| ACK | External holder uses complete exact destination receipt bytes and full vector under real retained read/pin/source authority; commit and publicly reconcile protected ACK. Missing, conflicting or unknown outcomes stop the schedule and retain original recovery custody. No caller Boolean, verifier JSON or task success substitutes for this authority. |
| Replay/recovery | Exact committed replay and fresh-process journal restart reuse original intent/operation/receipt without new graph or manifest commits. Uncertain submission and missing original journal refuse; preserve retained older publication reads and breaking-evolution refusal. |

The A1 pilot proves only its selected transaction and exact runtime/source subset.
It does not complete R04, qualify two-source lifecycle before B evidence, or prove
1B/5B capacity. On admitted A2, execute the selected unchanged Weft grouped,
negative-HAVING, first-page and continuation cases once against their independent
oracle under CONTRACT-002; do not repeat that matrix on every slot. Weft owns
compiler SQL; the new adapter cannot repair it.

## Component changes and smallest Luna slice

First review and adopt the proposed transport/profile and source-holder renewal
boundary in CONTRACT-001/003/004 (and CONTRACT-002 only for changed consumer
behavior), then update the toolkit campaign plan. Exact public signatures and
profile fields belong in those contracts, not this draft. No physical carrier
migration or replacement schema interpreter is proposed; public UMF owns DDL.

The smallest implementation slice is an offline, read-only Spark receipt owner
in a new `src/ashlar_host/managed_current_spark_receipt_read.py` with focused
`tests/test_managed_current_spark_receipt_read.py`. Reuse portable request,
manifest, resolver, source-receipt codec and carrier validation. Keep Spark/SDK
integration in `ashlar_host`; do not introduce those imports into `ashlar` or
import the publisher as a verifier. Keep SQL runtime admission separate; extract
shared pure validation only where its existing semantics remain explicit.

Spark-native values require explicit lossless normalization at the read boundary:
versions/ordinals remain non-Boolean signed64 integers, native arrays/maps retain
complete structure, timestamps use the original exact microsecond representation,
and column type names map to the admitted UMF physical types. Do not reuse SQL
string-only parsers or stringify arbitrary Spark values. Focused tests cover SQL
and Spark representation differences, including overflow and timestamp precision.

That slice accepts explicit injected session/configuration and retained original
inputs, independently reopens one receipt with bounded read-only statements,
constructs the full vector and exact source receipt, and performs substantive
closing. It does not provision a cluster, submit Jobs, mutate data, ACK or claim
that an external source holder has a usable live pin. Tests demonstrate exact
parity with a trusted frozen fixture and refusal for each changed UUID/head,
protocol/schema/owner/principal, manifest/request/vector/progress mismatch,
missing journal, truncation and opening/body/closing failure. Keep the path
unavailable to the campaign until the live ACK renewal boundary is implemented.

A following separately reviewed slice wires publisher then verifier tasks and the
external holder through existing campaign/trial orchestration, with no automatic
retry or replacement operation on uncertainty. Run the repository boundary
checker and focused existing receipt/source/resolver tests. Diagnostic records
follow CONTRACT-006: bounded references and redacted digests, never credentials
or payload dumps; telemetry cannot establish commit. Formal review maps the
publication-before-ACK and ACK-before-next-slot transitions to these tests;
no new model-check result is claimed by this draft.

## Cost admission, rollback and open questions

Prefer total spend below $10; retain the hard $100 total trial ceiling. Neither
single-node selection, a timeout nor a cost estimate enforces an invoice cap.
Before launch record the actual admissible node SKU/region, runtime/access mode,
Jobs DBU and VM rates, disk/storage/network meters, startup and idle billing,
two task maxima, finite caller wall/cancellation deadline, concurrency one,
zero automatic retries, cluster termination and owned-schema cleanup reserve.
Estimate the entire pilot and already incurred campaign spend conservatively,
including a live ACK read interval; if a dependable bound is unavailable or
cannot cover the ceiling, do not launch. The old serverless $0.47/DBU observation
and assumed 120 DBU/hour cannot price this proposed cluster. No new numeric
estimate is asserted here.

After A1, retain terminal receipts, actual elapsed times and fresh usage where
available; conservatively cover delayed billing and seven remaining slots before
continuation. Review the envelope before each slot and stop before consuming the
cleanup reserve. Keep predictive optimization enabled. Retain evidence before
owned cleanup and verify absence; cleanup failures remain explicit. On a failed
pilot terminate owned compute and retain original request/journal/receipt custody
for reconciliation; do not erase ambiguous effects or claim a rollback occurred.
Selecting the existing SQL profile remains possible under its own separately
admitted endpoint and budget, not an automatic fallback.

Open prerequisites are the exact UC-capable single-node runtime/access mode and
rates, genuine verifier session separation, an external live read/pin renewal
mechanism through protected ACK, explicit exclusion against other writers,
selected source acceptance/feed custody, and the frozen schedule/oracle. Review
must resolve these before marking the campaign implementable or launch-ready.

### Reduced offline preparation slice

An earlier partial slice MAY use the distinct internal
`ashlar-managed-current-spark-receipt-reopen/0.1` identity solely to exercise
read-only inventory, manifest correspondence, metadata resolution, native
normalization and closing against offline fixtures. It MUST return an explicitly
non-authoritative observation without source ACK ports or an ACK-capable backend.
It is not the complete proposed verification profile above. Complete oracle bags,
committed attempt/effect checks, job/run/task and Delta-build identity, retained-file
and source-authority proof, and external live renewal remain unimplemented gates.
Tests for this preparation slice MUST NOT be reported as those gates passing or
make the path reachable from campaign ACK orchestration.

### Next offline slice: complete bags and committed custody

Keep the internal reopen identity and no-ACK observation. Add complete four-role
bag verification and committed phase/result/plan correspondence for one bounded
frozen source prefix; this does not add live retention or ACK authority. Inputs
must retain the full independently source-derived prefix, its source-bundle and
schedule/request anchors, and original per-publication clocks. Native destination
rows and publisher `expectedRows` cannot supply expected bags.

Existing reusable seams are `truss_note_publication.graph_expected_rows`
(independent source-image projection), `TrussGraphAdmission.changes` and original
event bytes, and `managed_current_query._managed_source_identity` (digest-bound
current bundle/schedule/version identity). The latter's Person/Place query oracle
is not a complete four-role oracle. Likewise `managed_source_oracle`'s configured
JSONL oracle is not a Truss current-core substitute. Reuse the source-image
projection through an explicitly retained source-prefix owner, starting at empty
state or independently retained prior source oracle, never native prior rows.
Freeze expected bags before verifier native reads; retain exact strings and
multiplicity, with explicit integer/timestamp normalization on actual captures.
The `.4` hash field needs its own admitted native typed-key expectation; do not
invent a Python hash equivalent or derive expected values from destination rows.

For attempt custody reuse `attempt_store.verify_request_digest` and the existing
phase validation semantics: complete prepared/applying/applied/committing/committed
prefix, exact payload digest and unchanged request/plan/result strings. The
writer-held `DeltaAttemptStore.session` MUST NOT be opened with a fake writer
policy just to read. Extract a pure source-qualified phase-inventory validator
from its existing read semantics if needed, preserving all callers. Read complete
bounded stream/batch rows before selecting feed/epoch; malformed foreign scope
must refuse. Reopen at the captured attempt-table head with UUID/protocol bookends.
Use the retained incremental plan/effect correspondence in
`ManagedCurrentIncrementalPublicationDriver._validate_predecessor_attempt` as the
existing semantic checklist, not as a verifier import of the publisher. Pure
shared custody validation may be extracted; neither affected `expectedRows` nor
baseline-plus-statement arithmetic alone proves native effects. Bind the exact
committed manifest/result/plan to the source request, four-vector, six inventory,
source admission, original plan digest and effect-row digest, then independently
compare the complete native bags. This slice proves retained artifact and final
state correspondence only; native operation-handle terminal recovery remains a
separate gate.

Implementation/test order:

1. Add a pure bounded prefix-oracle carrier/admission helper and fixture anchored
   to original Truss bytes, bundle digest, schedule step and request digest. Test
   two-source overlapping IDs, exact numeric/presence values, parallel edges,
   updates/deletes and original clocks. Do not add source mutation or provisioning.
2. Add a reusable pure source-qualified phase validator plus read-only committed
   artifact admission. Test missing/duplicate phases, changed request/plan/result,
   digest mismatch, foreign malformed scope, uncommitted state and vector mismatch.
3. Extend the Spark owner's exact statement allowlist with only explicit-role,
   literal-version full reads and the bounded phase lookup. Compare complete bags
   as multisets with whole-capture bounds, renew native bookends and substantive
   closing, and keep the observation non-authoritative with no backend/ACK API.
   Test missing/extra/duplicate rows and raw-history changes that preserve counts,
   overflow, native value normalization and closing refusal. Focus verification on
   these tests plus unchanged receipt/attempt-store callers.
4. Retain a small build-evidence packet with exact fixture/code/profile/digest
   anchors and focused commands/results, labeled offline. No live UC pass follows.

Retention/read pin seams remain `PinVector`, `PostgresPins.hold` (native assert-active guard),
`ManagedPinReader.transaction`, `validate_publication_retention` and the existing native
snapshot policy; metadata LIMIT 0 and a PinVector value alone prove no retained
files or held authority. Their real integration and independently observed runtime
job/task/build identity remain required before full-profile or campaign use.

### Bounded committed-attempt reopen increment

The next smaller increment implements only native retained phase/result/manifest
correspondence, before full source-oracle or effect validation. Keep the internal
reopen profile, no ACK ports and no publisher imports. After request digest and
native installation admission, query the captured attempt-role UUID/head using
this exact parameter-bound statement shape (identifier and nonnegative signed64
version come only from the admitted inventory):

```sql
SELECT phase,request_digest,payload_json,payload_digest
FROM `catalog`.`schema`.`publication_attempt_phase` VERSION AS OF VERSION
WHERE stream=:stream AND batch_id=:batch
ORDER BY CASE phase WHEN 'prepared' THEN 0 WHEN 'applying' THEN 1
WHEN 'applied' THEN 2 WHEN 'committing' THEN 3 WHEN 'committed' THEN 4 END
LIMIT 161
```

Observe UUID/protocol before and after the read, compare both to the admitted
attempt identity, and refuse an over-limit result before scope selection. The
executor must actually capture the 161st sentinel even when its normal row budget
is lower: either require at least 161 rows for this owner or reserve a dedicated
161-row capture cap. Enforce whole-result byte and cumulative statement/deadline
bounds; no truncation. Validate all rows, including foreign scopes sharing local
stream/batch IDs, through `validate_source_qualified_phase_inventory`. Require
exactly five selected phases ending committed. The original request strings must
equal the caller's verified request, including exact checkpoint, source batch and
schema inventory; do not compare only its digest. Existing phase validation owns
payload digests, phase order and unchanged plan/result custody.

Decode retained result/descriptor with duplicate-member/nonfinite refusal. Require
the result's closed `{effects,manifest}` carrier, object-valued effects and exact
manifest equality to both the independently reopened native manifest and retained
committed descriptor. Retain hashes of original committed payload, plan if present,
result and descriptor text as bounded correspondence evidence. A plan may be absent
under the existing phase format; do not require or infer an incremental plan
profile here. Its contents and `effects` are opaque retained artifacts in this
increment, not proof of native effects, statement execution, source correctness or
retention. Do not widen observation capabilities or release an ACK backend.

Focused tests use explicitly authored offline phase fixtures, never relabeled as
accepted-source evidence: valid selected and overlapping foreign scopes; absent
or noncommitted prefix; duplicate/missing/unknown phase; digest mismatch; changed
request/checkpoint, plan or result; descriptor/result/native-manifest mismatch;
malformed foreign scope; 161-row refusal and byte overflow; UUID/protocol drift
around the attempt read; and timeout/closing failure. Run changed Spark and scoped
phase tests plus unchanged SQL receipt tests. Complete oracle bags, semantic
plan/effect proof, native terminal handles and live source/read pins remain gates.
