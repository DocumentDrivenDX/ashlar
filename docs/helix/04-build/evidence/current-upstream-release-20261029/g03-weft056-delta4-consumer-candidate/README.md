---
ddx:
  id: ashlar.evidence.g03-weft056-delta4-consumer-candidate-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.release-closure-20261029
      kind: informed_by
    - id: ashlar.prd
      kind: informed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: TD-009
      kind: governed_by
---

# Gate 3 Weft 0.4.56 / Delta 0.4 candidate

Status: **inert candidate; no `.4` publication, query SQL, or native result is
claimed**. This packet fixes the smallest graph and oracle to use for the Gate 3
run. Sol reviewed the fake-transport coordinator. It has not been prepared for
native execution and carries no execution permit.

## Frozen inputs and exact oracle

`inputs/` is copied byte-for-byte from the reviewed configured-local `.4`
initialization packet. The source contains three ordered transactions. The
candidate publication is exactly source transaction `typed-graph-1` (eight
events, described in `source-oracle.json`); transactions 2 and 3 are excluded.
The selected model is UMF 0.10.0, model revision 1, with the same typed graph
binding and configured-local source profile. Input digests and the oracle's
expected bags are in `source-oracle.json`.

The first transaction publishes four objects and four directed edges. It
includes an isolated `Person(person-3)` with a Unicode name, one person without
the optional quantity property, and two separately identified parallel
`knows` edges from `person-1` to `person-2`. The grouped oracle is exactly
`[["person-1",2,1]]`; the SDK page size is two, with rows
`[["person-1","Ada"],["person-2","Ben"]]`, continuation after
`["person-2"]`, and page two `[["person-3","雪"]]`. The direct singleton
expectation for `person-1` preserves the integer token `9007199254740993` only
as quoted exact JSON text, without a parsed JSON-number duplicate, together
with the original unknown retained JSON text.

`publication-vector.json` is an immutable null template and must remain byte
unchanged. After a fresh publication exists, a future worker must create a
separate, exclusive `realized-publication-vector.json` output from that
publication's retained receipt and independently observed Delta identities.
The realized file must include its canonical SHA-256; every compiler target and
query must bind that digest. It must not synthesize a vector from table heads or
borrow the `.3` R02 vector. Because there is no existing `.4` publication, this
candidate packet cannot truthfully supply concrete UUID/version values before
native publication.

## Original compiler artifacts and native scope

`query-scope.json` defines five held reads under the same complete vector:
the native singleton, original Backend03 grouped query, positive
Backend03 `HAVING COUNT(*) > 1` multiplicity query, Backend02 first page, and
Backend02 continuation. These are separate original Weft compiler boundaries.
Weft owns compilation and emitted SQL; the Ashlar adapter must execute original
artifacts unchanged. The `.4` Backend02 and Backend03 requests must be freshly
bound to the actual publication and compiled with the pinned Weft 0.4.56 APIs.

R01 records that Weft 0.4.56 compiled the original `.3` Backend03 request and
Backend02 page/continuation requests to the same compiler responses as .53.
Those exact `.4`-bound request/response bytes are not in the checkout. The
retained `positive-backend03-request-ashlar-delta-0.3-compiler-only.json` is an
explicitly labeled `.3` compiler-only candidate, not an admissible `.4` query
artifact. Do not copy its binding or response into a run manifest.

## Proposed one-shot envelope

The later execution packet should use one private local Spark 4.2.0 / Delta
4.4.1 process and Weft 0.4.56 on the verified macOS arm64 wheel selected by
R01. Proposed limits: 450 seconds total wall time, 512 MiB private attempt
storage, 16 MiB retained evidence, 1 MiB combined process output, 1 MiB per
compiler/result artifact, at most 10,000 native rows per result, and 65,536
cells per result. Compile and review the `.4`-bound artifacts before the first
query statement. Use a two-stage review gate: singleton + both Backend03 cases
+ page one; then derive the continuation from the returned page-one token,
freeze/review it, and execute page two. The second stage must bind the same
vector and runtime identity as stage one. Do not run until Sol approves the
exact packet digest and a fresh one-shot permit binds the actual prepared
manifest, vector, launch descriptor and empty process-slot receipt.

Refuse before SQL on any missing source/model/config/binding digest, absent or
ambiguous transaction-1 publication, malformed/incomplete four-role vector,
vector drift, any `.3`/`.50`/`.53` compiler bytes, unrecognized Weft wheel or
index, compiler status other than `compiled`, binding/response mismatch,
rewritten SQL, failed source-oracle comparison, duplicate or missing identity,
wrong multiplicity, wrong page boundary/token, resource/deadline exhaustion,
publication mutation, or failed opening/closing identity checks. Preserve
ambiguous native effects for reconciliation; never retry a dispatched attempt.
No protected ACK, cloud/Unity Catalog authority or Truss source claim belongs
to this gate.

## Missing executable material

The following remain prerequisites to a Sol-reviewable execution packet and
are not represented as completed work here:

1. A reviewed one-shot native initializer/publisher that applies only
   `typed-graph-1` under `ashlar-delta/0.4-xxhash64`, creates one immutable
   publication, and retains the native four-role vector plus independent
   six-role inventory. `configured_local_delta4_publication.py` now supplies a
   pure transaction-1 source plan, role-qualified hash input validation and
   vector shape binder. It also has a bounded native Spark hash-observation
   adapter for a journal-pinned six-role installation; that adapter has only
   inert fake-transport tests so far. The module starts no Spark session,
   writes no tables, and cannot mint a complete manifest. A separate frozen
   transaction-1 intent now emits three parameterized `.4` INSERT proposals
   bound to the request and installation-profile digests; it also performs no
   native writes. Read-only helpers admit a fresh, empty six-role installation
   and compare all four data-role bags at exact post-effect versions, including
   native `xxhash64` checks. The owner-held coordinator now binds the request,
   source custody, whole effect plan and writer lock; it grants only the three
   exact original operation IDs, supports recovery from a verified committed
   prefix, quarantines submitted/uncertain states, and checks complete bags,
   receipts and closing heads. Sol reviewed the fake-transport gate; the
   focused `.4`/`.3` suite passed 36 tests. These checks do not exercise Spark
   or native Delta effects. No `.4` transaction-1 effect or native snapshot
   comparison has run, and no publication or consumer success is claimed.
   A pre-effect prerequisite now retains one byte-exact source-scope record in
   the local journal under the held owner; focused inert tests cover exact
   replay and changed/missing/corrupt custody. During coordinated execution,
   the temporary policy scope admits only the three original operation IDs.
   A read-only observer can now reconcile each contiguous committed prefix of
   the three frozen effects against original journal rows, native commit
   metadata, exact receipts and six role snapshots under the held local owner.
   Recovery-only baseline rehydration requires the original journal record,
   version-1 installation history and six empty historical snapshots before a
   new local owner can reconcile a committed prefix. The held owner retains the
   exact whole effect plan through `LocalDeltaEffects.prepare`, then executes or
   recovers it only inside the coordinator's temporary per-operation policy
   scope. Fake-transport checks cover clean completion, committed-prefix
   recovery, pre-dispatch recovery, post-commit mismatch and submitted-state
   quarantine. Native execution, immutable manifest commit/readback and the
   realized publication vector remain unproven.
   A separately named local-journal output-completion receipt now binds the
   exact source scope, effect plan and receipts, four-role versions, six-role
   inventory and complete observed bags; fake tests cover exact replay and
   tamper refusal without adding plan/operation rows. It is supplemental
   custody, not a native `applied` attempt phase. A separate versioned local
   attempt journal now binds exact source/request/checkpoint/graph-plan custody
   and persists `prepared` then `applying` before graph dispatch; direct calls to
   the graph executor refuse without that held-owner admission. This remains a
   local prerequisite, not the native Delta attempt-table protocol. A separate
   local metadata-lane reservation binds the exact prepared phase MERGE to its
   source, graph plan, target UUID/path and predecessor. A separate bounded
   metadata-operation journal now records `submitted` before dispatch and
   `committed` only after the exact named commit, version and full phase-row
   readback validate. Fake-transport tests cover restart/reopen, reservation
   tampering, altered phase inventory and submitted-without-commit quarantine.
   `DeltaAttemptStore` accepts the configured immutable JSONL scope with its
   original checkpoint validation intact. The live `PrivatePolicy` has no
   metadata-lane admission branch, and graph close still requires fresh
   metadata heads; this executor is not connected to the `.4` coordinator and
   has no native route. Native phase integration, manifest and consumer phases
   remain pending. See [the phase and receipt
   boundary finding](output-completion-receipt-boundary-20261029.md).
2. An independent `.4` vector reader and native source-oracle comparison that
   reopens those exact Delta versions and proves complete object/edge bags.
3. A Weft 0.4.56 host distribution adapter for the local configured runtime.
   The checked configured query adapter and R02 live-alias gate are pinned to
   Weft .53; R01's .56 evidence is compiler-only.
4. Exact original `.4` Backend03 original/positive and Backend02 page/continuation
   request/response artifacts compiled after the actual vector exists, plus
   original typed singleton request/artifact and result descriptors.
5. A packet-specific Sol receipt and exact execution permit after these files,
   the vector, runtime and launch descriptor are frozen and reviewed.

The prior `.4` native receipt proves initialization only. The successful `.3`
R02 receipt proves only its `.3`/Weft .53 publication and reads. Neither is
substituted for these missing `.4` facts.
