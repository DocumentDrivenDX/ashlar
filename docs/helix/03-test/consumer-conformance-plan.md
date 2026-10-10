---
ddx:
  id: TP-001
  type: test-plan
  activity: test
  status: draft
  authoring:
    home: repo
  links:
  - id: CONTRACT-001
    kind: informed_by
  - id: CONTRACT-002
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
---

# Consumer conformance test plan

## Testing Strategy

Prove deterministic publication and bounded authorized reads using
[consumer-conformance.json](fixtures/consumer-conformance.json). Expected state
is authored fixture data, not derived from a publisher implementation. A
criterion is covered only when an implementation actually runs it with cited
acceptance IDs and evidence. This plan scopes consumer, domain-model and engine
conformance; implementation status belongs in build evidence.

| Level | Coverage target | Gate |
| --- | --- | --- |
| Fixture validation | Unique case IDs, resolvable identities, coherent expectations and acceptance references | Before handing off corpus |
| Contract | Every publication/read case; exact logical state and outcome | Before boundary conformance claim |
| Platform integration | Real table publication failure/recovery, policy delegation and progress visibility | Before Databricks support claim |
| Query feasibility | Workload, plans, runtime and measurements for US-003-AC5 | Before performance acceptance |

Run portable Python checks with the standard-library unittest runner and native
checks on explicitly pinned runtime profiles. Schema/corpus validation is distinct
from behavioral execution. A passing local simulator
cannot establish Databricks transaction or access-control guarantees.

## Test Data

The synthetic profile uses opaque ASCII node/edge identifiers, revision r1,
feed S/epoch e and integer positions. Graph fixture contains isolated A0,
parallel A1→B1 edges, B1→C1/C2 and a B1→A1 cycle. History fixture contains
create v1, replace v2 and delete v3. A second source tests authority and progress.

`publicationCases` initialize a complete state and apply actions in order.
`expected.state` is the final logical state; compare current/history/tombstone
collections by identity and version, and progress by feed/epoch. Do not compare
wall-clock times or generated publication IDs for replay equivalence. Inspect
intermediate visible state when an action injects failure; later recovery must
not hide an earlier incomplete visibility failure.

Plain batch actions invoke CONTRACT-001. `applyWithFailure` is a harness action
injecting failure at beforeCommit, then inspecting visible state. `bootstrap`
is a harness attempt to replace current state with an older snapshot without
reviewed reconciliation; it must return RECOVERY_REQUIRED. These actions are
not production feed messages. Outcomes for each action must be captured even
when the fixture asserts only the final outcome/state.

`readCases` reference a named publication and simulate the stated policy/runtime
conditions before executing the request. Response publication is its ID, not the
full state. Unspecified setup defaults: operations permitted, no hidden data,
delegation supported, store available, no continuation support, all history
retained. A setup override MUST NOT silently mutate the fixture publication.
`earliestAfter` sets the retained-history lower bound. `cursorContext` injects
an earlier cursor binding. `hiddenIdentities` suppresses incident edges and
`hiddenProperties` forbids filters/counts involving that property.

## Coverage Requirements

100% of fixture cases must pass with no unexplained differences. Every P0 and other in-scope
criterion needs a covering executable test with `@covers US-nnn-ACm` and recorded
pass/fail evidence. Fixture metadata `covers` records planned coverage only.

| Acceptance class | Fixture cases | Primary layer | Remaining proof |
| --- | --- | --- | --- |
| US-004-AC1–AC5/AC7/AC8 | PUB-* | Contract | Native adapter, atomic visibility, freshness instrumentation |
| US-004-AC6 | READ-history/changes/expired | Contract | Actual retention and historical policy |
| US-003-AC1–AC4 | READ-two-hop/isolated/parallel/cycle | Contract | Pinned physical query execution |
| US-003-AC5 | No fixture substitute | Platform feasibility | Target/runtime, corpus workload and query evidence |
| US-003-AC6–AC10 | Remaining READ-* | Contract | Full policy enforcement and safe continuation integration |

The existing story requirements remain authoritative. These cases exercise
specific examples; they do not exhaust each criterion. Add cases for publication
revision recovery, position conflict, node-plus-edge atomic deletion, missing/null
count groups, valid continuation across unchanged context, expired publication,
epoch mismatch, source-time measurement and protection in historical reads before
claiming complete boundary coverage. Story test plans own the per-criterion
executable case matrix and must reference the selected execution interfaces and
platform mechanics.

## Implementation Order

1. Review the proposed identity, replacement, conflict, retention and disclosure
   policies with the consumer/producer; revise Contracts and fixtures together.
2. Specify an adapter-independent execution harness and story test plans.
3. Exercise publication and read boundaries against an independent implementation.
4. Select and prove physical publication/read mechanisms on a pinned target.
5. Bind pinned supported UMF capabilities, with explicit loss and preservation tests.
6. Load small domain-pack publications and verify Weft queries and engine parity.
   Source/engine availability affects only the relevant lane.

## Infrastructure

No platform resources are required to review the corpus. Native policy and
Databricks evidence require a synthetic sandbox, a pinned target, authorized test
principals and failure injection. No production data is authorized by this plan.

## Risks

| Risk | Consequence | Mitigation |
| --- | --- | --- |
| Whole-entity adapter conflicts with producer property events | Lost transactional/version meaning | Require native-to-boundary mapping and batch proof |
| Simulator policy mistaken for user delegation | Unauthorized access | Separate platform integration gate |
| Expected data follows implementation assumptions | Correlated mistakes | Review corpus independently with producer/consumer |
| Retention allows stale resurrection | Incorrect state | Retain tombstones until reconciled expiry is proven |

## Domain-pack and query conformance

Retain original pinned UMF model/data bytes, hashes and notices. Test typed
records, relationships, exact decimals/integers, absence/null/value states and
unknown extensions through shared UMF interpretation and validation; no private
Ashlar validator may redefine their meaning. Include isolated nodes, parallel
edges, typed endpoint failures, updates/deletes and schema evolution.

Weft-generated query tests must compare independently authored expected results
against immutable publication data. Cover singleton/list filters, relationships,
joins, bounded one/two-hop traversal and aggregates. Record numeric
precision/scale, result identities, multiplicities and unsupported-query refusals.
Every emitted integrity obligation and closing source/pin/authorization/retention
check must execute before releasing results. Do not use compiler output as the
expected-result oracle.

Fabric Graph GQL, GraphFrames and PuppyGraph Cypher/Gremlin experiments must
execute against releases derived from a complete publication. Record engine
version, release identity, refresh/snapshot behavior, original composite node and
unique edge identities, query corpus and access limits. Export/model validation
alone cannot prove engine execution. Test canonical parity and explicit omitted
scope; managed-UC compatibility must be demonstrated, not assumed.

## Explicit Paths compiler distribution

Treat Weft CONTRACT-003's compiler distribution properties DIST-F1–F4 and
DIST-L1 as P0 consumer gates: cover 100% of the selected properties with positive
and refusal controls before an installation-availability claim. The Paths
installation has its own typed configuration, `weft-compile/0.4.0` /
`weft-ir/0.4.0` schemas and `weft-backend/0.3.0` manifest. Preserve the old
installation profile and its refusal behavior.

| Property | Primary layer | Required checks |
| --- | --- | --- |
| DIST-F1 | Portable package admission | Inject the independently trusted index revision/digest and platform before caller package admission. Refuse missing/duplicate/unregistered identities and incompatible profiles; a local manifest, matching checksum or passing corpus cannot register itself. |
| DIST-F2 | Package/receipt correspondence | Check exact source inventory, executable, features, target, backend, schemas and complete declared corpus against the selected realization. Preserve every request/response byte, legacy namespace refusal, fresh-binding control and capability-coverage reference. Refuse malformed/duplicate metadata, escaped or nonregular paths, extra/missing files, opening/closing drift, exceeded file/total/decoded bounds and truncated, trailing or multiple gzip members. |
| DIST-F3 | Host contract and native integration | Verify registration leaves all source, policy, publication, stored-value and ACK obligations pending. Exercise failed guards, wrong result schema, incomplete capture and closing failures through injected ports; qualify actual native behavior separately against original source data. |
| DIST-F4 | Installation/restart and process transport | Failures before the final ready-link availability commit, or any failed required verification, leave the component unavailable. Noncritical postcommit staging-cleanup failure sets `cleanup_pending` and does not itself revoke an intact verified ready installation. Reopen using only the installed tree and trusted configuration; reject tampered executable/schema/receipt/ready bytes. Reject nonzero exit, stderr, malformed UTF-8, partial/extra protocol output and exceeded input/output/deadline bounds without fallback or artifact release. Preserve the primary failure through cancellation and descendant/stream cleanup. |
| DIST-L1 | Local installation integration | With an explicitly trusted valid realization, available package, passing receipts, compatible platform and writable fresh destination, complete installation and reopen successfully. Missing external inputs remain explicit prerequisites. |

Portable checks use temporary files, injected trust/platform ports and controlled
child processes; they require no native SDK or engine. Compare produced-byte
compiler evidence to independently accepted complete expectations. Preserve
malformed raw requests in refusal controls; never repair SQL, normalize response
bytes or derive semantic expected results from the executable under test. Valid
fresh publication bindings must remain usable beyond the qualification fixture.

Test `installed_schema_bundle` against the retained exact schema bytes and actual
offline validation; importing a schema or returning a claim-only callback is
insufficient. For `ashlar_host.path_admission`, `path_capture` and `path_execution`,
verify immutable callback/input custody, complete bounded capture, original
parameter/result representations and successful publication-hold closure before
return. The outer composition must close readers/clients before releasing its
report. Importing these modules must not construct an engine, install a compiler
or grant source authority. This strategy specifies executable checks; it does not
assert native qualification, OpenTelemetry integration or mechanical proof.

### Installed publication-held Paths workflow

Run `query-commerce-paths` through the installed package with its separately
selected indexed compiler and actual offline installed-schema validator.
Compare independent raw-source expectations for original commerce collection,
expanded path count, distinct terminal count, one-hop related keys, positive
nonempty grouped expanded-path count, original property-equality join,
fulfillment, partial return,
settlement and refund arithmetic. Preserve original UMF 0.8 bytes/version;
a separately authored model requires its own admitted source profile.

Record original requests/recompiles/results and the complete opening/closing
publication, native schema, source, compiler and protected ACK vectors. Exercise
unknown-obligation, changed binding, stale schema, capture-bound, closing-drift
and cancellation refusals through public host APIs. Fake-port tests qualify their
component invariants; actual native results and successful reader/client/Spark
cleanup are required for the installed workflow. Neither the historical compiler
profile nor its native receipts qualify a newly selected compiler implicitly.

For `query-commerce-paths --profile paths-keys`, independently qualify all ten
original commerce cases under the selected new compiler and immutable index.
Retain the original Paths profile's compiler/refusal cases. Pure one-hop decoder
tests exercise closed descriptors/carriers, exact pins, original bytes, composite
String tuple order, equal-key multiplicity, empty/bound/bound-plus-one markers,
malformed Unicode/JSON/numeric atoms and finite byte limits. Decoder tests do not
prove full-bag or native ordering semantics.

Component admission/execution tests require exact one-hop collection/ordinal
obligation inventories, complete consumed-edge schema observations even for
empty or standalone tables, unknown/duplicate obligation refusal and all existing
closing/cancellation gates. Explicitly refuse unmatched LEFT roots, grouped or
aggregate collections, expansion mixing and non-String keys. Tiny actual native
fixtures cover parallel edges, forward/inverse traversal and composite key order
against independent source expectations. Returned key tuples prove key order
and occurrence multiplicity; equal tuples cannot reveal edge tie-breaking.
Retain an independent native ordinal-to-original-edge witness for negative,
zero and positive BIGINT IDs and review its correspondence to the actual lowering.
Do not infer edge order from identical returned key tuples. Verify complete
native edge non-null/uniqueness and guarded DECIMAL ordinal overflow behavior;
an artificial counter boundary may test the arithmetic guard, but cannot claim
a physically enumerated population of that size. Full-bag checks must precede
truncation and must not inherit the legacy ROW_NUMBER implementation.

Refresh TD-001 PUB-F1/PUB-F2 correspondence for the profile: no result before
successful held and outer closure, and no source progress change. Review the
unchanged publication model separately from query semantic/native qualification.

## Evidence and performance policy

Fixture, simulated contract, native target and engine execution evidence are
separate layers. A small local simulator cannot prove native atomic visibility,
source fencing, authorization delegation, retained data availability or complete
consumer-corpus coverage. Require original operation/handle custody for uncertain
recovery and original descriptor replay; do not fabricate accepted IDs or ACKs.

Reuse the existing [table-design measurements](../02-design/spikes/SPIKE-001-table-layout.md).
New checks remain small and do not renew scale benchmarks. Latency/freshness/
throughput are measured qualification limits; misses do not change the Unity
Catalog Delta architecture. Do not disable predictive optimization or alter
retention, grants or paid capacity to make checks pass. Record readability under
the observed configuration.

## Build Handoff

Run portable component checks from the repository root:

```sh
PYTHONPATH=src:tools python3 -B -m unittest discover -s tests
```

Use the [runnable example](../../../examples/end-to-end/README.md) for explicitly
configured native commands. Complete claims require cited execution at the same
scope as the requirement; synthetic fixture metadata, exports and authored plans
are insufficient. Full accepted Truss catalog/mutation/feed/ACK proof remains a
requirement for the Truss source lane. Continue independent source, schema,
Weft and engine work when an external prerequisite is unavailable.

The [execution plan](../04-build/end-to-end-plan.md) owns results and pending
work. The [pre-cleanup test plan](../04-build/evidence/documentation-history-20261009/docs/helix/03-test/consumer-conformance-plan.original.txt)
preserves historical execution/status notes verbatim.

## Module, configuration and diagnostic verification

Run `python3 tools/check_module_boundaries.py` with the same policy locally,
pre-commit and CI. Test the actual checker: one allowed edge succeeds and a new
forbidden edge/private cross-module import fails. Inventory exact existing debt;
removing a baseline entry must expose its still-present violation. Dynamic import,
API/type ownership and mutation boundaries need named semantic review evidence.

Configuration tests construct explicit settings without a developer .env,
exercise each source precedence and origin, and refuse missing/invalid operator
fields before SDK construction. Inject synthetic secrets and control characters;
assert they are absent from every console, file, exporter and subprocess artifact.
Browser settings tests reject Node-only/secret configuration and unknown keys.

For an OTel support claim, test an actual exporter/receiver with Resource,
timestamps, severity, event attributes and valid optional trace correlation;
JSONL parsing is insufficient. Exercise exporter outage, oversized records,
bounded queue overflow, concurrent attempts, no active span, shutdown failure and
capture loss. Verify one ingestion route and explicit loss disclosure. Diagnostic
failure must not advance progress, fabricate a commit or release a held result.
One-shot runners inherit no HTTP-service SLO or new scale benchmark requirement.
Verify explicit spanless selection through actual SDK logs and a receiver: no
trace fields or submitted spans, unchanged completion counters, immutable
per-attempt selection, nested/concurrent binding isolation, and refusal of
parent/retry contexts before spanless attempt or transport effects. An unfinished
spanless invocation must not invent unknown span loss.

For the installed command configuration boundary, test all eight required JSON
members and derived origins, duplicate/unknown keys, malformed UTF-8, booleans in
integer limits, oversize files, symlinks and opening/closing file identity. No real
credential file is needed: use synthetic values and prove absence from argv,
stdout, stderr, capture, diagnostic signal payloads and retained receiver evidence;
transport headers may reach only the selected trusted receiver. Disabled commands and
retrieval must operate without selected SDK dependency discovery. Missing or
incompatible selected dependencies and local capture construction failure must
refuse before native effects; ordinary later worker/transport startup failure must
preserve local capture, unknown remote accounting and the business result.
Verify all sixteen dependency pins and actual selected imports precede the sole
positive initialization admission frame and provider construction. Missing or
incompatible transitive metadata, import failure, duplicate/late admission and
negative cleanup must withhold fallback. Test exact typed startup disposition:
both read-only booleans true admit safely closed operational loss; false values,
nonbooleans, subclasses and lookalike attributes refuse. Provider construction
failure after import admission must retain bounded parent-owned cleanup.

Exercise the actual installed retrieval command against immutable closed capture
and compare its bounded complete JSON output with the public reader. Cover every
filter conjunction, zero matches, truncation and open/expired/changed snapshots;
no stdout may escape a refused snapshot. Verify complete bounded serialization
precedes publication; physical output failure preserves already delivered bytes. Timestamp evidence must not be
interpreted as causal order or an unsupported timestamp filter.

For workflow composition, map the real Paths host's phase transitions to
CONTRACT-006 and DIAG-F1/F2/F3/L1. Inject ordinary diagnostic failure and first
cancellation at construction, emission, finish and shutdown. Verify exactly one
shutdown owner across partial construction and transferred ownership; preserve
business exception identity and native cleanup markers for ordinary diagnostic
failures. Negative controls cover an ordinary business error followed by first
diagnostic cancellation, an existing business cancellation followed by later
diagnostic cancellation, and cancellation at every post-Spark-acquisition phase
hook. Verify the original winning cancellation object and required native cleanup;
composition must close the run without passing the business primary.
Exercise exact `ReaderCleanupState` admission, read-only flags and one-reader
claim. Actual provider-method controls distinguish closing ACK guard refusal
with successful rollback/close from rollback/close failure. Reader controls
likewise distinguish closing native-custody drift from transport-close failure;
no generic context-exit classification may invent cleanup loss. Verify cleanup-only
facts classify cleanup failure, combined failure preserves the business category,
and omitted observation preserves existing lifecycle behavior. The state must
retain no raw exception or authorize native work.
A successful attempt
requires all ten original query results, reader/Spark cleanup, closing custody
and published report. Actual native evidence is required for this workflow claim;
synthetic ports qualify only failure/ownership controls. An external credential
or engine gap does not permit substituting a fake workflow pass.

Before the diagnostic pilot, pin one small synthetic failed-run fixture with
interleaved attempts and a retry, its ground-truth cause and exact commands,
versions and budgets. Give an investigator the failure question without its
answer and compare the diagnosis and cited records with that ground truth.
Predeclare project-owned accuracy, investigation-time, tool-call, context-use
and runtime-overhead thresholds against the same fixture with diagnostics
disabled. Retain actual measurements and sanitized receiver/local evidence;
report unknown or incomplete outcomes explicitly. Service-pilot requirements
apply only when Ashlar owns a service boundary.

## Formal analysis and implementation correspondence

PUB-F1–F4 and PUB-L1 in TD-001 initially require precise semantic review and
implementation correspondence. A bounded executable transition analysis raises
assurance only after its actual evidence is reviewed.
The owning model records its exact command/tool version, bounds and assumptions;
retain completed exploration, success/recovery witnesses and deliberate
broken-guard counterexamples. No currently green model is presumed by this plan.
The command must return failure for a property violation or incomplete search.

Map each transition/guard to actual publisher, recovery, resolver and protected
ACK code plus existing small contract/native tests. Reuse exact missing-journal,
original-submission absence, lost COMMIT response, manifest-before-ACK and
closing UUID/pin refusal controls. Qualify current-core fixture proof separately
from required public-UMF admitted commerce evolution/relationship and two-source
coverage; never count structural synthetic edges as public semantic admission.
Recheck affected model/code/config correspondence after changes. Model-green
cannot substitute for real durable effects, ordinary-role ACK or source authority.

CONTRACT-006's DIAG-F1–F3 and DIAG-L1 require precise semantic review and
implementation correspondence for capture, SDK worker and host composition.
Map each property to its actual guards and tests; recheck the mapping when
configuration, transport, cancellation or mapped code changes. Verify authority
isolation, sentinel privacy in every sink, bounded loss, primary-failure
preservation and deadline/owned-resource cleanup with small actual runtime
controls. State clock, scheduling and cancellability assumptions separately
from observations. Actual SDK-to-receiver mapping is necessary for an OTel
integration claim; a capture test or finite transport spike cannot qualify the
complete host path. Any executable diagnostic model must retain its bounds,
reachable successful-close/loss witnesses and broken-mechanism negative
controls; incomplete analysis cannot satisfy that assurance level.
