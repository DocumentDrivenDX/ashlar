---
ddx:
  id: CONTRACT-001
  type: contract
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: FEAT-004
    kind: informed_by
  - id: FEAT-002
    kind: informed_by
---

# Contract: publication boundary

**Contract ID**: CONTRACT-001
**Type**: boundary
**Version**: proposed ashlar-publication/0.1
**Status**: Draft
**Related**: FR-4; PUB-01–PUB-10; US-004

## Purpose

Define an implementation-independent publication boundary for external producers,
Ashlar gold state and consumer progress. The proposed synthetic profile makes
ordering and refusal outcomes precise without selecting UMF fields or vocabulary.
Normative statements apply to this profile if adopted; owner approval is pending.

## Scope and Boundaries

Ashlar owns acceptance, projection integrity and published progress. Producers
own source transactions and their guarantees. This profile uses one ordered feed
per source authority, whole-entity replacements, explicit edge identities and
atomic change batches. Transport, physical tables, UMF binding and live connectors
are outside this Contract. A producer with property-level events requires an
explicit adapter into this boundary; this does not redefine its native semantics.

## Normative Surface

All objects below are abstract boundary records. Names are exact for the synthetic
fixture profile, not a selected production wire format. Strings are nonempty;
identity comparisons are exact and case-sensitive. Values are JSON-compatible
synthetic data; no native type-coercion or decimal representation is implied.

| Element | Type / Shape | Required | Rules |
| --- | --- | --- | --- |
| Entity identity | `{kind: node or edge, type: string, key: string}` | yes | Tuple is opaque; no concatenated-key comparison |
| Batch | `{feed, epoch, position, previous, revision, changes}` | yes | Feed/epoch/revision strings; position and previous nonnegative integers; changes an ordered array |
| Change | `{identity, version, op, value?}` | yes | Positive integer entity version; op is replace or delete; value required only for replace |
| Node value | `{properties: object}` | replace node | Whole value replacement; omitted properties are removed |
| Edge value | `{source: identity, target: identity, properties: object}` | replace edge | Both endpoints MUST be nodes present in final batch state |
| Publication | `{id, progress, recordedAt, current, history, tombstones}` | yes | ID identifies immutable visible state; recordedAt is a timestamp string |
| Progress entry | `{feed, epoch, position}` | yes | One entry per feed/epoch; vector, never a cross-feed scalar |
| History entry | `{identity, version, op, value?, feed, epoch, position}` | yes | Unique accepted identity/version; deletes have no value |
| Tombstone | `{identity, version}` | delete | Preserved until an explicitly safe bootstrap/expiry procedure exists |
| Outcome | `{status, code?, publication}` | yes | status accepted, duplicate or blocked; blocked retains last complete publication |

MUST reject malformed records before changing visible state. This fixture profile
starts each feed at position 0 and accepts a batch only when previous equals its
published position and position equals previous+1. A duplicate position is accepted
as duplicate only when the entire batch matches the retained accepted record;
conflicting reuse is blocked. Comparison ignores object-member order but preserves
array order, value type and missing-versus-null distinctions.

Within an entity identity, versions MUST increase for changed content. Repeating
an accepted identity/version/op/value is a no-op even at a later feed position;
an older version is a no-op. Same-version different content is VERSION_CONFLICT.
No history entry is added for duplicates or stale changes. An accepted stale batch
still advances source progress. Version ordering and feed ordering are distinct.

A batch MUST validate its final prospective state and publish all changes and
progress together, or publish none. Node deletion with surviving incident edges
is ENDPOINT_INVALID. Deleting those edges in the same batch is permitted. Multiple
changes to one identity in a batch MUST appear in increasing version order.

Delete removes current state and records history and a tombstone. Older replace
MUST NOT resurrect the entity. This profile blocks any newer replace after delete
as RECREATION_UNDECIDED; a future recreation policy requires a profile revision.
Full synthetic history and tombstones are retained; production retention remains
open. Publication ID and time are operational metadata: replay equivalence means
current, logical history, tombstones and progress, not equal wall-clock times.

A batch revision MUST belong to the selected supported-revision set before any
change is considered. An unsupported revision blocks the feed at its previous
position. Unknown semantic content MUST NOT be dropped: refuse and retain the
original input for explicit recovery. No silent quarantine is selected here.

## Precedence and Compatibility

This profile refines FEAT-004 without closing PRD Q2/Q3/Q9/Q10 for production.
One source authority owns each identity in a run. A second feed claiming it is
SOURCE_CONFLICT. Independent feed positions remain separate. Changed ordering,
identity, update, retention or deletion rules require a new profile version.
An epoch change requires a reviewed bootstrap and reconciliation; numeric
positions from different epochs MUST NOT be compared.

Replay requires retained accepted batches or equivalent deduplication evidence.
If the requested replay point predates that evidence, publication MUST block
with RECOVERY_REQUIRED rather than assume it is safe. No transport exactly-once
or multi-table transaction mechanism is claimed by these boundary requirements.

## Error Semantics

| Condition | Code | Retry | Recovery |
| --- | --- | --- | --- |
| Missing/malformed field | INVALID_INPUT | after correction | Keep previous publication |
| Missing contiguous batch | FEED_GAP | after gap supplied | Resume from previous position |
| Reused position with different batch | POSITION_CONFLICT | no unchanged retry | Reconcile producer evidence |
| Same version, different content | VERSION_CONFLICT | no unchanged retry | Producer resolves conflict |
| Unknown revision | REVISION_UNSUPPORTED | after support added | Retry original blocked batch |
| Missing or invalid endpoint | ENDPOINT_INVALID | after correction | Submit valid complete batch |
| Competing identity authority | SOURCE_CONFLICT | after authority decision | Reconcile source ownership |
| Replace after tombstone | RECREATION_UNDECIDED | after policy revision | Explicit recreation policy |
| Epoch mismatch or expired evidence | RECOVERY_REQUIRED | after reconciliation | Safe bootstrap |
| Failure before publication commit | PUBLICATION_INTERRUPTED | yes | Retry batch; old publication remains visible |

## Examples

A feed S/epoch e starts at 0 with supported revision r1. Position 1 creates
node TypeA/A1 version 1 with `{label: "old"}`. Position 2 replaces it at
version 2 with `{label: "new"}`. Replaying both batches yields label new,
two history entries and progress S/e/2. A stale v1 replacement at position 3
adds no history and leaves label new, while progress advances to 3. Delete v3
at position 4 removes A1 and leaves tombstone v3. Replaying v2 at position 5
cannot restore it.

## Validation Checklist

- [ ] Producer confirms whole-entity adapter, authority and ordering guarantees.
- [ ] Owner reviews rejection, recreation and retention proposals.
- [ ] Shared corpus exercises every outcome against an implementation.
- [ ] Physical publication mechanism proves visible-state/progress atomicity.

## Non-Normative Notes

Every producer mapping MUST identify exact native-to-boundary loss and preserve
original source evidence. Boundary conformance, Databricks compatibility and
producer-adapter qualification require independent evidence.

## Durable publication coordinator boundary

The proposed Python publish_batch coordinator consumes a complete source batch,
explicit stream, original predecessor publication, exact nonempty named schema
revision JSON and caller context. It derives and retains one deterministic full
request digest over the original stream/batch/predecessor/schema/source custody.
Changed semantic request under an existing attempt key refuses before effects.
Revision inventory must be duplicate-free JSON with named nonempty string values.
The request mapping is immutable; backend implementations retain exact original
bytes and do not reconstruct intent from current graph state.

PublisherBackend is a mandatory native port, not a default implementation:
writer holds current authority/exclusion/source fencing through the attempt;
observe returns original durable Attempt or None only after proving absence;
prepare retains complete original input and predecessor/revision/authority
custody. Unresolved original submission never counts as absent. Every state-
changing native port preserves original handles/receipts and uncertain outcome.
An ordinary caller flag or in-memory callback cannot supply durable authority.

The attempt phases are prepared, applying, applied, committing and committed.
Persist applying before any graph submission; apply returns the completed actual
effect result, which retain_applied preserves once under the original request.
A later applying attempt uses recover_apply to inspect original native handles/
effects; it never invokes apply again or generates a new mutation plan. Missing
original result/custody refuses rather than filling from current tables.

Validate complete actual source/schema/graph/history/tombstone/delivery/pin
correspondence under current authority before descriptor submission. Validation
must succeed with None, never a false/success flag or partial report. Persist
committing before descriptor submission; commit returns one immutable original
descriptor and retained committed attempt. A later committing attempt uses
recover_commit to resolve the original submission/descriptor, never commit again.
Original apply/result/descriptor/resource and native recovery producers remain
backend obligations; these signatures do not qualify their implementation.

Only a committed original descriptor reaches acknowledge. That port independently
checks source/descriptor correspondence and current checkpoint authority, moves
progress only to the completed boundary and handles identical replay idempotently.
A committed repeat observes the retained descriptor and reuses acknowledgement;
it does not rerun graph effects, validation callbacks or descriptor production.
No receipt from an earlier phase permits acknowledgement. False/non-None policy
or acknowledgement completion refuses. Unknown commit/ack outcome retains the
original recovery custody and cannot create a replacement publication.

Native publication production MUST connect staging/apply/resolver ports to
complete durable attempt stores and recovery procedures. A materialized graph
fixture MUST NOT be promoted solely from coordinator unit checks; original
effect, authority, retention, manifest and source-checkpoint obligations remain
mandatory for every supported producer profile.

## Native attempt phase custody store

DeltaAttemptStore MUST implement the serialized original-phase carrier.
Its six nonnull STRING columns are stream, batch_id, phase, request_digest,
payload_json and payload_digest; the logical key is (stream,batch_id,phase).
Native SHA256 checks bind payload_digest to exact payload_json UTF8 bytes.
The payload has exactly request, result_json and descriptor_json. Request
retains all original coordinator fields including its verified request digest.
Result/descriptor are exact original JSON text artifacts, not expanded trees.
Unknown content inside those artifacts remains unchanged. Artifact semantics
and original producer authority are separate admission obligations.

Within an injected authorized exclusive-writer session, verify native table UUID,
read at most six records for one bound attempt, and require a contiguous prefix
of the five declared phases with no duplicates/unknown substitutions. Every
record must have exact digest and matching original request/source identity.
Prepared/applying have no result/descriptor; applied retains one result;
committing retains exactly that result; committed additionally retains the
original descriptor. Phase payload budget is 4MiB. Duplicate JSON members and
invalid original artifact text refuse. Read-side verification does not repair
or recreate missing original phase custody.

Append validates complete expected predecessor and original input/result
correspondence before parameter-bound MERGE. Identical original phase replay
returns its retained record without writes; changed phase/request/result refuses.
Native readback verifies exact new prefix/record. Release verifies the same
table UUID and invalidates the session object. Unknown native outcomes require
original submission handle/state recovery and cannot count as proven absence.
The exclusive writer remains a mandatory external policy, with no permissive
implementation; a local caller flag cannot establish native authority.

This carrier is not complete native publication production: it does not itself
validate graph effects, implement source fencing/grants, inspect uncertain graph/
descriptor handles, create an immutable manifest or acknowledge a source. Its
phase label alone never proves semantic completion or actual outer commit.
The qualified publisher backend must independently verify original effect/
descriptor provenance and current authority. Client state-machine checks and
MERGE replay do not establish remote/adversarial uniqueness or direct-DML denial.

### Native outbox checkpoint custody

The candidate outbox wrapper additionally retains exact source_checkpoint_json in
the immutable original request. Its closed ashlar-postgresql-outbox/0.1 carrier
contains feed, epoch, previous native group position, position, original UTF-8
payload digest and contained batch identity. Canonical nonnegative signed64
positions must be contiguous. Exact original inner transaction bytes/profile/
feed/epoch/identity and independently zero-based inner byte cursor must agree.
The full original checkpoint text participates in the request digest and every
phase retains it unchanged. Phase readback revalidates it against the original
contained source artifact, not merely a recomputed request hash.

Legacy JSONL-only requests preserve their original shape/digest. An outbox producer
must use the wrapper; an inner JSONL byte offset cannot substitute for its native
position. The native backend must independently bind the descriptor's progress
and acknowledgement to this original native group under current source authority.
Checkpoint-shaped text, wrapper success or retained phase custody alone grants no
native authority or source ACK. Changed outer position under an existing attempt
is a different original request and refuses instead of committed replay.


### Descriptor-to-native checkpoint correspondence

For the selected outbox handoff, source_progress_json maps the original feed to
its complete closed native checkpoint carrier (profile/feed/epoch/previous/
position/payload_digest/batch_id). Other feed progress remains in the original
descriptor. bind_outbox_descriptor requires the independently admitted publication
ID, exact full original schema inventory and validation_report.request_digest,
then checks the complete native checkpoint against that feed's descriptor entry.
It revalidates original request/inner-byte custody first. A JSON numeric position,
inner byte offset, changed epoch/payload/batch or a different request refuses.

This is correspondence validation, never publication or ACK authority. The native
backend must independently resolve immutable original manifest/effects/pins and
current source/checkpoint permission before acknowledging. It cannot call this
helper with a caller-created Descriptor and infer authoritative native custody.

## Stored publisher composition

StoredPublisherBackend MUST connect publish_batch to a held DeltaAttemptStore
session, a mandatory native effect/source driver and a mandatory manifest-store
factory. Its session is single-use, nonreentrant and invalid outside the held
stream/context. Phase reads retain original request bytes, complete result text
and descriptor custody rather than reconstructing them from current state.

Applied result text MUST be bounded JSON with exactly effects (object) and
manifest (complete immutable row). The driver MUST retain actual original
effects and proposed manifest provenance before returning that artifact, including
original publication clock, UUID/version vector, schema/source custody and
configured finite retention anchors. The bridge checks exact request digest and
schema revision correspondence. Native source checkpoints additionally pass
bind_source_descriptor; other source profiles still require independent complete
source correspondence in the driver. These checks do not admit effects by flags.

The applied artifact is retained unchanged across applied, committing and
committed phases. Current complete driver admission MUST return None before
initial commit and every recovery. The factory's commit/recover result MUST
equal the complete retained manifest row. Recovery uses its separate recover
port and cannot substitute commit. Only after exact descriptor custody enters
committed can current source authority consider ACK; identical committed replay
retains the same descriptor and renews ACK through the driver.

Host JournaledManifestStore connects the manifest store to an explicit original
OperationExecutor identity and DurableSQL journal. Recovery requires that original
submission record, renews mandatory manifest admission/readback and recovers the
same terminal receipt or handle. Missing original submission or handle refuses
for explicit reconciliation; a phase label never permits a new manifest POST.
Closing admission failure preserves an already committed row, leaves phase
committing and emits no ACK. A later admitted recovery reuses original custody.

Host JournaledAttemptExecutor supplies explicit phase SQL identity from an
admitted namespace plus stream/batch/phase. Changed payload under that identity
conflicts. Before any native phase read, it resolves outstanding original
submissions within that namespace through DurableSQL. Pending handles or
uncertain no-handle submissions refuse before a read can claim phase absence.
It does not establish exclusive writers, source authority or immutable grants.
The namespace and journal must be independently bound to the original installation
and authority; replacing a journal cannot establish absence or recovery.

## Immutable JSONL source checkpoint

The additional ashlar-immutable-jsonl/0.1 checkpoint profile binds an admitted
ashlar-jsonl-transactions/0.1 SourceBatch. Its exact fields are profile, feed,
epoch, previous, position, payload_digest and batch_id, all strings. previous
and position are canonical nonnegative signed64 byte-offset cursors from the
original complete batch; position MUST be strictly greater than previous.
payload_digest is SHA256 over exact begin + ordered record bytes + commit bytes.
Batch custody MUST reparse and match the original transaction. Unknown fields,
versions, changed bytes/cursors or substituted group ordinals MUST refuse.
The existing CSV row-ordinal and PostgreSQL outbox group profiles stay separate.

JournaledFileProgress is a shared host handoff for the two explicitly supported
immutable-file adapters. CSV retains its original journal tables/configuration,
request/checkpoint profile and row ordinals. JSONL has separate scope/progress
tables and actual byte positions. The bounded private original file MUST match
its original full SHA256 and adapter configuration on every admission. Complete
source checkpoints MUST form the original ordered prefix; byte offsets MUST NOT
be inferred from batch counts. position() returns the source cursor, while
completed_batches() identifies the number of complete original groups for resume.
A missing group, changed file/epoch/configuration, predecessor or retained proof
MUST refuse. Registration of a new source profile alone grants no authority.

Mandatory source/writer policy and native resolver admission remain held across
the durable local COMMIT, including complete publication correspondence and pin/
retention checks. A post-COMMIT closure failure remains explicit
LocalProgressOutcomeUnknown and licenses only original-receipt reconciliation.
This is local consumer progress, never native Truss registration or remote ACK.

The host source runner MUST select CSV or JSONL through the same admitted
stored-publisher boundary. Each selected schema/value/source profile requires
explicit binding and authority; fixture IDs MUST NOT become accepted native IDs.
The CSV wrapper MUST preserve its original stream, publication and row-ordinal
checkpoint meanings when delegating to the shared runner.

Source switching requires its own admitted empty installation and original
journal. It MUST NOT overwrite an existing publication or treat distinct feeds
as one state without an explicitly admitted combined-source profile.

## Installed original-commerce two-source evolution boundary

The named development profile `ashlar-commerce-evolution-transactions/0.1`
MUST be available through installed host APIs `publish_commerce_evolution(config)`
and `resume_commerce_evolution(config)`. This finite profile does not authorize
arbitrary model migration or confer Truss source authority. CONTRACT-003 owns
its complete source/materialization meaning.

Fresh and resume configuration MUST have distinct immutable types. Both MUST
supply original input custody, the explicitly selected public UMF producer and
runtime, finite resource bounds, installation identity, graph/attempt/manifest
registry, two independent source registrations and their ordinary source/ACK
session factories. Operator-owned resource handles and secrets have no committed
default. Configuration MUST be validated before construction or effects; the
portable core MUST NOT read environment, discover runtimes or construct clients.
A registration descriptor or configuration flag alone MUST NOT establish source,
writer, publication, retention or ACK authority.

Each source registration MUST bind its original source installation, feed,
epoch, source namespace, complete transaction-byte inventory and protected ACK
scope. Current authority MUST be independently established by mandatory native
policy/session ports. Source identity qualifies overlapping local entity keys;
feed positions and schema revisions remain independent while publications retain
one complete ancestry and progress vector. The host MUST NOT provision sources,
create roles/grants, discover Docker credentials or escalate sessions
through this workflow. Supplied credentials MUST remain opaque to diagnostics.

Fresh configuration supplies the proposed layout and independent authority; it
MAY initialize only a separately authorized, new empty installation. Authorized
initialization MUST establish and retain the actual table UUID registry; proposed
UUIDs MUST NOT stand in for that observation. Resume configuration MUST name the retained original installation,
journal, table UUID registry and original source/operation custody. Resume MUST
NOT initialize, reseed or replace a missing journal, regenerate submitted SQL or
operation identities, or infer absence from current graph rows. An unresolved
submitted operation, absent original handle, ambiguous native result, changed
input/registration/epoch/UUID or missing original plan MUST withhold publication
and ACK until original-operation reconciliation establishes the outcome. A
retained complete plan MAY first-submit an originally unsubmitted ordinal only
after proving it has no original submission. An already submitted or uncertain
ordinal MUST NOT be resubmitted. A previously committed operation with lost
response is a distinct recoverable case;
its evidence MUST NOT qualify unresolved absent or ambiguous submissions.

The orchestration MUST use the stored-publisher boundary for every complete
source transaction. Exact replay MUST preserve original descriptors, native
versions and logical current/history/tombstones while independently renewing
source ACK admission. Restart MUST reconstruct work from retained original
custody and complete admitted progress, never from in-memory prefix counters.
Default execution MUST NOT inject failures. Fault controls use explicitly named
verification compositions and cannot silently alter the installed public API.

Success MUST be released only after complete independent prefix-union validation,
protected ordinary ACK readback, closing source/native/installation admission and
owned resource cleanup. Failure or cancellation MUST preserve the primary outcome
through cleanup and withhold the success report. CONTRACT-006 owns bounded safe
run/attempt diagnostics; telemetry and receipt flags MUST NOT establish commit,
source authority or ACK. Report failure MUST NOT imply rollback of an already committed publication or
ACK. Qualification requires installed execution without a
checkout plus actual fresh-process original-journal resume; simulator-only guards
or prior checkout runs cannot substitute.

The following configuration groups MUST each have one owner and declared source;
unknown groups/options and invalid finite bounds MUST refuse before effects.

| Configuration group | Owner and source | Required admission |
| --- | --- | --- |
| Finite profile and eight-step schedule | Developer-owned immutable package definition | Exact supported version and complete original schedule; no fault-injection option |
| Model/graph, preparation seed/proof, presence candidate/proof and registry | Caller absolute locations or fixed package resources; developer content pins | Bounded exact original bytes/inflation; original and authored augmentation distinguished |
| Evolution producer revision, checker, Bun/Git locations | Developer version/content profile; operator explicit locations | Separate exact e44cd15f336dfb33db35acf20eee13dd120a1a28 profile, opening/closing custody and fresh public verification |
| Fresh root, installation ID, stream, predecessor, clock and namespace | Explicit operator/trusted fresh configuration | Exclusive new target; complete original plan retained; actual UUIDs observed under independent authority |
| Resume installation/registry, journal and original plan; invocation evidence output | Explicit retained installation custody | Exact original identities and separate fresh invocation output; no initialization fallback |
| Two source and consumer/ACK registrations and ordinary session factories | Independently authorized source operator injection | Complete actual identity and current source/ACK rights; descriptors alone grant none |
| Native runtime/JAR locations and writer/exclusivity/retention ports | Operator locations/authority; developer runtime/hash profile | Exact selected Python3.11 native profile, independent authority; portable core remains3.9 |
| Input/receipt/report/result limits, deadlines and diagnostics | Developer admissible ceilings; operator finite selections | Typed non-bool bounds, no silent truncation, CONTRACT-006 privacy/loss; no unimplemented hard native deadline claim |
