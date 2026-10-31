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
**Version**: proposed ashlar-publication/0.1 with profile-specific `ashlar-managed-operational-retention/0.1` amendment
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
payload_json and payload_digest. The compatible unscoped profile uses logical
key (stream,batch_id,phase). The installed outbox evolution profile MUST select
an exact verified original request and use logical key
(feed,epoch,stream,batch_id,phase), with feed/epoch derived from its original
checkpoint. It MUST preserve all six columns and original request/batch bytes,
permit overlapping source-local IDs across qualified sources, and refuse changed
original requests within the same qualified identity. Bounded complete original
stream/batch rows MUST be validated before source selection; malformed or unknown
source custody MUST NOT be hidden by native JSON scope predicates.
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

Local managed-Delta publication MUST durably prepare the exact manifest mutation
before appending `committing`. Preparation retains the original request digest,
manifest row, statement, parameters, operation identity, target UUID and native
predecessor under the existing installation journal and current writer authority;
it MUST NOT submit a native mutation. The manifest store exposes
`prepare(row, context=...)`, returning `None` only after complete opening and
closing admission. Repeated preparation MUST verify exact retained intent rather
than change it. A preparation failure MUST leave the publication out of
`committing` and withhold ACK.

Recovery from `committing` MUST require that retained manifest intent. A retained
`prepared` intent MAY be first-submitted after independently verifying its native
predecessor and current admission; submitted, uncertain or missing intent MUST NOT
be replaced. Interruption after preparation but before the phase append resumes
from `applied` by verifying the same intent. Older `committing` publications that
lack this intent remain reconciliation cases; a proposed manifest or phase label
MUST NOT manufacture it. Legacy manifest adapters without the preparation port
retain their stricter original-submission recovery profile and MUST NOT claim
this local preparation guarantee. An exposed but invalid preparation port MUST
refuse rather than silently downgrade.

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

## Selected Truss record evolution

`evolve-truss-record` MUST accept a closed configuration with `connection_uri`,
`bun`, `document_path`, `document_revision`, `origin_path`, `accept_request_key`,
`update_request_key`, `object_id`, `document_id`, `module_id`, `record_id`,
`field_name` and `field_value`. The selected object ID MUST be a positive
signed64 integer. The authored revision MUST retain its declared UMF version;
the local additive-field profile admits declared UMF 0.7.0 or 0.8.0 rootless
records and a selected supported scalar field. Previous and revised documents
MUST retain the same declared UMF version. Preparation MUST select the public
SDK path for that exact version; unsupported or cross-version selections MUST
refuse before acceptance.
The previous accepted record MUST NOT already declare that field, and the new
record MUST declare it before acceptance. Truss owns model compatibility and
value validation.

Schema acceptance and the expected-version object update are separate caller
commits. The command MUST preserve native identity and every original value,
read back the accepted document and updated object, and verify the original
committed update receipt. Its private report MUST retain exact previous/revised
catalog and acceptance bytes after resource closure. This command advances no
consumer checkpoint; publication consumes the original contiguous feed pages.
A later failure MUST NOT imply rollback of an earlier source commit.

## Native Truss continuation page capacity

`continue-truss-graph --maximum-page-bytes N` MUST select an explicit integer
read bound in bytes, from 1 through 2,097,152. Omission retains the 1,048,576-byte
default. The selected bound MUST reach the ordinary public feed reader without
truncating or splitting a committed source transaction. Resource refusal MUST
leave source progress unchanged.

The `truss-native-feed/0.1` batch and staged recovery MUST preserve complete
original transactions up to 1,125,000 bytes, matching the retained page carrier's
1,500,000-character Base64 ceiling. Archive encoding MUST refuse a page that
cannot be decoded under the same original bounds, before retaining a publication
intent or applying effects. A larger selected read bound does not authorize
unrepresentable archives. The JSONL transaction staging limit remains 1 MiB;
source registration, identity, epoch, exact-byte custody and publication-before-ACK
requirements remain unchanged.

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

### Fresh current-core two-source acceptance amendment

The separate proposed `ashlar-current-core-two-source/0.2` profile is a fresh
current UMF core `0.10.0` and Truss `0.0.45` workflow. It MUST NOT continue,
relabel or use completed ordinals from the original UMF 0.8
`ashlar-commerce-evolution-transactions/0.1` installation. Its two source
registrations A and B MUST each have an independently accepted Truss source
installation, feed, epoch, source namespace, original transaction inventory,
ordinary source session and protected ACK scope. A/B are workflow labels only;
the native source identities MUST remain distinct even where local entity IDs,
revision numbers or feed positions overlap.

After actual Truss catalog acceptance, the exact canonical bytes and digest of
the complete eight-slot semantic schedule, source assignments, accepted
model/schema pins, source and ACK identities, and independently expected values
and progress MUST be frozen before the first native publication attempt. Each
slot's exact original mutation-request bytes and digest MUST be resolved from
verified predecessor results and durably bound to that schedule digest before
that slot's source mutation. The immutable schedule and slot records MUST be
retained with the new installation's journal and complete table UUID registry.
At each ACK-gated slot, the actual complete feed page, prior and next cursor,
writer position, event-byte inventory and digest MUST be bound to its frozen
request before that slot's native graph effects. A missing, changed or
unadmitted page MUST refuse effects and ACK; no later page may be inferred from
an earlier request or checkpoint. No draft example, synthetic source fixture,
partial catalog response or old `native-r2` prefix may fill a missing slot.
The owner approved this staged feed-binding rule on 2026-10-21 because Truss
`0.0.45` public
`read_feed_in_transaction(tx, consumer_id, limits)` reads at the registered
checkpoint, while the public feed query requires that checkpoint; it provides
no future-page cursor argument. The observed API constraint and unchanged feed
SQL digest are recorded in the [Truss source comparison](../../04-build/evidence/truss-current-native-lifecycle-0.10-20261021/truss-0.0.45-source-comparison.json).
That 2026-10-21 decision approves only staged feed-page binding and does not
establish R04 execution evidence.

#### Approved R04 slot-by-slot request freeze

**Owner decision, 2026-10-23:** approved this request-freeze timing amendment
for R04. It changes only when exact request bytes are frozen; the separate
2026-10-21 decision approving staged feed-page binding remains in force. This
decision does not authorize R04 execution or establish execution evidence.
Within each source, R1 creates the selected entity, R2 updates that entity, and
R3 deletes that same entity. The schedule MUST NOT substitute an unrelated
pre-R04 seed entity for that R1-created target.

Before the first R04 publication attempt, the caller MUST freeze one immutable
closed semantic schedule for all eight slots: ordered operations, A/B source
assignment, accepted model and schema pins, authored values and presence,
symbolic references from each R1-created entity to its R2/R3 operations,
expected state transitions, independent source and ACK identities, and the
complete prefix-oracle rules. A symbolic reference records the intended
lineage; it is not a guessed provider ID and cannot authorize source or graph
effects. The exact canonical schedule bytes and their digest MUST be retained
before the first R04 publication attempt; this digest is the immutable schedule
identity used by every later slot record.

For each slot, the exact original Truss mutation-request bytes and digest MUST
be frozen and durably joined to that slot and the frozen semantic-schedule
digest before invoking the public source mutation API for that slot. The
immutable slot record MUST bind the schedule digest, slot and source identities,
resolved predecessor identities, exact request bytes, and request digest. R1
request bytes are frozen before R1. After a committed R1 returns its
provider-assigned identities, the caller MUST verify its original public
result and readback; only then may it resolve the frozen
symbolic references and freeze R2's exact request bytes, before R2's source
effect. R3 follows the same rule using the verified R2 result and readback.
Apply the rule independently to B. A missing or mismatched result, identity,
version, request digest, or semantic transition MUST stop the schedule before
the next source effect; an exact request MUST never be rewritten after its
source effect or after any slot's graph effect.

Source mutations MUST follow the frozen global slot order
`A1, B1, A2, B2, A3, B3, A4, B4`. A slot's source mutation MUST NOT begin until
the immediately preceding slot's graph publication has completed, its complete
vector and source progress have been independently verified, and its protected
source ACK has durably completed. In particular, R2 and R3 source mutations
MUST wait for the preceding slot's publication, independent verification, and
protected ACK; a failed or uncertain predecessor stops every later source
mutation.

After a slot's source transaction commits, its actual complete feed page,
prior and next cursor, writer position, event-byte inventory and digest MUST
still be bound to that slot's already frozen exact mutation request before
that slot's graph effects, as in the existing staged feed-binding rule. The
publisher MUST refuse a missing, changed or unadmitted page. It MUST publish
and independently verify the complete vector before acknowledging only that
slot's source. The schedule freeze does not permit source write-ahead for
later slots, page inference, early ACK, or reuse of R03 seed bytes as an R04
slot. [CONTRACT-003](CONTRACT-003-delta-graph-tables.md) carries the
corresponding materialization requirement.

Each complete source transaction MUST publish one immutable full table-version
vector and both source progress entries. Protected durable ACK MAY advance only
for that source after its transaction is durably published and independently
verified. A later transaction from the other source MUST NOT collapse, overwrite
or acknowledge the first source's custody. Exact replay/restart MUST recover
from the original journal and accepted feed positions, preserve old retained
publication reads and produce no extra native commit. An uncertain native
submission or missing original journal MUST withhold publication and ACK until
reconciled under the ordinary recovery rules. A breaking evolution candidate
MUST refuse before effects and ACK. Completion requires all eight frozen slots,
current source/ACK readback, complete native vectors and closing admission; a
partial prefix remains incomplete evidence.

### Proposed managed current-core source and publication owner

The separate proposed `ashlar-managed-current-core/0.1` profile MUST begin with
actual current UMF core `0.10.0` accepted source and an explicitly selected
Truss `0.0.44` installation, registration, feed, epoch, ordinary consumer and
destination receipt scope. A development JSONL selection, local Spark journal
or old UMF 0.7 managed publication MUST NOT supply its source authority. The
owner MUST retain complete original catalog/source/page bytes and accepted
identities, prepare the exact request and native effect intent durably, and
publish one immutable complete Unity Catalog Delta table vector with schema
revision and source progress before requesting the exact protected ACK. A
catalog-only transaction is a zero-graph-effect publication, not a skipped
source position. Replay, uncertain submission, restart and ACK reconciliation
MUST use the original intents and receipts without a replacement commit.

Configuration MUST select the target catalog/schema, source registration and
ordinary principal explicitly. Until an independently observed UC installation,
native runtime and source/ACK owner match that selection, the profile MUST
refuse before publication or ACK. The selected dedicated compute, resource and
cost ceilings remain open R05 inputs; this clause grants no production support.

## Explicit original-commerce publication source

The separately selected original-commerce publication host MUST accept explicit
ordinary writer, registered reader and protected ACK session ports for the exact
`private-original-commerce-fixture` feed and `original-commerce-v1` epoch.
It MUST reuse the admitted original transaction bytes, model, graph and independent
complete oracle. Source registrations, roles and signatures MUST be checked under
the actual held installation lease; provider configuration MUST NOT provision,
discover credentials or grant authority. The complete six-table UUID registry,
native reservations and journal MUST already exist under separately authorized
initialization and be empty before publication effects. The installation lock
MUST exclude other cooperating operators of that same installation.

This entry point admits initial publication and exact same-invocation replay.
A source head of one MAY reuse only the complete exact original already-appended
transaction after current source admission; it MUST NOT append a replacement.
A changed transaction, larger head, nonempty native state or missing original
journal MUST refuse. This source-append recovery does not authorize general
process-restart publication recovery. Protected ACK MUST follow the durable,
fully admitted publication vector. Closing source, runtime and installation
custody and resource cleanup MUST complete before releasing the report.

### Explicit original-commerce local initialization and publication commands

The installed `initialize-original-commerce --configuration PATH` command MUST
consume bounded closed profile `ashlar-original-commerce-local-initialization/0.1`
and create only a separately authorized exclusive new private local installation.
It MUST refuse existing roots, tables, journals, reservations and catalog conflicts.
Fresh native creation MUST consume the exact separately authored local UMF physical
model and public UMF-generated schema/layout artifacts for object_current,
edge_current, tombstone, whole_source_history, attempts and manifest. Complete
ordered fields, types, nullability, metadata, layout and properties MUST survive;
unknown or stale model/generated content MUST refuse. A local path binding MUST
NOT introduce a second schema interpreter or arbitrary caller SQL. Actual native
UUIDs, version-zero emptiness and complete schema correspondence MUST be observed
before `LocalDeltaTransport.initialize` creates the original journal/reservations.
Authored UUIDs, a code pin or a generated proposal MUST NOT grant that admission.

The installed `publish-original-commerce --configuration PATH` command MUST consume
bounded closed profile `ashlar-original-commerce-local-publication/0.1`, reopen the
exact retained installation and ordinary source/ACK ports, and delegate to the
existing original-commerce publication owner. It MUST NOT create an installation,
provision PostgreSQL, create roles/grants, discover credentials or invoke legacy
fallbacks. Query, evolution and resume providers MUST NOT invoke this initializer.

Both configurations MUST select absolute original model/graph locations and content
pins, explicit public declared-version UMF producer resources, installation ID,
canonical root/journal, local table namespace, exact operation capacity and native
runtime/JAR locations. Initialization selects a fresh handoff output; publication
selects its retained handoff and independently chosen digest plus a fresh publication
output. Unknown options, duplicate keys, nonfinite numbers, boolean/float integer
substitutions, aliases and oversized inputs MUST refuse before effects. Handoff
bytes MUST contain only bounded correspondence: observed six target table/path/UUID/
version identities, original journal/installation/capacity and model/runtime pins.
They MUST NOT contain credentials or establish current authority.

The initial connector MUST be explicitly selected isolated loopback-trust development
access to an already provisioned dedicated local PostgreSQL fixture. Exact original
scope/feed/epoch, source schema/signature and separate writer/reader/ACK roles MUST
match actual ordinary sessions. Explicit host/port/database, empty password,
`/dev/null` passfile, disabled SSL/GSS and finite connect/statement/lock timeouts
MUST prevent inherited credential discovery. A forwarded port selects a connection,
not route authentication. Other authentication profiles require separate admission;
this fixture MUST NOT be represented as accepted Truss or production authority.

The setup owner MUST acquire and renew the original private root and shared
`.evolution-operator.lock` identity under actual POSIX exclusion; original source,
model/generated artifacts and selected runtime/JAR custody MUST remain current
around effects and closing. Fresh selected Python3.11/Spark3.5.3/Delta3.2.1 processes
and the existing exact publication JAR profile MUST be explicit. Neither command
installs dependencies or downloads packages. Full existing publication/source/ACK
custody, original native registry/reservations and lease admission remain mandatory;
configuration or retained output MUST NOT replace them.

Cleanup and closing admission MUST be attempted for every acquired resource interval,
including errors and cancellation, preserving the primary cancellation identity.
A partial fresh installation or post-effect evidence failure MUST remain retained
and incomplete; no automatic native deletion, replacement or rollback claim is
permitted. Initial publication, same-invocation replay and exact head-one pre-native
source reuse remain governed by the preceding original-commerce section. No general
publication process-restart recovery follows. Reports MUST be released only after
substantive closing admission and cleanup; bounded diagnostics MUST follow
CONTRACT-006 and MUST NOT expose credentials or grant commit/completion authority.


### Bounded fresh original prefix

Fresh publication MAY select an absolute `through_ordinal` from 0 through 7,
retaining the complete admitted eight-publication schedule and original journal.
The installed fresh invocation MAY omit this option to preserve full-run behavior;
when supplied in JSON it MUST be an integer, never null or Boolean. The immutable
Python configuration uses `None` for the existing full-run selection. Invocation
and provider MUST agree on the bound before effects.

Bounded fresh publication MUST begin with all eight original slots unprepared
under independently admitted new installation and source/ACK authority. It MUST
complete every transaction from ordinal zero through the selected ordinal, stop
only between publications, and leave later slots unprepared. All ordinary
per-attempt publication, protected ACK, full-vector and closing gates remain
mandatory. The bound MUST NOT truncate or rewrite the original schedule.

The result and receipt MUST use the distinct prefix profiles described below,
including the original digest, selected ordinal, completed count, total of eight
and complete source progress. An earlier prefix MUST NOT claim full-run success.
A later process MUST continue through the retained original reservation and
journal; it MUST NOT rerun fresh initialization or reset source consumers.

### Explicit original-ordinal continuation

A separately typed retained-run continuation MAY select one absolute ordinal in
the unchanged original eight-publication schedule. It MUST retain the original
run digest, installation, complete eight-slot journal, source registrations and
source/ACK/native authority. It MUST NOT initialize, reset, append replacement
source transactions, skip the first incomplete ordinal or stop within a
publication. Selecting an earlier ordinal than the actual completed prefix MUST
refuse; exact replay of the current completed prefix remains admissible.

Continuation MUST independently validate every earlier completed descriptor,
original native handle/vector, ancestry and protected source ACK through the
stored-publisher owner. The selected publication MUST complete and the complete
eight-slot inventory and actual settled prefix MUST pass current closing gates
before a distinct prefix result is released. Producer, original run, ledger,
lease, ordinary source/ACK and native closing admission MUST be attempted on
opening/body failures and cancellation while their owned resources remain held;
cleanup MUST preserve the primary outcome. Invocation and provider configuration
MUST agree on the exact absolute ordinal before effects.

Within one explicitly held read-only full-ledger admission, committed-handle
inspection MAY reuse bounded complete history observations keyed by the actual
transport owner, table, UUID and path. Each use MUST freshly admit native identity
and preserve every policy, profile, journal-row and exact receipt check. Reused
histories and intermediate receipts remain provisional until independent complete
native histories are freshly compared on interval exit, including failure and
cancellation; changed history or identity MUST withhold admission. Reuse MUST be
bounded to 64 histories, 1 MiB per history and 8 MiB aggregate with existing
snapshot observations; non-retained observations MUST close independently.
This permission MUST NOT cache mutation, recovery or current-head history reads,
source/ACK authority or extend across admissions.

Fresh bounded native identity checks MAY project a newly executed native detail
row to its original STRING UUID and the three original STRING registration
properties before payload capture. Projection MUST preserve the existing bounded
capture, exact UUID/property comparisons and reservation-file checks on every
call; it MUST NOT cache identity or cast native values. Consumers that inspect
other native detail fields, including all initialization properties or medical
location, MUST explicitly retain the complete bounded native row. Unbounded
native detail behavior remains unchanged.

Within that same held read-only admission, an optional typed phase-read port MAY
prove provisional absence for the exact original full-ledger inventory's strictly
unprepared slots. Its carrier MUST bind an actual open core session, context,
original request bytes, stream/feed/epoch, table, UUID, path and active interval.
Before a single version-pinned zero-row probe over at most eight original keys,
the owner MUST retain complete raw phase history within the existing history and
aggregate bounds. Each use MUST freshly admit native identity and preserve all
core writer-policy, request-digest and source checks. Any observed row MUST fall
back to ordinary scoped phase reads, including their validation of foreign-scope
groups; a positive row MUST NOT become a blanket refusal. Missing optional ports,
ineligible keys or insufficient retention capacity MUST use ordinary reads;
malformed exposed ports or carriers MUST refuse. Retained and completed slots,
mutation and recovery paths MUST retain ordinary phase reads. The owner MUST
become inactive before interval release on every outcome; complete fresh closing
history equality remains required, including failure and cancellation. Safety
requires zero-row evidence at the retained version plus unchanged closing history,
not absence asserted by a carrier. Progress requires bounded fallback to the
original path whenever that evidence cannot be retained; no larger capture limit
or additional source/native authority is permitted.

A prefix result MUST identify the original run, selected ordinal, completed
count and total of eight; it MUST NOT claim full-run qualification. A result MAY
report run completion only for ordinal seven after all eight contiguous original
slots and the final current descriptor/vector independently agree. The distinct
prefix profile remains distinct at ordinal seven. Full-run native qualification
still requires all original prefix-union oracles, retained old reads, exact
replay, durable two-source ACK/restart and missing-journal refusal evidence.
Continuation does not authorize a larger resource cap or stale admission cache.

### Explicit verification-only original ordinal

A distinct typed verification invocation MAY select one completed absolute ordinal
0–7 of the unchanged original eight-publication run, including an older completed
publication after later appends. It MUST bind the original run digest, installation,
complete ledger, producer and two source/ACK registrations, selected ordinal and
existing descriptor. Selection MUST NOT truncate complete-run admission or grant
source authority. Boolean/float ordinals, aliases, duplicate or unknown configuration
members and ambient environment selections MUST refuse.

Verification MUST NOT initialize, repair, reconstruct, prepare, publish, submit
native effects, transition a ledger, append source transactions or submit an ACK.
Before driver construction it MUST independently require the complete existing
journal schema, original registry and reservations; missing custody MUST refuse
before any constructor can create tables. Read-only ledger inspection MUST preserve
all eight slots, completed-prefix ancestry, retained or uncertain original handles
and unprepared suffix. The selected slot MUST already be completed; verification
cannot resolve an uncertain slot or manufacture completion.

Current held lease, original run/ledger, producer, ordinary source identity,
permissions, signature, checkpoint, protected ACK and native publication admission
MUST remain mandatory. Protected reconciliation is limited to SELECT/read-transaction
commit of the exact existing request/manifest/receipt under its current owner. An
absent or conflicting receipt MUST refuse; neither acknowledgement submission nor
stored-publisher recovery may be reached. Transient exclusion, pins, read-side
bookkeeping and bounded evidence output are permitted resource effects, not
publication or source-progress advancement.

The driver MUST own a bounded public complete-materialization-bag port for the four
exact data roles at the selected immutable descriptor versions. A Snapshot identity,
row count, digest-only assertion, private transport access or caller-authored oracle
MUST NOT substitute. The port MUST compare complete schema/carriers, values,
presence/null, tombstones, history and occurrence multiplicity against the original
source-prefix union. Exact descriptor data-vector membership remains distinct from
the complete six-target installation identity. Current UUID/location, complete native schema/rows/descriptor observations,
retention and source/ACK correspondence MUST be independently checked; old reads
MUST NOT refresh to current heads.

Verification MAY select `native_schema_profile` independently of resource bounds.
Omission preserves `legacy-sql-bootstrap/0.1`: primitive fields with SQL-default
nullable columns and empty field metadata. `canonical-managed-carriers/0.1` MUST
instead compare all four roles with the exact packaged UMF-generated carrier
columns, order, native types, declared nullability and empty field metadata.
One selected profile MUST apply to every role at opening and closing; native
schemas MUST NOT select their own profile. Null, Boolean, unknown profile names
and caller-authored schema declarations MUST refuse. Invocation and provider
MUST agree before verification. Canonical results and receipts MUST retain the
profile and generated-carrier SHA256. This correspondence selection grants no
source, publication or native authority and MUST NOT weaken any other gate.

Every acquired resource interval MUST independently attempt its required producer,
configuration/runtime, source/ACK, original run/whole ledger and native/vector/bag
closing, including opening/body failures and cancellation, while requisite lease,
writer and pins remain held.
One failed callback MUST NOT suppress another. Cleanup MUST preserve primary
cancellation identity. Only successful closing and cleanup may release a bounded
receipt identifying exact run, ordinal, descriptor/vector, four-bag evidence and
observed completed prefix out of eight. It MUST distinguish selected verification
from whole-run completion and qualification.

Typed input, row, cell, total capture and receipt limits MUST have explicit owners
and conjunctive enforcement before buffering/release; finite row counts alone do
not establish byte capacity. Operator-selected finite process supervision MUST
bound execution, streams and owned cleanup without implying an existing built-in
timeout. The read-only evolution-publication verification supervisor MAY explicitly
select an integer deadline of 1–600 seconds for a finite local audit, including
complete opening and closing ledger checks. This ceiling leaves existing explicit deadline selections unchanged and does not
change any other workflow or native deadline, or lookup latency targets. The explicit operator owner, at-most-1-MiB stream capture and all
input, row, cell, total, source/ACK, writer and independent closing limits remain
required. No capture cap increase, stale observation cache or weakened admission
is authorized.
Safe bounded diagnostics remain governed by CONTRACT-006, without exception
messages, locals, credentials or source identities.

For an effective native metadata row limit of one, capture MAY use one aggregate
summary over a native two-row limit instead of a window and sentinel. It MUST
retain the same schema/type/per-cell measurements and conservative total envelope;
count MUST use literal rows, including all-null originals. Only an empty input
may replace null aggregate metrics with zero. Invalid count, cell, total or null
metrics MUST mask the entire packed native payload before transfer and MUST refuse
on the host. Exactly one closed summary and zero or one original typed packed row
MUST correspond to the admitted metrics; no JSON reinterpretation is permitted.
The single iterator MUST close independently on success, failure or cancellation,
preserving the primary error. This bounds driver transfer, not JVM heap, and MUST
NOT cache current identity or alter multirow capture or authority checks.

The [verification design](../technical-designs/TD-003-evolution-publication-verification.md)
MUST preserve full eight-publication/32-bag, retained old-read, exact replay, durable
two-source ACK/restart, missing-journal refusal and uncertain-operation obligations.
Separately verified bags cannot replace recovery or discharge PUB-L1. Decomposed
verification does not guarantee completion within a selected finite budget.

## Installed original historical medical local operator

The historical medical development profile MUST preserve original declared UMF0.8
model/graph bytes, complete public compact-dataset and Boolean lexical admission,
unknowns/warnings and `ashlar-medical-development-bindings/0.2` identities. Its
complete materialization is 51 objects, 62 independent edges, zero tombstones and
113 history records. Development identities are not accepted Truss IDs.

The installed commands `initialize-original-medical`, `publish-original-medical`,
`replay-original-medical` and `read-original-medical` each require
`--configuration PATH`. Exact owning configuration types are respectively
`OriginalMedicalInitializationConfig`, `OriginalMedicalPublicationConfig`,
`OriginalMedicalReplayConfig` and `OriginalMedicalReadConfig`; closed profiles are
`ashlar-original-medical-local-initialization/0.1`,
`ashlar-original-medical-local-publication/0.1`,
`ashlar-original-medical-local-replay/0.1` and
`ashlar-original-medical-local-read/0.1`. Corresponding exact typed host entries
are `initialize_original_medical_local`, `publish_original_medical_local`,
`replay_original_medical_local` and `read_original_medical_local`.

Configurations MUST explicitly select original intake/producer pins, compatible
qualified native runtime/origins/JAR profile, current source/ACK registrations,
installation/root/journal/six-target handoff/capacity, exclusive output and finite
input/row/cell/total/stream/receipt/process limits. Unknown/duplicate members,
malformed or aliased paths, nonfinite or boolean/float integer substitutions and
ambient credential/runtime discovery MUST refuse. Package pins grant no authority;
portable Ashlar Python support is distinct from selected native SDK compatibility.

Initialization MUST select an authorized absent private root, actual shared lease,
checked UMF-generated schemas and observed six UUIDs/empty version-zero bags before
public journal initialization, followed by its exact registration-stage checks.
All other commands MUST refuse missing/incomplete/foreign journal custody without
initialization or repair. Handoff grants no authority. `OriginalMedicalSourcePorts`
MUST bind exact public source/ACK registrations and independent current ordinary
sessions to the same native installation. One dedicated medical outbox source and
protected ACK scope suffice. Explicit loopback-trust development authentication
MUST close ambient libpq routing/configuration/credential discovery. Publication
MUST NOT provision sources, roles or grants; file-only progress cannot replace ACK.

Recompute complete original compact/Boolean receipts before native acquisition.
Current source/writer/retention authority, six-target identity and full original
bags MUST remain mandatory. Head-zero append or exact head-one original readback
recovery are the only admitted source states. Existing request/attempt/uncertain
handle semantics and durable manifest validation precede protected ACK. Settled
replay MUST preserve histories, bags, descriptor and progress; missing ACK recovery
uses the governed publisher protocol. Read-only reopen MUST independently admit
existing journal schema, original descriptor/versions, current source/ACK,
UUID/location, retention and full four-role bags under actual exclusion/pins. It
permits existing-record SELECT/read-transaction commit only, never source append,
prepare/effect/ACK submission, journal repair or mutable-latest fallback.

`NativeDriver.bind_original_medical_read(plan, bounds)` MUST accept only exact
medical admission, a separately typed `MedicalReadSourceFactory`, a read-only
Delta transport and explicit `VerificationBounds`. The retained plan supplies
the independent oracle; it grants no publisher or writer authority.
`observe_original_medical_read(descriptor, context=...)` MUST verify all six
native schemas, identities and histories and compare complete four-role bags
against that oracle under the held read interval and immutable publication pins.
Every capture, including manifest validation and closing verification, MUST
respect cumulative row/cell/byte bounds; generic unbounded snapshot fallback
MUST refuse. Independently renew native and ordinary source/ACK authority at
opening, use and closing. A missing existing journal or exclusion record MUST
refuse without creating or repairing it.

`recover-original-medical --configuration PATH` owns partial-publication recovery
under `OriginalMedicalRecoveryConfig` and
`ashlar-original-medical-local-recovery/0.1`. Its host entry is
`recover_original_medical_local(config, supervision=...)`. The configuration
selects the existing installation and a pinned original request; it does not
require or fabricate a completed publication descriptor. Before native effects,
require the complete existing journal, original plan and source transaction,
exact request correspondence and current six-target/source/ACK custody.
Read the already committed head-one source through `read_original`; never append
it again, initialize a journal or replace original operation records.

Recovery MUST use the same `DeltaAttemptStore`, `StoredPublisherBackend` and
`publish_outbox_transaction` protocol. Before issuing remaining effects,
reconcile original committed receipts and uncertain handles through their owning
native recovery ports. Missing, conflicting or unprovable originals MUST refuse.
`NativeDriver.preflight_original_medical_recovery(plan, context=..., bounds=...)`
MUST verify the exact original source plan and complete effect-intent inventory
and inspect every already committed ordinal through the native receipt owner
before the publisher can issue any remaining effect. A later corrupted receipt
MUST NOT be discovered only after an earlier missing ordinal is executed.
Full medical schema/bag validation and durable publication precede protected ACK;
independent closing precedes the success receipt. Fresh publication remains
fresh-only and settled replay remains committed-only. A lost diagnostic does
not prove a cause and cannot substitute for current recovery evidence.

`preflight_original_medical_recovery_local(config, supervision=...)` and the
installed `preflight-original-medical --configuration PATH` select the same exact
`OriginalMedicalRecoveryConfig`, original request and existing authority. The
selected native deadline MUST be at most 60 seconds. They MUST perform complete
recovery preflight and independent closing, but MUST NOT construct the publisher,
apply or reconcile remaining effects, append source data or submit an ACK.
A successful result uses `ashlar-original-medical-local-recovery-preflight/0.1`,
retains installation/request correspondence and reports `preflight_completed`,
`closing` and bounded `stage_records`; it MUST NOT contain a publication or ACK
success claim. Deadline, refusal or incomplete cleanup MUST withhold success.

`inspect-original-medical-commit --configuration PATH` selects the additive
`ashlar-original-medical-local-commit-inspection/0.1` configuration. Its exact
members are `profile`, `recovery`, `original_configurations` and
`process_observer`. `recovery` contains the complete existing recovery
configuration, including its unchanged finite worker bounds and a fresh private
output. `original_configurations` contains 1–32 pinned original configuration
files, each with `path`, `bytes` and `sha256`. `process_observer` contains exactly
`ps`, `java`, `owner_uid`, `visibility_profile`, `maximum_processes`,
`maximum_output_bytes` and `timeout_seconds`. `owner_uid` MUST equal the actual
local owner UID; `visibility_profile` MUST be
`ashlar-private-local-same-uid-process-visibility/0.1`.
`ps` and `java` are pinned files; the Java identity MUST match the selected and
retained original configurations. Process bounds MUST be positive integers no
greater than 4096 processes, 1,048,576 output bytes and five seconds.

The inspector MUST acquire the existing installation lease, perform complete
original recovery preflight and independently validate the retained proposed
manifest, actual native phase/artifact correspondence, full publication vector,
source and protected ACK scope. It MUST NOT initialize, construct a publisher,
prepare or submit a mutation, repair original custody, append source data or
submit ACK. Independent closing MUST precede release of the exact proposed
manifest bytes and bounded observation report.

On its supported Darwin private, same-user profile, the inspector MUST observe
current processes before native acquisition and at closing. It MUST inspect the
physical executable identity of every visible process with the original local
owner UID, rather than discover candidate executors only from process titles or
arguments. Other-UID processes are outside this declared profile. Caller booleans,
missing PIDs or timeout receipts MUST NOT substitute for image observation. External matching medical workers or the selected JVM,
unreadable or raced candidate identities, incomplete inventory and unsupported
observation profiles MUST refuse. Closing MAY exclude only the inspector's
independently owned process lineage. Unrelated processes require an independently
observed different executable identity; they MUST NOT be stopped.

A process that exits during observation MAY be recorded as terminal only when
the native image lookup returns ESRCH and an independent zero-signal kernel
existence check also returns ESRCH. A permission error, ENOENT, missing row alone
or failed query MUST NOT establish terminality. The closing process inventory
MUST omit that PID and a repeated zero-signal check MUST still return ESRCH;
PID reuse MUST refuse. A new same-owner process MUST undergo the complete
image/mapping or terminal observation and an independent closing inventory
before observation can succeed; it MUST NOT be silently omitted. Closure may
repeat only within the original process-count and overall time bounds. This confirms an exited local
process only. It grants no remote-operation terminality or future-writer fence.

Argument text is an auxiliary classifier, not physical process identity. If only
arguments change while PID, UID, parent, process group and start time remain
unchanged, the observer MAY reclassify the new arguments only after independently
proving the same physical image or exact mapped kernel start identity. It MUST
apply every original executor/medical-worker/path guard again and obtain an
independent closing inventory. Changed image or kernel identity MUST refuse.
Repeated argument changes consume the original overall deadline and process
bound; they MUST NOT restart either budget.

The separately selected visibility profile
`ashlar-private-local-same-uid-mapped-vnode-visibility/0.1` adds exactly one
required process field, `mapped_vnode_helper`, with the existing pinned-file
shape. The strict visibility profile MUST reject that field. The mapped profile
retains primary executable observation and MAY use the helper only when
`proc_pidpath` returns ENOENT. Other errors and process races MUST refuse.
This profile assumes cooperative, unmodified original Java/Python runtimes
retain file-backed executable mappings. Anonymous mappings remain unknown;
this profile does not establish primary executable identity for a fallback PID.

The selected helper MUST be an explicitly built, pinned Darwin executable;
command execution MUST NOT discover or invoke a compiler. It consumes canonical
unsigned decimal arguments in order: PID, owner UID, maximum regions, remaining
milliseconds, then one or more device/inode pairs. PID is positive, UID is
32-bit, region count is 1–4096, remaining time is 1–5000 milliseconds, and
1–33 selected pairs cover the original Java and all available original Python
executable identities. Device and inode values are unsigned 64-bit integers.
It emits one JSON object under the existing aggregate stream budget, with exactly
`profile`, `pid`, `owner_uid`, `start_seconds`, `start_microseconds`, `regions`,
`file_backed_regions`, `executable_file_regions`, `no_vnode_regions`,
`selected_vnode_matches`, `terminal_errno`, `stable_bsd_identity`.
The profile is `ashlar-darwin-mapped-vnode-observation/0.1`; counts and start
seconds are nonnegative integers, start microseconds are below 1,000,000,
`regions` is positive and within the selected bound, and the two file-backed
subcounts cannot exceed their containing counts. File-backed and no-vnode
counts MUST sum to regions. Success requires exit zero, matching selected PID
and UID, native terminal EINVAL (22), stable kernel PID/UID/start identity and
zero selected vnode matches. The helper PID/UID/start time MUST correspond to
the same process-inventory row, and its microsecond start identity MUST remain
unchanged at closing. Complete native structure sizes, monotonic region
addresses and unchanged process identity before/after enumeration are required.
Permission errors, disappeared processes, partial structures, bounds, deadline
and malformed or extra output MUST refuse. The caller independently renews
helper bytes and selected identities, repeats observations at closing, and
retains fallback identity and counts with the declared assumptions.

An `exact-existing-commit` observation requires complete native manifest
correspondence and validation of its actual retained original intent and receipt
through the existing committed-operation owner. An exact native row with missing
original intent MUST instead use
`observed-exact-manifest-row-no-original-intent`; matching row values or plausible
commit metadata MUST NOT promote it to original submission custody. An `observed-absent-at-anchor` observation requires the
unchanged original manifest identity/history/anchor and the separately selected
owner's complete current admission. Process profiles require no matching executor
in the observed intervals. The journal profile requires its conditional
write-ahead lineage and actual cooperating-writer lease, without asserting
process absence. Each describes its bounded private development assumptions;
neither may claim that arbitrary future writers are fenced. Neither observation, report hash,
proposed manifest nor process absence grants a new submission or ACK. Missing
intent remains a reconciliation case, and ordinary recovery MUST retain its
original refusal. A prospective reconciled submission requires a separately
specified authorization boundary.

Inspection opening and closing compare complete native history using exact
native types, timestamp timezone and fold. JSON reports preserve every history
field and encode a native timestamp as
`{profile: "ashlar-native-history-timestamp/0.1", native_type: "TIMESTAMP",
session_timezone: "UTC", python_iso8601, python_tzinfo, fold}`.
`python_iso8601` retains six fractional digits; `python_tzinfo` is null for a
naive Python value or `"UTC"` for the explicitly UTC carrier, and `fold` is 0 or
1. Unsupported carriers MUST refuse. Report encoding MUST NOT replace the
original values used for native comparisons or infer a timezone for a naive
value. Prospective correspondence uses the same deterministic report encoding.

The same `inspect-original-medical-commit` command MAY separately select
`ashlar-original-medical-local-journal-inspection/0.1`. Its exact configuration
members are `profile`, `recovery` and `lineage`. `recovery` is the existing closed
recovery configuration with fresh output; `lineage` is a pinned-file selection.
This profile observes retained write-ahead custody under the actual installation
lease. It MUST NOT produce a process-absence observation or require laptop-wide
process quiescence. Existing process profiles retain their behavior.
Held journal renewal and closing MUST finish before the actual lease is released,
including failure and cancellation paths. After release, final report admission
renews only the exact selected files; it MUST NOT reuse or renew released lease
authority. Released leases continue to refuse held operations.

The lineage document has exactly `profile`, `installation_id`, `request_digest`,
`installation`, `snapshots`, `executions` and `storage_profile`. The profile is
`ashlar-original-medical-journal-lineage/0.1`; installation ID is a canonical UUID,
request digest is lowercase SHA-256, installation is the exact pinned original
handoff, and storage profile is
`ashlar-private-exclusive-original-journal-no-out-of-band-rollback/0.1`.
`snapshots` contains 2–16 ordered pinned complete SQLite backup files, each at
most 128 MiB, with aggregate snapshot bytes within the selected journal budget.
`executions` contains 1–16 exact records with `configuration`,
`terminal`, `code` and `launcher`. The first two fields are pinned original files; `code`
contains exactly `transport`, `driver`, `capture` and `lease`, each a pinned
installed module. The configuration selected by the terminal invocation MUST
correspond to that execution; original source, installation, journal and six
native target identities MUST agree across the known executions. Initialization
records do not replace complete publication/recovery execution custody.
`launcher` is null for direct installed CLI invocations. The separately retained
original diagnostic route MAY select its pinned 698-byte launcher with SHA-256
`92a6b22ad2fb95a4d22e602c69969e70d35eed314a298e0dd391908cbde160fb`.
Its exact terminal argv MUST invoke that script, and its selected configuration
MUST be the sibling `configuration.json` consumed by that reviewed public
recovery call. Other script launchers MUST refuse; terminal files remain
unchanged and MUST NOT be relabeled as direct CLI invocations.

Only the independently reviewed original transport digests
`0261d6c09783b15850591206ae373b288b879e0a212a9fc9cc12aea7085aabbc`,
`3f4225eb773cf6baaa6df87f09ab92f190eb6ac03e733e85e6d40cbdecceda5b`
and `b2944f04bbb4d69f4c25824455b62ffd57e902c8e0a7b34671cc63ffcd63141a`
qualify this version-specific write-ahead premise. Driver, capture and lease
must respectively match
`713f7844233e380f1a0c29c57e176c13ff11bbdc7387c8f87f694bf7565fce3e`,
`64e23a9c8a1e440a084a38303ae8a80dd387b8c971d3d4b99819c03e306e9d03`
and `36c2133874ef4952b302dc255053627b7037c9569b7888ccb0035f0b865dcaff`.
Their durable prepared/submitted transactions precede native SQL, operation
records are retained, cooperating writers hold the installation lease, and
supervised terminal cleanup kills and reaps the owned group. Arbitrary pinned
code, a status flag or current missing intent alone MUST NOT establish this
premise. Terminal evidence MUST contain a terminal state, actual integer exit
status and exact original configured invocation; a timeout label alone is
insufficient.

Under the real lease, the observer MUST bind current journal file identity,
verify complete SQLite schemas/registry and every retained snapshot's operation,
plan, source-plan and artifact inventories, then compare their monotonic prefix.
Prior intent bytes, request/source/effect plans and committed receipts MUST NOT
change or disappear. Only prepared-to-submitted-to-committed progression with
its original receipt is permitted. Every operation in the final baseline MUST be committed;
original manifest intent MUST be absent. The current journal MUST preserve the
complete last baseline. During reconciliation it MAY extend that baseline only
with the same request's explicitly authorized prospective manifest intent and
existing committed-attempt append; unexplained new effects, another request,
conflicting intent, missing rows, unknown schema or registry drift MUST refuse.
All checks repeat at closing while current identity and lease remain held.

This profile assumes exclusive original storage and a complete known execution
inventory, with no out-of-band record deletion, restore or rollback. Endpoint
snapshots do not independently prove those historical assumptions or recover
lost journal custody. No historical inode assertion may be synthesized. Current
native receipt/history, complete medical artifact/phase/vector, source and ACK
checks remain independently mandatory after Spark acquisition and before any
new intent. Journal observations are retained under `journal_observations`;
their profile-specific validator grants no native, publication or ACK authority.
A durable local SQL intent that never existed cannot have reached an orphan JVM
through the selected write-ahead routes. This conditional observation does not
qualify unjournaled writers, remote services or production authority.

`reconcile-original-medical-commit --configuration PATH` selects the separate
`ashlar-original-medical-prospective-manifest/0.1` configuration. Exact members
are `profile`, `inspection_configuration`, `inspection`, `proposal`,
`authorization` and `output`. The four file selections use the existing pinned
`path`/`bytes`/`sha256` shape. `output` is a fresh private absolute output path.
The pinned inspection configuration remains the single owner of all runtime,
source, request, installation and observer selections; only the explicitly
selected fresh output replaces its already-used output for the new operation.
Parent and worker MUST independently renew the complete wrapper and pins.

The authorization document has exactly `profile`, `authorization_id`,
`installation_id`, `request_digest`, `applied_artifact_sha256`,
`proposal_sha256`, `inspection_sha256`, `inspection_configuration_sha256`,
`manifest_uuid` and `anchor_version`. The profile is
`ashlar-original-medical-prospective-manifest-authorization/0.1`;
`authorization_id` and `installation_id` are canonical UUID strings, digest
fields are lowercase SHA-256 strings, `manifest_uuid` is the selected original
UUID and `anchor_version` is integer 1 for this private profile. The selected
authorization expresses permission for this one prospective operation under
the declared local development authority. An inspection report, boolean,
matching row or timeout alone MUST NOT authorize it.

The original inspected outcome MUST be `observed-absent-at-anchor`. Before any
new intent, the operation MUST hold the real installation lease, renew the
selected process observer, and establish current original source/head, protected
ACK zero, full applied artifact/phase/effect correspondence and unchanged empty
manifest identity/schema/history anchor. The original inspection configuration,
report, applied artifact and proposal MUST match every selected authorization
digest. Original graph effects MUST NOT be reapplied. A conflicting existing
ordinary manifest intent, foreign native version or incomplete admission MUST
refuse. The exact authorization bytes MUST be exclusively retained and fsynced,
including their parent directory, before any new native submission intent.
One immutable authorization sidecar per installation/request MUST exclude a
second authorization for the same uncertain submission; the existing journal
schema MUST remain unchanged.

SHA-256 of exact authorization bytes identifies the new operation prefix
`reconciled-manifest:<request_digest>:<authorization_sha256>`. The existing
SQL/parameter child digest completes its identity. The original manifest SQL
owner MUST generate the statement from the unchanged proposed row; a separately
retained prospective intent MUST precede its submission. Existing `recover`
semantics MUST continue to refuse the missing historical manifest intent.
An explicit stored-publisher `reconcile_commit` port may transition only the
original `committing` attempt with its exact retained applied artifact to
`committed`, after current validation and the prospective owner's exact native
receipt/manifest checks. It MUST NOT implement a second phase append protocol.
The prospective helper MUST use the publisher's already held original writer
interval and refuse calls outside it; it MUST NOT reacquire that exclusive writer.

Uncertain replies resume only the same sidecar authorization, new operation
identity and original retained SQL/parameters. Submitted and committed intents
MUST NOT pass through fresh preparation. A completed native commit may be
reconciled through its original new receipt before the existing attempt owner
records `committed`. Subsequent protected ACK uses the existing original request,
descriptor, scope and source owner. Post-commit or post-ACK continuation validates
that exact prospective receipt and original phase/source/ACK custody; it MUST NOT
require the fresh-only ACK-zero/empty-manifest condition or submit another
manifest. All opening/body/closing failures preserve their primary failure and
withhold success output. Cancellation does not establish rollback. Disabling
this additive command leaves original and prospective custody intact. A successful
receipt uses the prospective configuration profile, the existing complete medical
publication observation and `authorization_sha256` identifying its exact selected
authorization bytes.

The private output retains exclusive `preflight-stage-NNN.json` records before
and after each checked stage, with at most 128 records and 1024 bytes per record.
`ashlar-medical-preflight-stage/0.1` has `sequence` (integer 0–127), `stage` and `phase`
(`begin` or `end`); optional `role` selects one of the six existing roles,
`ordinal` is an integer 0–31, and end records carry nonnegative monotonic
`duration_ns` below 2^63 and `outcome` (`completed` or `refused`); begin records
MUST omit duration and outcome. Closed stages are
`runtime-custody`, `source-admission`, `source-plan`, `prior-capture`,
`ordinal-state`, `native-target`, `committed-ordinal`, `source-closing`,
`ack-scope`, `binding`, `composition-closing` and `runtime-closing`.
The driver preflight MAY accept keyword `stages`, either `None` or the exact
`MedicalPreflightTiming` owner from `medical_preflight_timing.py`; it MUST NOT
accept a caller-supplied replacement for a native or authority check.
Records MUST exclude paths, SQL, source values, credentials and exception text.
Capacity or capture failure MUST preserve the original failure and all required
closing. A retained begin without end identifies incomplete observation, not a
proven timeout cause. This local projection grants no authority and claims no
OpenTelemetry receiver integration; CONTRACT-006 privacy remains applicable.

After acquiring its private output, a failed medical operation SHOULD retain
`failure-location.json` through the same output owner. The closed profile
`ashlar-original-medical-failure-location/0.1` contains `exception` (the nearest
built-in exception category) and `frames` (at most 64 owned-code locations).
Each location contains only `module`, `function` and positive integer `line`;
the complete UTF-8 record MUST fit 8192 bytes. Frames are diagnostic locations,
not exception messages or source excerpts. Paths, local variables, source values,
SQL, credentials and chained exception payloads MUST be excluded. Failure to
retain this passive record MUST preserve the primary error and run all required
closing callbacks. An unacquired output MUST NOT create a diagnostic file.
The record grants no publication, recovery or ACK authority.

The lower `publication_planning.py` module owns the pure
`request_for(stream, transaction, predecessor, revisions)` and
`local_effect_plan(generated, prior_rows, tables)` surfaces. The driver MUST retain
compatible re-exports. Request construction preserves the complete public outbox
checkpoint, batch and deterministic request digest. Effect selection may elide
only the existing closed generated zero-match DELETE with complete prior-row
correspondence. These functions grant no source, writer, phase or ACK authority
and MUST NOT depend on the native driver or command composition.

`MedicalPublicationPlan` MUST bind an exact `MedicalAdmission`, its complete
original `OutboxTransaction`, deterministic request and independent four-role
oracle. Construction and renewal MUST preserve the original feed/epoch,
checkpoint, declared schema revisions and generated effect correspondence;
caller-supplied expected bags or a commerce admission cannot confer this grant.

Its constructor is `MedicalPublicationPlan(admission, transaction, tables)`, with
an exact six-role mapping to `local.original_medical.<role>`. Read-only `admission`
and `transaction` properties preserve construction identities. `renew()` MUST
recompute complete original correspondence; `require_admission(actual)` MUST
require that same admission object, and `require_request(request)` MUST require
the exact original primitive request. Both renew before accepting. `snapshot()`
MUST renew and return a detached complete JSON view with exactly these members:
`profile`, `request`, `publication_id`, `materialized_at`, `recorded_at`, `tables`,
`columns`, `schemas`, `generated_provenance_sha256`, `source_admission`,
`generated_steps`, `selected_steps`, `zero_match_elisions`, `previous_expected`,
`expected`, `previous_progress` and `progress`. `schemas` retains all four checked
generated `schemaJson` values; mutating a returned view MUST NOT change the plan.
The original profile fixes stream `private-original-medical`, predecessor
`explicit-original-medical-origin`, publication `original-medical-publication-1`
and materialization time `2026-10-09T12:00:00+00:00`; these development selections
cannot confer accepted Truss or production identity.

`NativeDriver.bind_original_medical_publication(plan, bounds=bounds)` MUST accept
one exact `MedicalPublicationPlan` and exact `VerificationBounds` before writer
or pin acquisition, only on an exact existing `NativeDriver` with registered
ordinary source/ACK owners and `LocalDeltaTransport`. It MUST require the driver's
construction-bound medical admission, all six original target bindings, current
original admission metadata and the plan's complete active request/effect/oracle
correspondence. It MUST retain the same plan and bounds objects and their original
complete views. A query-bound driver, duplicate binding, foreign profile or
subsequent replacement MUST refuse. Binding grants no additional authority and
cannot replace installation, source, native retention or publisher guards.

`NativeDriver.observe_original_medical_publication(plan, descriptor,
context=context, bounds=bounds)` MUST require that exact construction-bound plan
and admission, a held native writer and matching pins throughout capture,
complete original descriptor/vector
and all six current registry bindings. `bounds` MUST be exact `VerificationBounds`
selected within the owning medical configuration's limits. The observation MUST
compare all four selected immutable-version bags to the independent medical
oracle and their complete schemas to checked UMF-generated carrier schemas,
including field order, types, nullability and metadata. Bounds apply before
transfer, cumulatively across all four roles. Identity-only snapshots, row counts
or inferred schemas cannot satisfy this check.

The medical publication adapter MUST require this observation before delegating
protected ACK to the shared publisher. The actual protected ACK publication
admission MUST renew the same observation within its acquired pin interval;
an earlier adapter check cannot substitute for that current check. Observation
MUST reuse an already held matching pin interval or acquire and close one under
the held writer, never nest or bypass the shared pin gate. Settled replay MUST
perform the same
observation, including when no ACK needs submission. Each acquired role MUST
receive an independent closing observation; all six current UUID/location and
retention facts and ordinary source/ACK custody MUST renew before release even
after failure or cancellation. No observation may be cached across intervals or
used to grant source append, phase resubmission or journal repair.

`OriginalMedicalSourceOwner.read_original(batch, context=context)` MUST require
its exact construction-bound medical batch and current installation/source
authority, then independently read the complete original head-one transaction
through the registered ordinary reader. Head zero, later progress or different
bytes MUST refuse. Reader and ACK authority MUST renew independently at closing;
this port MUST NOT open a writer connection or call append. Settled replay uses
this port, never the append/reply-loss recovery path.

`OriginalMedicalSourceOwner.renew_original(batch, context=context)` MUST attempt
the captured original ordinary reader and ACK authority independently, including
when the current registration, factories or bound owner have drifted. It MUST
refuse that drift without selecting a replacement login. The reader MUST compare
the complete original head-one transaction; ACK renewal MUST verify the original
scope, signature and ordinary privileges. Each acquired connection MUST receive
independent closing observations, rollback and close with primary-preserving
cleanup. This closing port MUST NOT append or submit ACKs; it grants no authority
to continue a failed publication.

`MedicalReadSourceFactory` MUST accept only an exact
`OutboxAuthorityRegistration` for the historical medical feed/epoch and preserve
its reader/ACK connection factories. `NativeDriver.bind_query_sources(factory)`,
`query_read_interval(context)` and `renew_original_graph_read_source(vector,
context=context)` MUST admit this separately owned type only with an exact
`MedicalAdmission` and its original medical admission profile. Commerce bindings
retain their own admission boundary. No profile relabeling, caller-selected
admission callback or broader provider discovery is permitted.

Medical binding MUST retain the shared reader's complete six-target registry,
journal/reservation identity, original request/manifest, four-role publication
vector, actual exclusion and pins, current ordinary source/ACK admission and
independent native retention observations. Closing MUST attempt each acquired
native/source/ACK interval before releasing authority even after earlier failure.
Binding grants read custody only; it does not grant medical Weft or engine-query
support or enable any mutation path.

Each command MUST own finite separate-process supervision/descendant cleanup,
with an explicit integer native deadline of 1–900 seconds, an integer whole
deadline greater than the native deadline and at most 960 seconds, and
local[1]/512MiB for this small functional profile. Bounds
hold before buffering/serialization; operation-intent capacity is not journal-file
capacity. Every acquired interval independently attempts required current closing
on opening/body failure, timeout and cancellation while authority remains held,
then releases resources with primary-error precedence. Successful closing/cleanup
precede bounded evidence; retained incomplete state is not rollback. CONTRACT-006
owns privacy. [TD-005](../technical-designs/TD-005-original-medical-local-operator.md)
owns composition and MED-F1–F4/MED-L1. No medical query/engine, managed-UC or real
Truss qualification is implied; original eight-publication recovery remains due.

`medical_process_supervision.py` owns `OriginalMedicalNativePhase`, the exact
worker-side owner of an explicitly inherited writable pipe descriptor. Its
`begin(custody)` and `end(custody)` require the same actual
`OriginalMedicalRuntimeCustody` and unchanged descriptor/bounds; the owner is
single-use. The private channel carries only ASCII `native.begin\n` followed by
`native.end\n`, at most 32 bytes total. Unknown, duplicate, reversed, truncated or
missing messages MUST refuse a successful invocation. The parent MUST enforce
the configured whole deadline from launch and configured native deadline from begin,
through session/context cleanup. Closing the pipe or exiting does not replace end.

The four typed medical host entries MAY accept keyword `supervision`, and
`original_medical_native_session` MAY accept keyword `phase`; each is either the
exact `OriginalMedicalNativePhase` or `None`. Installed commands MUST inject their
actual phase owner through both boundaries. `None` retains the externally
supervised lower-port contract and MUST NOT establish an installed-command
supervision claim. Begin MUST precede native session acquisition/imports, and end
MUST follow successful native cleanup. This channel carries no source/native/ACK
authority, credentials or configuration values; it is separate from diagnostics
and does not alter any other workflow's deadline.

`medical_worker_capture.capture_original_medical_worker` owns parent capture of
the explicitly selected worker argv, directory, closed environment and exact
`OriginalMedicalBounds`. It MUST enforce both deadlines, independently cap stdout
and stderr, and validate phase and canonical worker exit observations. A retained
session leader MUST preserve process-group ownership after worker exit; every
outcome MUST kill and reap the owned leader and terminate surviving descendants.
The parent MUST renew selected custody and refuse changed bounds, preserve primary
cancellation, and close its own pipes. Worker capture is a transport obligation,
not source, native retention or publication acceptance.
Failure MUST retain bounded original diagnostic prefixes and observed phase/status
on the unchanged primary exception as `medical_capture`, with `complete: false`.
This private carrier MUST NOT automatically log its bytes or claim that truncated
prefixes are complete output. Final custody renewal MUST run on every outcome.

The original medical applied artifact MAY use effects profile
`ashlar-original-medical-effect-proof/0.1`. Its closed source-plan reference MUST
contain profile `ashlar-original-medical-source-plan-reference/0.1`, the original
request digest, UTF-8 byte length and SHA256 of the complete existing
`local_source_plan.original`. It replaces only duplicated generated SQL and prior
native anchors in the effects object. The complete source-plan row MUST retain
the original request, generated and selected effects, elisions, complete prior
oracle and observed prior anchors. The effects object MUST retain the original
ordered effect-plan digest, intent digest, compact ordinal receipt arrays,
elisions and source-plan digest; the complete manifest MUST remain unchanged.

The actual bound medical driver MUST resolve exactly one original source-plan row
and compare its full contents, reference length/hash and request with the current
original `MedicalPublicationPlan`. It MUST validate the retained ordered effect
plan through `LocalDeltaEffects.original_plan` and `observe`, require every ordinal
committed, and inspect each original operation through
`LocalDeltaTransport.inspect_committed`. The returned compact native receipt
fields MUST equal the artifact's ordinal receipt arrays. Complete native receipts
and source/effect plans MUST remain in their original journal rows. Missing or
changed rows, native commits, schemas, snapshots or admission MUST refuse; hashes
alone MUST NOT establish retention or native acceptance. Independent closing MUST
recheck the original rows, plan and current native/source/ACK/lease authority even
after failure or cancellation. No reconstruction, repair or cached authorization
is permitted. Other profiles and shared publisher/phase envelopes remain unchanged.

`OriginalMedicalBounds.journal_bytes` MUST bound the canonical SQLite main-file
size before opening and on every installation renewal, separately from per-intent
and native capture limits. Its maximum MUST equal the existing carrier ceiling of
128MiB; a caller's explicitly selected lower budget MUST still be enforced. This
main-file bound MUST NOT be reported as a bound on WAL files or total disk use.
No invocation may automatically enlarge a selected budget or retry under a larger
one. Fresh configurations MUST select a sufficient finite budget before work;
exceeding it retains incomplete state and refuses further mutation/ACK acceptance.

## Truss destination receipt acknowledgement

A Truss source session MAY explicitly select destination receipt acknowledgement.
Selection MUST require the public receipt ACK and read-only reconciliation APIs;
missing capabilities MUST refuse without falling back to plain ACK. Existing plain
ACK sessions retain their delivery assertion semantics and MUST NOT acquire receipt
proof retrospectively. Persisted attempt identity MUST distinguish these modes.

Before receipt ACK, Ashlar MUST independently resolve and validate the complete
durable destination publication under its original source owner and native pins.
The bounded opaque receipt MUST retain the exact request, manifest and full version
vector. Its source binding MUST match installation, source epoch, consumer,
registration, prior/next cursors and feed digest; its destination binding MUST match
the explicit destination identity, request digest, publication ID, receipt bytes
and receipt digest. Truss retention proves source-side receipt custody; Ashlar
continues to own destination durability verification.

The caller MUST commit the receipt ACK transaction before claiming advancement.
After an uncertain commit, a fresh read-only reconciliation MUST compare every
binding and the exact original bytes. Only a matched receipt can establish the
original source ACK outcome, including after later checkpoint advances. Missing
receipts remain unresolved; conflicting receipts MUST refuse. Neither outcome
licenses a second ACK write, republishing or checkpoint-only inference. Final
success still requires current destination publication admission and resource
closure.

Receipt transport MUST respect the source SDK's one-MiB opaque byte limit.
Previously admitted small receipts MUST retain their exact original encoding.
An oversized complete receipt MAY use a versioned lossless compressed envelope
that declares its encoding, complete original byte count and digest. Compression
MUST retain every original byte, including unknown content and the full vector;
neither truncation nor a digest-only substitute is permitted. Both original and
encoded lengths MUST be bounded, and encoded overflow MUST refuse before ACK.

### Complete Truss source coverage without entity changes

For the selected `truss-native-feed/0.1` source profile, an original public
`empty_interval` page with a null writer and no catalog, journal, source or
reservation members MAY advance the source cursor without changing any graph
row. Ashlar MUST retain its complete original page bytes, digest, prior and next
cursors, consumer, registration, installation and epoch. The next cursor MUST
equal the page's safe frontier and follow the prior cursor within that same
registration. An unchanged prior/next cursor is only a poll result; it MUST NOT
create a publication or source ACK.

An advancing empty interval MUST use the same predecessor, immutable publication
manifest, complete table-version vector, durable source-plan, replay and
publication-before-ACK gates as an entity transaction. Its graph table versions
may remain identical while its published source progress changes. The schema
revision vector MUST carry forward the admitted predecessor; an empty page
cannot establish new catalog meaning. Recovery MUST reproduce the exact original
page and request, including an uncertain publication or ACK outcome. A later
transaction MUST begin at the published next cursor. No source ACK, delivery
observation or wall-clock timestamp alone establishes destination visibility.

## Configured typed immutable-file publication

The local configured lifecycle MAY consume the separately versioned
`ashlar-jsonl-typed-source/0.1` profile. Its closed configuration MUST retain
`feed`, `epoch`, `sourceSystem`, `schemaAlias`, `documentRevision`,
`producerRevision` and exact `source`, `schema` and `bindings` file references.
Original authored UMF core 0.8 bytes and explicit development ID bindings MUST
remain distinct from STRING/core 0.7 intake and accepted Truss catalog authority.

Before native setup, a bounded selected clean public UMF producer MUST validate
every carried scalar Record payload using `validateCoreRecordValues`. Exact
integer/decimal tokens, booleans, optional absence/null/value and unknown retained
content MUST survive transport, history, restart and replay unchanged. All events,
including deletes, MUST carry complete scalar Record values in this first profile.
UMF validates that carried payload; core apply separately owns the delete
transition and tombstone. Empty delete payloads, target-existence proof, dataset
keys and relationships require separate admission.

The minted local source MUST bind original configuration/model/source/binding
bytes, complete original requests, receipts and checkpoint/predecessor custody.
Receipt or file drift MUST refuse. Initialization, publication, replay and raw
singleton reads MUST use the same native admission, original operation journals,
complete source-prefix carrier checks and closing rules as the configured local
lifecycle. Numeric properties MUST remain exact opaque JSON text in canonical
carriers; this profile MUST NOT imply numeric Weft query qualification. Existing
STRING request/registry/progress correspondence MUST remain unchanged. Local
consumer progress MUST NOT be represented as protected producer ACK or remote
writer fencing.

### Configured typed graph source

The separate `ashlar-jsonl-typed-graph-source/0.1` profile MUST pin original core
0.8 model, complete JSONL source and explicit development binding bytes, selecting
public UMF producer `07357eadbda9cf8299b9d72d004cbb77f8091aee` independently of the
scalar profile. Record and relationship mappings MUST retain qualified logical
identities and separate object/edge storage namespaces. Singleton type admission
MUST enumerate object types only. Development mappings grant no accepted catalog,
protected source ACK or native writer authority.

Each of at most four complete source groups MUST contain each typed entity
identity at most once. The first relationship subset admits directed monomorphic
independent-lifecycle declarations with explicitly bound target Keys. Nonempty
edge properties MUST refuse; opaque retained content MUST remain unchanged.
Object scalar tokens and absence/null/value states MUST retain their original
carriers, including complete carried payloads on deletes.

Before publication planning, a fresh public producer MUST validate every carried
object Record and validate plus independently verify each proposed complete
post-group dataset using the public compact dataset API. Dataset scope MUST be
`supplied-dataset-only`. Original per-Record key/relationship-context warnings
MUST remain retained. Only the exact model-derived context diagnostics owned by
this public producer MAY be discharged by the joint complete dataset receipt;
every carried Field check MUST independently be valid and complete. Unknown
warnings or unresolved joint obligations MUST refuse, without fabricating a
complete per-Record result. Receipt custody MUST bind the exact ordered original
control/event bytes, byte cursors, source/configuration/binding/model digests,
model revision and each independently derived complete proposed dataset. Target
resolution, declared keys and relationship multiplicities belong to UMF; a
successful scalar check alone MUST NOT establish them. Independent edge instance
IDs MUST preserve parallel occurrences and self-loops.

Proposed datasets are transport inputs, not accepted graph states. They MUST NOT
be constructed through a permissive schema-policy substitute. After authentic
receipt attachment, the ordinary core apply/replay planner MUST check versions,
original deliveries, prior-state replacement/deletion correspondence and complete
transaction endpoint integrity. Deleted edges MUST retain their exact prior
payload; their prior admitted dataset supplies schema meaning. A failed semantic,
mechanical or closing check MUST release no publication or progress result.

The shared local publication/read lifecycle MAY consume only this freshly admitted
source and original receipts. It MUST retain complete native carrier parity,
publication vectors, source progress and closing custody. Relationship source
admission alone MUST NOT enable the scalar-only query binding or grant graph-query,
remote fencing, schema migration or protected ACK authority.

### Proposed Spark-task receipt verification amendment (TD-009)

Draft proposal only: `ashlar-managed-current-spark-receipt-read/0.1` MAY verify
one bounded managed-current publication in a fresh verifier task/session after
its publisher task terminates successfully on the same explicitly dedicated
Jobs cluster. This proposal grants no launch, runtime or production qualification.
The SQL receipt-reader profile remains current-channel serverless-warehouse-only.

The verifier MUST independently reopen durable UC state, exact original request
and manifest, committed attempt/effect custody, complete source/epoch/checkpoint
and both progress entries, schema revisions, full four-role UUID/version vector,
destination receipt and compiler-binding inputs. It MUST compare complete bounded
versioned logical bags against an independently authored source-derived prefix
oracle, preserving exact values, presence/null, numeric tokens, source identity,
parallel-edge multiplicity, history and tombstones. Publisher objects, caches,
temporary views, DataFrames, task flags and result files MUST NOT supply proof.

Only the original external source holder MAY ACK, after observing terminal durable
verifier success and independently admitting the exact receipt under a live,
protected source/read/pin interval through ACK commit, reconciliation and closing.
A completed verifier report MUST NOT substitute for this live owner. Until a
reviewed renewal handoff exists, this path MUST remain unavailable for campaign
ACK. The global A1,B1,A2,B2,A3,B3,A4,B4 sequence and ACK-before-next-slot mutation
rule remain unchanged. Drift, overflow/truncation, unknown protocol, cancellation,
uncertain native outcome or incomplete closing MUST withhold success and ACK;
original operation/recovery custody MUST survive. Fresh-session separation MUST
NOT imply independent-principal security or source authority.

#### Proposed source-holder custody ports

The following interface is a draft amendment for review; it does not change this
contract's approval state or enable campaign use. The external holder MUST keep
three independently admitted live custody handles plus its protected ACK
transaction port under one source-owned interval:

- `ManagedPublicationReadPort.open(receipt_bytes, vector, context)` returns a live
  handle whose `renew()` reopens the exact managed publication/runtime identity,
  complete six-role installation and four-role pinned snapshots, and verifies
  that each pinned version remains readable. Its `close()` performs the same
  substantive checks independently. `receipt_bytes` are the exact retained
  destination-receipt bytes; `vector` is the complete `PinVector`.
- `NativePinPort.open(vector, source_selection, context)` returns a live pin
  handle for the complete four-role UUID/version vector. Its `renew()` asserts
  that the original pin remains active and its retention admission still covers
  the source and publication; its `close()` rechecks both. A `PinVector` value
  or registration row alone MUST NOT count as a held pin or retained-file proof.
- `TrussSourceCustodyPort.open(source_selection, request_bytes, context)`
  returns a live handle whose `renew()` admits the original registered feed,
  epoch, consumer, checkpoint, destination registration and ACK scope from
  separately retained registration and cursor evidence. Its `close()` rereads
  that exact source custody.
- `TrussProtectedReceiptAckPort` is the source holder's existing protected ACK
  transaction owner. Its
  `commit(request_bytes, manifest_bytes, receipt_bytes, context)` accepts only
  exact original bytes and commits once. Its
  `reconcile(request_bytes, manifest_bytes, receipt_bytes, context)` reads back
  exact ACK correspondence. It MUST NOT accept a verifier Boolean or report in
  place of those bytes.

The source holder MUST open all three read/pin/source handles before ACK
admission. It MUST establish that their live renewal interval covers the bounded
ACK and reconciliation window, or keep an independent renewal owner active
throughout; an expiring or unrenewable handle refuses admission. It MUST renew
all three immediately before commit, then commit and reconcile
the protected ACK while all handles remain live. It MUST attempt all three
closes on every exit, including cancellation, deadline and body failure, while
preserving the primary failure. A close or reconciliation failure after commit
leaves the slot incomplete; if exact reconciliation failed, ACK settlement is
also uncertain. Retain the original operation and receipt custody, report no
slot success, authorize no next-slot source mutation, and do not blindly
resubmit.
Only successful exact ACK readback and all three independent closes complete the
slot. Jobs publisher/verifier tasks receive neither the source ACK credential
nor an authority to renew or synthesize these handles.

#### `ashlar-managed-operational-retention/0.1` bounded A1 profile

This profile is selected only when explicitly named by the original request,
destination receipt, verifier observation and source-holder operation. It
applies to one source slot, one exact original four-role `(name, UUID, version)`
vector, and one ACK interval of at most 900 seconds. It does not alter the
stronger retention-backed profiles or their admission rules. It is an
operational availability profile, not a retention lease or a claim that files
are continuously reserved.

The source holder MUST independently retain and select the exact original
request, accepted source-prefix bytes and digest, manifest bytes and digest,
destination receipt bytes, complete four-role vector, source-derived expected
bags, and source registration/cursor/ACK-scope custody before publication. A
verifier observation or digest alone MUST NOT select these anchors. The holder
MUST require the verifier task to have terminated and its own read interval to
begin only after that terminal state.

At open, immediately before ACK, and at close, the holder MUST independently
observe the four exact pinned versions with native reads and compare complete
bounded row bags to its independently rebuilt source oracle. Each observation
MUST include pre/post native UUID and version identity, complete ordered
carrier projection, runtime/principal/namespace identity, UTC session timezone,
the selected data/log retention settings, effective PO setting, maintenance
identity inventory, source current/retained page and checkpoint custody, and a
bounded digest of the complete comparison result. Require at least seven days
for both `delta.deletedFileRetentionDuration` and `delta.logRetentionDuration`
on every role, and effective PO `ENABLE`; these are observations, not a hold.
Every stage MUST match the frozen original vector, source selection and
observation identity. No latest-version fallback, count-only comparison,
truncated read, or cached verifier result is admitted.

The interval MUST expire at 900 seconds measured by a monotonic host clock.
Before ACK the holder MUST complete a fresh full four-role comparison and source
custody reread, then submit the exact original destination receipt once, reconcile
from a fresh source transaction, and perform the close observation. No later
slot may mutate source before exact ACK reconciliation and all closing checks
pass. A missing or changed observation, expiry, changed pin/source identity,
insufficient bounds, close failure, or unknown ACK result returns a non-success
outcome and preserves original recovery custody. An uncertain ACK MUST be
reconciled; it MUST NOT be blindly resubmitted.

This profile covers visible UUID/version replacement, failed exact-version
reads, changed observed retention/PO settings or maintenance inventory, source
page/checkpoint drift, runtime/principal drift and elapsed-time expiry. It
assumes that admitted application operators do not intentionally run destructive
maintenance during the short interval. It explicitly excludes a malicious or
mistaken workspace/metastore administrator, cloud/storage administrator,
undocumented service-side removal, and out-of-band deletion between observations.
It MUST describe the outcome as `observed_available_at_checks` (or equivalent),
never as held, retained, protected or guaranteed. Removal after the last read
and before/during ACK remains possible; if it occurs, the ACK outcome must be
reconciled before recovery or retry. Selection of this profile is an explicit
owner acceptance of these exclusions and MUST NOT weaken any other profile.

For this profile only, `NativePinPort` establishes and rechecks exact complete
vector identity/custody; it does not claim that a pin row or renewal reserves
files or supplies a retention admission. The independent managed read observer
must prove exact-version readability at each required stage. Implementations of
the stronger provider-backed port retain their existing retention-admission
requirements without change.
