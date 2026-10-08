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

The boundary can be exercised before UMF merges. Later mapping must identify
exact native-to-boundary loss and preserve source evidence. This is a proposed
contract, not Databricks compatibility or producer-adapter evidence.

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

Forty-eight focused local tests include original-handle recovery after uncertain
apply, incomplete validation retaining previous publication/progress, original
descriptor recovery after uncertain commit and lost acknowledgement, conflicting
intent refusal, malformed revisions and denied writer before metadata effects.
Those tests use an explicit in-memory backend to exercise coordinator order only;
no native durability, fencing, retention, complete effect parity, immutable
manifest or actual source checkpoint is established by the mock.

Native implementation must connect the existing Delta staging/apply/resolver
ports to complete durable attempt stores and recovery procedures. The currently
materialized private graph fixture remains unpublished and cannot be promoted
merely because coordinator unit tests pass. Full Truss acceptance/feed and the
additional source/schema paths remain required by the active toolkit goal.
