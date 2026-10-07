# Bounded maintenance publication design r359

Supplement to the draft publication/maintenance policy, governed by CONTRACT-001–003 and proposed ADR-001. The existing logical publication path remains the only evidenced publication mode. This design specifies a future maintenance-only mode; it does not activate it.

## Table layout and scheduling

Keep current objects/edges unpartitioned and liquid-clustered by derived lookup_hash, with full typed identity predicates. Keep64MiB as a target; measured rewritten files reach96MB. Raw records and property journals remain independent permanent semantic history. Adjacency is optional and carries the exact edge/version/endpoints. Maintenance changes physical versions, never source progress, identities, bags or history. No UMF binding or graph-engine release is added.

Use one serialized maintenance lane per publication authority, sharing exclusion with logical publication writers. The scheduler first checks input backlog and outstanding publication recovery. Either condition defers new maintenance. A backlog arriving during an owned run stops subsequent admission; it does not justify abandoning an unresolved handle. No numeric backlog tolerance is selected without measured queue slack. The demonstrated33.8s rewrite is not small relative to a10s100k-batch arrival window at10k/s; mandatory maintenance per batch remains unsuitable.

A candidate request records table UUID, base version, schema/profile revision, canonical half-open hash range, actual overlapping file set and bytes, estimated read/write/interference allowance, wall/statement guards and selection evidence digest. Count distinct candidate files when combining ranges: overlapping extrema can count the same file more than once. Candidate coverage is an admission estimate, not an engine promise. The49-file/2.338GB pilot is a qualification fixture, not a universal limit. Re-read inventory after every committed slice; do not execute all16 old selections blindly.

## State transitions and durable interfaces

| State | Required evidence and next action |
| --- | --- |
| Selected | Freeze request and parent immutable publication vector; validate backlog/resource admission. No mutation yet. |
| Owned | Obtain exclusion/fence token from the publication authority; recheck UUID, schema revision and head against selected base. Persist attempt ID before submission. |
| Submitted | Persist returned statement ID before polling. Observation timeout retains this state. Recover the same handle; do not resubmit. |
| Terminal | Obtain terminal status and finalized native costs. Failed/canceled work is charged and retained; inspect actual commit history even after failure. |
| Preserved | Enumerate every commit in(base, selected head], verify custody, profile and complete logical equality. Unknown/intervening commits refuse this attempt. |
| Ready | Verify fence still owned, parent vector still the intended logical parent, all required pins available, source progress unchanged and retention registry current. |
| Published | Append one new immutable manifest with distinct publication ID, maintenance kind, parent ID, exact new vector, unchanged logical progress/revisions, attempt and preservation receipts. Read back exactly before marking complete. |

The r350 receipt has base3 and selected5: both4 and5 belong to one OPTIMIZE statement. Version5 adds/removes no files, but is still part of custody. Neither a single last-history row nor assumed base+1 is sufficient. A later physical head does not invalidate a successfully preserved pinned snapshot by itself; it must be separately accounted for and never substituted silently. An unknown writer in the selected interval requires requalification, rather than filtering its commit out.

A maintenance manifest changes only qualified roles. All other vector entries retain their parent UUID/version, and raw/journal/tombstone progress is unchanged. Schema revisions must be compared independently: DESCRIBE DETAIL protocol/features are not a full schema proof. Preserve the selected parent and all active cursor vectors; a new manifest does not migrate existing cursors. External engine releases remain independently validated and pinned to one release.

## Recovery and retention

After a crash, inspect the durable attempt ID and returned native handle. If submission outcome is unknown and no statement ID was durably captured, resolve it through attempt tagging/history; missing observation is not permission to replay. Multiple commits from one statement are retained as one attempt. A terminal failure with partial commits cannot be published until complete preservation/custody is independently established under a newly admitted recovery path.

Publication must be idempotent under publication ID plus canonical descriptor digest: exact duplicate is read back; conflicting reuse is refused. Manifest append and exclusion ownership require a real concurrent implementation, not Delta's unenforced UNIQUE declaration. No source ACK advances on maintenance publication. Retention eligibility is the union of active manifests, pagination cursors, recovery attempts, retained serving releases and their required files/logs. No default TTL, VACUUM or pruning is authorized here. Expired snapshots refuse reads explicitly.

## Qualified controls and next implementation boundary

[Local receipt controls](out/maintenance-receipt-controls-r359.json) accept the audited native4/5 interval and refuse12 altered receipts, covering UUID replacement, intervening MERGE, unknown writer, missing/duplicate/reordered commits, base drift, carrier digest/coverage changes, protocol drift and resource overrun. The guard checks all400 complete20-field groups/39.98M rows under the recorded SHA256 collision assumption. This is offline saved-evidence validation, not native adversarial concurrency or a production schema/fence proof.

The next executable slice is a private maintenance-manifest reference with exact duplicate/conflicting-ID and old-cursor controls. Before any production claim, supply an authority/fence implementation, independently verified schema revisions and active-pin registry; then test two competing writers, publication crashes and retention refusal on the actual native path. Scheduling requires a matched ingest/read interference measurement with maintained versions genuinely published. Keep the250ms caller,100ms engine,60s freshness and1B/5B obligations open; the RPC diagnostic gives no basis for a polling optimization or target revision.

The [native existing-ID guard](manifest-atomic-disposition-r364.md) now qualifies conditional MERGE refusal for conflicting matched descriptors, with whole-statement rollback controls. This does not establish uniqueness for simultaneously inserted absent IDs or ownership across role tables; keep Ready-state fence and full-role obligations unchanged.

[Absent-ID race/recovery evidence](manifest-race-disposition-r368.md) now records native concurrent-append refusals and one admitted recovery per terminal failed handle. Recovery reloads UUID/head/winner before exact replay or semantic conflict refusal. The two observed races do not replace the required authority fence across graph roles or establish universal absent-ID uniqueness.

[Full-vector authority reference](full-vector-authority-disposition-r370.md) now qualifies a same-table expected owner/token condition and descriptor append, including stale-owner rollback after handover. Six original graph/history pins remain exact. This is not a fence against writes to separate role tables; generation issuance and concurrent takeover need additional qualification.
