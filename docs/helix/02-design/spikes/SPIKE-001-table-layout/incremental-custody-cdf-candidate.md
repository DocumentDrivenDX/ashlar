# Incremental publication custody candidate

Governed by [CONTRACT-001](../../contracts/CONTRACT-001-publication-boundary.md),
[CONTRACT-002](../../contracts/CONTRACT-002-consumer-read-boundary.md),
[CONTRACT-003](../../contracts/CONTRACT-003-delta-graph-tables.md) and
[ADR-001](../../adr/ADR-001-delta-canonical-and-serving-layout.md).
Status: experimental physical validation proposal. No selected real producer,
UMF vocabulary, source ACK, concurrent-writer fence or new support profile.

The complete r281 sweep proves one synthetic publication preserves its8M/40M
baseline but takes922.120s after ready-input preflight; apply-only49.209s also
exceeds the10s modeled100k-batch arrival interval at10k/s. Reduce inherited-row
validation cost without weakening exact change-set and snapshot custody proofs.
This proposal does not establish sustained throughput or passing freshness.

## Native mechanism to qualify

Azure Databricks documents automatic CDF for eligible row-tracked Delta tables
on Runtime18LTS+, using table_changes; it returns row data and change/version
metadata. Legacy CDF explicitly configured on a table is a different mechanism.
CDF uses the latest schema and has retention/schema-evolution limitations; it is
not permanent history. See [Microsoft's CDF documentation](https://learn.microsoft.com/en-us/azure/databricks/tables/features/change-data-feed)
and [table_changes](https://learn.microsoft.com/azure/databricks/sql/language-manual/functions/table_changes).
These documentation statements are not runtime compatibility evidence for Ashlar.

The existing r281 edge/forward descriptors record rowTracking and Runtime19.9 /
SQL2026.39. Start with a read-only table_changes interval covering exactly the
recorded MERGE version1 in those owned private tables. Do not toggle features,
retention or clone existing shallow clones. Cloned histories are distinct: qualify
only that table UUID's own mutation interval, never transplant source versions.
The independent r288 oracle expects per role90k update preimages,90k postimages
and10k deletion preimages, all known fields, no inserts or additional events.

## Acceptance proof for an owned synthetic publisher

1. Inherit an independently qualified immutable baseline vector, table UUIDs,
   supported schema/profile revisions and retained evidence. Read every exact
   predecessor from immutable input. Validate original payload SHA, exact token
   replay, complete replacement/tombstone/current partition, origin/cursors,
   unique identities/event ordinals and prospective typed endpoints before writes.
2. Record every actual mutation SID and bind it to that table's Delta history
   version. Account for all commits between baseline and selected candidate;
   refuse unexplained data, schema or protocol changes. A supported maintenance
   interval requires separate explicit custody proof. Commit metrics alone do
   not prove which complete carrier fields changed.
3. Read CDF over the complete accounted interval, without identity or batch
   filters that could conceal collateral changes. Require every event's commit
   version in the approved ledger, exactly expected classes/counts/identities,
   and complete all-field preimage/postimage/deletion multisets. Refuse extra,
   missing, conflicting or unavailable events. Empty/expired CDF is not success.
4. Check exact raw/journal appends and versioned tombstones against immutable
   inputs. If their CDF is unavailable, retain a separately qualified append
   custody proof and complete added-row checks; do not assume an INSERT receipt
   or current-state counts alone prove retained-history preservation.
5. With a qualified CDF interval and immutable baseline, complete exact change
   coverage supplies the unchanged-row custody argument. Validate it against the
   full r281 sweep before using it in the critical path. Snapshot/schema identity
   must remain stable; SHA comparisons retain multiplicity and assume collision
   resistance. The independently retained exact raw source/journal remains the
   semantic history; CDF is temporary physical validation evidence.
6. Publish a new immutable exact version vector only after the whole certificate
   passes. Include the actual revision map, input pins and validation report,
   canonical recorded_at and explicit synthetic source progress. Readers consume
   that vector. Unknown write outcomes use their same handles; no blind replay.

## Refusal and qualification corpus

Exercise omitted/duplicated events, wrong class or commit version, same-version
conflicting payload, exact-decimal or retained-content drift, invalid deletion
version, unexpected external commit, changed UUID/schema, missing retention
interval and interrupted partial roles. Each must retain the previous complete
publication and original input; no source ACK. Reconcile duplicate/stale ordering
under CONTRACT-001 before extending beyond this distinct version1-to2 batch.

Native read-only CDF comparison is the next experiment. Then compare a private
incremental certificate with the already qualified full sweep, measure the entire
new critical path, and run bounded successive-batch/queue tests. A single fast
sample, commit metrics, or preserved final state alone does not admit10k/s or
100k/s bursts. Production writer fencing and authorization remain separate open
requirements; this controlled fixture is not their substitute.
