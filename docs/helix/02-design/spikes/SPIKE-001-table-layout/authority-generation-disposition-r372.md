# Native generation guard and concurrent handover r371–r372

Governed by CONTRACT-001–003 and proposed ADR-001. Private control-table reference, not a source-authority service or performance/scale admission.

The previous full-vector authority reference is cloned at recorded version4 into `client_dev.ashlar_entropy_20261006_r86.authority_generation_r371` UUID`a25452e6-fff0-4615-8a9f-1c771edb5203`. All three inherited rows remain exact, including both descriptors carrying the complete six-role reference. The seeded authority begins as writerB/token3.

The conditional MERGE now checks expected owner/token and requires successor=current+1 and successor>current inside the native statement. Two sources propose a regressing2 or skipped5 successor and a novel descriptor. Native ASHLAR_AUTHORITY_GUARD refuses both; exact whole-table readbacks and complete history remain clone0. Neither proposed descriptor is committed. The source worker is trusted and this does not qualify arbitrary descriptor-token binding or a public control API.

Two persistent clients then propose different owners at the same expectedB/token3, successor4. One succeeds and one fails. Native statement windows overlap2977ms; overlap includes planning/queue and does not establish simultaneous row reads. The winning owner/token4 is the sole authority row; inherited descriptors remain exact. Complete history is clone0/winner1 under the recorded IDs. The loser reports `ServerOperationError`; its full native error and final history are retained in the receipt.

All13successful and3failed native statements finalize. Workload21.233s reports165,979read bytes/5,539write bytes/0spill, including all failures. No graph-role mutation, production pointer, source ACK, provisioning/resize, maintenance, VACUUM or retention change occurs.

## Integration boundary

This strengthens the private seeded authority-row transition, not ownership over six separate Delta role tables. The guard cannot prevent an old worker from mutating shared current/history tables. Missing/duplicated authority rows, int64 exhaustion, descriptor-generation binding, actual access policy, lost submission outcomes, lease lifecycle and crash recovery require a complete implementation contract; no broad fencing support claim follows from this race.

The next design choice must address stale role writes: proven exclusive writer ownership, or immutable isolated attempt outputs whose only visibility transition is a guarded complete descriptor. Compare incremental copy/file/history costs before choosing attempt isolation; shallow clones and version retention are not free and cannot be inferred to meet1B/5B. Keep exact Truss identities/values/history, complete input/change/custody checks and pinned reads in either path. The existing LC physical candidate, graph-engine limits and deferred UMF binding remain unchanged. Warm/cold/freshness/sustained/burst and full-scale gates remain open.

Evidence: [audited receipt](out/native/ashlar_authority_generation_r371/audited-summary.json), worker and independent offline audit. Native window overlap is scoped evidence, not a universal handover guarantee.
