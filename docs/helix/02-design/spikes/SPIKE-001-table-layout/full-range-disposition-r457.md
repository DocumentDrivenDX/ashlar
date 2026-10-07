# Full scattered-batch range sweep (r456–r457)

All64 disjoint hash ranges and all100k fourth-batch changes were read against immutable prepared0/E6 on existing SQLwarehouse2439e1f2e37ac563/channel2026.39. Four persistent workers,16waves, result caching disabled and checked per session,60sstatement/socket bounds. Every query checks all20 predecessor fields and independently generated42-field source digest, including exact bags/cursors/delivery and delete-null after fields. All72native statements (64reads/8settings) succeed with final metrics; offline audit checks exact query texts, complete range coverage, results and costs. No canonical write, publication, source ACK, compute change or external-engine claim.

Native phase129.676s including3.887sconnection setup. Sweep125.790s includes per-wave complete cost/finality collection. Local independent oracle12.662s is charged separately. Reported83,650,922,507read bytes,5,033,043,183remote bytes,1401file-read occurrences,0write/spill; within110GB/240s bounds. Engine durations sum236.371s/caller durations288.043s across concurrent tasks: sums are not wall time. Complete100k source counts and all64 digests pass, zero predecessor mismatches, zero result-cache hits.

r447 prepared full-parent comparison measured39.279s/33,261,314,850read bytes. This sweep reports2.515times those read bytes and materially greater elapsed time; the full-parent run has no per-wave observation overhead and storage-cache/date conditions differ, so this is not a causal engine speed estimate. Even excluding controller observation, the64 query callers sum288s; batching/pruning is not admitted as a full-batch optimization. Do not adopt64partition directories, parallelMERGE or finer arrival microbatches based on the smaller four-range success.

Retain unpartitioned identity hash LC and the atomic inline predecessor guard as the measured candidate. Stop this range-scheduler branch. Current scale/caller/cold/ingest goals remain unproved.

## Next publisher critical-path work

Authoritative r437 receipts show current MERGE59.552caller seconds alone. Independent role writes cost raw4.195s, journal4.070s, tombstone1.640s, adjacency6.577s. Their sum16.482s is only a theoretical maximum reduction if overlapped without contention; concurrency cannot reduce current MERGE's own59.552s, and correctness/custody validation remains additional. Preflight31.991s and full231.609s ready-input processing demonstrate why role-write parallelism alone cannot credibly meet60s.

Next measure bounded concurrent **read-only custody/metadata checks** against the same immutable publication before attempting another full1GB mutation input. Determine which validation dependencies can overlap and which can be inherited from exact input/closed-CDF certificates; preserve complete evidence and explicit writer-fence limits. A later fresh batch may overlap writes to distinct role tables only after those dependencies are specified and bounded. Never remove semantic checks merely to improve the clock, claim production fencing from post-hoc custody, or extrapolate1B/5B from this40Mtarget.

A larger compute trial or revision of the provisional freshness/caller budgets remains an owner decision if existing-compute critical path cannot meet them. Do not resize/provision automatically. UC Delta architecture, exact Truss identity/properties/history, scoped graph mappings and deferred UMF remain unchanged.

Evidence: out/native/ashlar_full_range_sweep_r456/, out/full-range-audit-r457.json; governing CONTRACT-003/ADR-001; prior full publication r437 and full-parent comparison r447.
