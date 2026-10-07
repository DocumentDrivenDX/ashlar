# Typed mutation input and full-parent join comparison (r443–r448)

UC Delta remains the selected architecture. These experiments tune its ingest path; they do not change canonical table identities, Truss semantics or UMF scope.

Native EXPLAIN probes r443 identify a strict JSON parser fallback: `from_json does not support option(s): allowSingleQuotes, mode=FAILFAST`. Default wide source/target reads have two sort-merge joins. An inner broadcast hint changes one join, but retains the parser fallback. MERGE EXPLAIN exposes a command envelope only, so it does not qualify actual mutation execution.

r445 creates a private, immutable 42-column mutation cache at Delta version0: complete before20/after20, delete flag and original change delivery ID. All100k typed keys/delivery IDs,90k updates/10k deletions and the complete42-field SHA match the independent generated oracle. Original raw records, JSON, source cursors and permanent journal remain pinned separately. This is a derivative input cache, not a canonical replacement or production receipt.

Preparation costs38.139 seconds of native phase time, including16.327 seconds for CTAS; the local oracle generation is separately outside that clock. Reported398,113,157 read bytes,157,384,015 write bytes and zero spill. One157MB file results despite a64MiB target (target is not a maximum). Prepared wide EXPLAIN is fully Photon with a shuffled hash left join and no JSON parsing. Column ANALYZE succeeds, but pinned EXPLAIN still reports missing optimizer statistics: do not claim statistics drove the plan.

r447 executes the complete100k source/target comparison against immutable39.97M-row E6, prepared first and legacy second. Both return100k matches, zero full20-field predecessor mismatches, and the same complete42-field digest. All12 preparation/comparison native statements are successful and final; r448 checks exact SQL, source/code bindings, results and cost bounds offline.

| Input | Caller seconds | Engine seconds | Read GB (decimal) | Remote read GB |
|---|---:|---:|---:|---:|
| Prepared |39.279|36.684|33.261|10.421|
| Legacy |39.566|37.797|33.542|5.401|

Neither result is cached, but storage-cache conditions differ and order is fixed. This pair does not establish a causal latency improvement. Neither query prunes target files; the prepared query still scans about40M rows. Adding preparation makes this unattractive as a standalone speed optimization on present evidence. No canonical MERGE, publication or fifth-batch replay was executed.

Next material design work: evaluate bounded hash-range target pruning and source scheduling, explicitly accounting for scattered changes covering the entire hash space and queue/freshness costs. Preserve exact tuple matching and the atomic predecessor guard. Admit any mutation experiment only with a fresh disjoint batch and complete six-role custody checks. The60s publication target, sustained/burst throughput, controlled cold/concurrent reads and actual1B/5B scale remain unproved.

Evidence: `out/native/ashlar_merge_plan_probe_r443/`, `out/native/ashlar_prepared_mutation_r445/`, `out/native/ashlar_prepared_join_compare_r447/`, `out/prepared-compare-audit-r448.json`. Existing large tests remain24M local/40.77GB and47.97M native current entities; this is not a billion-scale run.
