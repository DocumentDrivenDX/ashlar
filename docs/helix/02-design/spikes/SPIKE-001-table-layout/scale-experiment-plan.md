# Scale experiment — owner authorized local then Databricks

Governed by CONTRACT-003/ADR-001 and the owner's instruction to limit time, money and effort. The earlier native 10M-edge experiments establish bounded pruning/preservation, not 1B-node/5B-edge capacity. The current warehouse is available; no Spark/graph runtime was found in the recorded inventory. The superseding owner authorization and executed results are recorded below.

## Questions the next experiment must answer

Measure file/metadata growth, native hash/full-key pruning and maintained ingest cost on the actual 0.3 carrier shape, including cursor/origin references. Keep client/server/compile/engine timing separate. Retain exact fields, source history and actual-version publication. A capacity run cannot pass by dropping raw history or structural coverage to make current-table reads fast.

## Initial resource-admission proposal (superseded for the current authorized phase)

Require an explicit owner spending/time limit and effective account metering/pricing for the existing warehouse. The quoted rate for a separate optional warehouse is not evidence of this shared warehouse's incremental bill. Capture current compute settings, baseline storage/file sizes and concurrent-use state. Do not resize/stop shared compute. Exclude the billion-row run until measured throughput and complete temporary-storage/cost estimates justify it.

Bound each submitted phase by a persisted statement handle and a wall deadline. On deadline, request cancellation and recover the same handle until terminal; do not retry an unknown write. Cancellation does not guarantee no committed side effects: inspect table histories and exact row/digest evidence. Temporary tables remain recoverable; DROP/VACUUM requires separate authorization. Storage therefore continues after the experiment unless an explicitly authorized cleanup policy exists.

## Prepared workload sequence

1. Reuse recorded 10M-edge baseline without copying. Inventory actual files/bytes/versions and inspect its high-entropy scope; do not extrapolate repeated-payload compression.
2. Under the selected cap, run one fresh 0.3 edge-carrier phase with at most 20M rows, two publisher-origin fields and realistic distinct payloads. Record all omitted graph/source tables as scope exclusions. This phase is a physical sensitivity experiment, not full graph admission.
3. If the first phase finishes within the admitted cap and demonstrates acceptable pruning/file/ingest costs, prepare the next larger graph phase with full canonical, raw source, property journal, adjacency and publication accounting. Derive its resource estimate from measured bytes and measured throughput, then obtain the required larger resource bound before submission.
4. At any failed deadline, digest, identity, history, publication or cost condition, stop growth. Keep the measurements and prior published state; do not repeatedly run cached lookup variants.

The 20M-row cap is a proposed first sensitivity step, not permission or a hidden replacement for the full 1B/5B requirement. It cannot establish sustained 10k/s plus bursts. A later publication schedule must include staging, validation, maintenance, readers and source arrival clocks, then measure queue growth and freshness distributions. The actual producer/worker profile is currently unqualified, so source-freshness admission remains synthetic until real source evidence exists.

## Metrics and evidence

Store generation seed/entropy profile, native schema and complete version vectors, query text/parameters and handles, all fields/digests, actual active files/bytes, clustering/maintenance histories, result-cache state, remote I/O, queue/startup time, compile/engine/caller distributions, per-arrival processing/freshness and backlog, cancellation/recovery outcome and resource/billing evidence. Report omissions and unknowns alongside every claim.

Use the historical performance targets for comparison only under the owner's fixed Unity Catalog Delta direction; a failure is not an architecture veto. Do not claim that small correctness or arithmetic capacity sensitivity proves the requested billion-node runtime performance.


## Verified existing baseline metadata

[r77 read-only inventory](out/native/ashlar_scale_baseline_inventory_20261006_r77/summary.json) confirms stable latest edge version 5 before/after DESCRIBE DETAIL: 136 active files, 6,182,560,646 bytes (6.18 decimal GB), mean active file size 45,460,004.75 bytes (43.35 MiB). The 64MiB target is a tuning property, not an observed hard file size. These compressed bytes describe this specific historical carrier mix and exclude raw feed/history/other graph tables, retained Delta versions and 0.3 reference width. Do not multiply them into a billion-node storage claim. No full scan or larger write was performed.


## Owner disposition

On 2026-10-06 the owner selected small tests and continued design work, with commit/push after each iteration. This resolves the pending resource question without authorizing the proposed 20M-row phase or new compute. Keep the plan prepared for future scale qualification; continue implementation-ready table/protocol/mapping design and small correctness tests.


## Payload variety qualification

[r80](out/native/ashlar_payload_sample_20261006_r80/summary.json) reads 256 rows
from the existing edge version 5 with one unordered LIMIT query. All 256 property
texts differ, but every bag has exactly key 201 and exactly 2,058 UTF-8 bytes;
retained content is the same 28-byte text in all rows. The sample includes two
source authorities and two relationship types. Local gzip preserves about 56.94%
of joined property bytes and 1.10% of joined retained bytes. These are sample
observations, not Parquet/Zstd compression or a representative full-table audit.
LIMIT bounds returned rows, not independently measured physical scanning.

A future qualified workload must distinguish byte variety from semantic shape
variety. Declare the distribution of property counts/IDs, variable text lengths,
missing versus explicit null, integers above 2^53, exact decimal/time tokens,
nested retained content, revisions and source cursor width. Include structured
repeated values alongside distinct opaque text, and report proportions and seed.
Also vary source/type skew, endpoint degree skew and updated-key locality rather
than assuming uniform records represent a real consumer. Preserve every exact
carrier and native source origin under the selected profile. Character frequency
or distinct whole-bag count alone does not qualify this workload.

Before any larger write, generate and inspect a small corpus for these declared
shapes and measure its actual bytes. Resource estimates must separately account
for current carriers, raw input, property-event fanout, structural projections,
retained Delta versions and releases. The owner's small-test disposition still
excludes the prepared 20M phase; this observation creates no scale authorization.


## Small declared-shape corpus

[varied-corpus](out/varied-corpus/summary.json) supplies 64 objects, 192 edges and
256 complete synthetic raw envelopes, generated deterministically without compute.
Eight property shapes occur 32 times each: empty, explicit null, exact large
integers, decimal/exponent tokens, timezone text, escaped/Unicode variable text,
wide repeated categories and nested values. Opaque-bearing shapes vary text length;
retained nested content varies by carrier. Native tuple IDs repeat across source
and type, and signed-int64 extremes remain decimal strings in transport. Typed
endpoint closure, independent parallel edges, self-loops and isolates are checked.

Every raw envelope includes every canonical fixture field with matching exact
props_json/retained_json and a checked UTF-8 payload digest. This closes the earlier
synthetic fixture's missing-structural-field issue for this local corpus only.
The corpus matches 0.3 object/edge column names, but is not native Truss wire or
transaction history. Property journal events, revision prerequisites, seed/coverage
boundaries and real source authority are still absent. Synthetic source xid text
exceeds signed BIGINT; no scalar checkpoint is generated.

The next small native fidelity slice may load these declared carriers into private
0.3 tables and compare exact raw strings, endpoints, hash computation and source
references. It should use byte-preserving transport and actual versions. Its
result would remain correctness evidence; do not infer compression, throughput or
billion-scale capacity from fewer than a thousand synthetic records.


## Superseding owner authorization (2026-10-06)

The owner explicitly requested a large scale test on this machine followed by
one on Databricks. The earlier small-test restriction no longer excludes this
phase. Start at 4M nodes / 20M edges / 200k scattered edge updates, using the
same deterministic full 0.3 current-carrier generator. Local resources: 128GiB
machine memory, 18 logical CPUs, 3.3TiB free disk; cap Spark at local[8] and
32GiB driver memory. Retain recoverable temporary data. Databricks uses the
existing running 2X-Small warehouse with one cluster; no resize/new compute.
Native statements have a 900-second server timeout. Exact billing remains
unavailable; report elapsed execution and stored bytes without invented dollars.

The local run range-distributes and sorts by lookup_hash into 256 partitions.
OSS Delta 3.2.1 rejects the Databricks targetFileSize/compression table properties;
use Parquet Zstd writer compression locally and record actual files. The native
run uses liquid clustering and the candidate 64MiB target. These implementations
share logical carriers and workload but are not identical physical mechanisms.
Two initial local attempts failed during table-option analysis before data writes;
no performance result can be drawn from those attempts.

Eight payload shapes include empty/null bags, exact large integers/decimals,
timezone text and distinct SHA-derived variable strings. Repeated digest blocks
are intentionally compressible and not fully independent random bytes. Type and
source identity vary; endpoints are deterministic and match the node type/source
profile. Every current carrier includes exact cursor and raw delivery references.
References are synthetic; source_record, journal, tombstones, adjacency, degree,
publication protocol and source arrival schedule are excluded in this physical
sensitivity phase. It does not qualify full ingest/publication freshness.

Measure initial writes, complete bidirectional full-field parity, file/byte
inventory, stage generation, scattered MERGE, exact updated-field parity, old
Delta snapshot stability, 50 dispersed singleton reads and local four-reader
concurrency. Local caller timing includes Spark query/collection after hash
preparation; it is not remote-client latency. Native measurements retain SDK
statement IDs and server history. Larger graph growth must follow actual
throughput, bytes and failed/passed limits rather than assuming billion admission.


## Executed phase

The [24M-carrier comparison](scale-comparison.md) now records completed local
and native runs, corrected property-ID-map fidelity, final uncached version-2
read metrics, DV amplification and a no-op FULL maintenance result. Native
latency misses the comparison budgets; the phase does not admit full source
publication or billion scale. See its explicit entropy, graph-distribution,
constraint and billing limits before selecting further growth.

## r213: measured resource and publication capacity disposition

The saved-evidence calculator `capacity_measured_r213.py` hashes its native inputs and produces `out/measured-capacity-r213.json`. Unlike the older assumed-width sensitivity, it derives update/publication numbers directly from completed native samples. No new native workload runs in this iteration.

For the maintained hot100k update, LC writes3,265 bytes per changed entity versus52,537 for bucket+Z-order, a16.09× ratio. LC caller4.110s versus37.018s; these standalone writes exclude complete publisher validation. Previously hot identities, different maintenance histories and DV behavior prevent causal or fresh-scattered-update extrapolation. Keep hash LC as the candidate; do not select bucket partitioning merely for its scoped one-file read result.

Full serial and shared-history publisher samples take62.468/64.647s for100k, implying1,601/1,547 entities/s only under fixed-service arithmetic. At10k/s, each100k input fills in10s but needs6.25–6.46 fill intervals of service. Twelve-batch FIFO sensitivity records accumulating backlog. Idealized independent-lane arithmetic gives7 lanes for10k/s and63–65 for100k/s; this is expressly not a recommendation to launch those writers on shared compute. Source transaction boundaries, validation resources, manifest conflicts, disjoint custody and real producer fencing must be solved and measured. Bigger current tables alone cannot establish sustained ingestion.

The next growth proposal is8M nodes/40M edges, generated in1M-edge slices on existing compute. Initial edge-current storage planning range60–100GB is a deliberately conservative assumption around the recorded20M/34GB carrier, not a measured future width. Node, raw, journal, adjacency, retained versions and failed work need separate estimates before admission. Proposed cumulative bounds:140GB newly committed data,400GB reported reads,one hour wall time,180s per native statement. These bounds are ceilings, not complete price estimates or permission to silently omit roles; if complete-role estimates plus validation reserve do not fit, revise the proposal rather than run a narrowed graph. Do not start this stage until the concrete generator, integrity controller and complete resource accounting establish feasibility within the bounds.

First prepare the mixed-shape corpus and full role accounting, then probe a fresh scattered update on the existing20M snapshot to distinguish first-touch rewrite cost from the already-hot set. Keep native exact raw/current/journal and manifest barriers; any improvement must reduce actual service or demonstrate safe scheduling, not move correctness checks outside the published freshness clock. Cache-state and caller routing remain separate unresolved read questions. The owner-selected1B-node/5B-edge objective and original comparison targets remain intact; these arithmetic calculations do not admit runtime scale.

## r214: full-role accounting and existing first-touch evidence

`role_accounting_r214.py` reproduces hashed native baseline/publication evidence in `out/role-accounting-r214.json`. The original r89 update already touched fresh deterministically scattered200k identities: native MERGE59.726s/caller61.367s,512 deletion vectors added across512 baseline edge files. The ID permutation in `scale_workload.py` is deterministic, not independent random draws. This property107 insertion differs from later property105 replacements, so the timings identify workload coverage without establishing a causal hot-versus-cold ratio. Another duplicate first-touch write is unnecessary; a future mixed-shape/fresh-property105 cell must isolate the remaining differences.

Separate measured current widths are1,691.108 compressed bytes/node and1,697.365 bytes/edge for this synthetic corpus. Holding widths and fill constant gives81.423GB current-only at8M/40M,128 object files and1,024 edge files. These are arithmetic sensitivities, not promised physical output. The140GB proposed total cap leaves58.577GB before raw bootstrap, property history, projections, stage overlap and retained versions. Full initial raw origins were absent in the original baseline, so filling that gap is required for complete-role admission; it cannot be hidden by calling current bytes the full graph.

The changed-origin sample stores1,729.206 bytes/raw event. At10k entity changes/s, unchanged width would accumulate1.494TB/day of raw records alone. The12.684-byte journal event is an absent-to-boolean property107 change with narrow repeated values; its10.959GB/day arithmetic cannot price general property105 before/after histories, multi-property fanout or arbitrary exact tokens. No retention deletion, acknowledgement policy or VACUUM is selected here. Retention must follow consumer recovery and publication-pin requirements, with bytes and cost independently accounted per role.

Next implement a bounded mixed-shape source/bootstrap corpus with complete raw and per-property history accounting before growth. Include variable property counts/IDs and widths, missing versus explicit null, exact numeric/time tokens, nested unknown retained content and skew; preserve deterministic full oracle comparisons. The large-stage controller must admit all roles plus validation reserve, or openly revise the proposed resource ceiling. Billion-node runtime, sustained/burst freshness and controlled-cold caller gates remain unproved; source fencing remains a separate real-producer qualification gap.

## r215: complete mixed-source/property-event corpus prepared

`mixed_history_r215.py` extends the existing256-carrier mixed fixture with bootstrap plus rotated-bag updates:512 complete carrier/raw records and2,496 property events (1,664 sets,832 removals;64 explicit-null sets). Every carrier has a unique raw origin, matching SHA256 and exact full-envelope carrier parity. Replay checks predecessor presence/value, then compares every resulting property token against the expected bag. Decimal exponent, negative zero, large integers, Unicode/escapes and nested tokens are retained by lexical spans rather than floating-point conversion. Empty bags create no invented property event. Typed graph identities/endpoints and retained fields are copied from the previously validated corpus.

Raw envelopes preserve complete bag order/whitespace. Property replay verifies exact value tokens and missing/null distinction; it does not claim reconstruction of whole-bag lexical ordering. Set/remove vocabulary remains a proposed synthetic source profile, not native Truss event semantics or production bootstrap authorization. Output evidence is `out/mixed-history-r215/summary.json` and three hashed JSONL role files. JSONL UTF8 sizes are not native Parquet/compression estimates.

Next load this bounded complete corpus into owned native role tables, with decimal-string BIGINT casting and no JSON numeric round trip. Check every field/token, raw origin and property predecessor, then measure actual role bytes/files. Use those results to revise full-stage accounting before growth. No new compute, baseline mutation, source ACK/fencing, billion admission or UMF binding is implied.

## r216–r217: native complete mixed role preservation passes

The initial r216 CTAS is rejected with INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE because decode/unhex expressions are not accepted in VALUES. The failure audit verifies the exact server query is FAILED and the owned target is absent before a corrected workload is admitted. Preserve the original source, failure and native history; no unknown write is replayed. Corrected r217 uses hexadecimal literal strings in VALUES and decodes in the SELECT projection, then casts BIGINT/BOOLEAN/TIMESTAMP fields explicitly. Native preservation does not rely on JSON floating-point conversion.

Four new owned UC Delta role tables pass every-field multiset round-trip comparison with the complete local oracle:64 current nodes,192 current edges,512 raw records and2,496 property events. Native reader3/writer7 are recorded with each UUID/version0. Exact props/retained/raw strings, old/new lexical tokens, presence flags, signed-int64 identities and decimal-string cursor components survive. Timestamp display is normalized to native string for comparison, not expanded source timestamp semantics. Combined native fidelity plus local full source parity/property replay is compositional fixture evidence, not a newly tested native join/replay or real producer guarantee.

Active role sizes/files: nodes17,715bytes/1file, edges38,464/1, raw79,055/1, journal80,211/3. Reported successful-r217 work reads215,445bytes and writes421,300,0spill, within1GB/100MB ceilings; physical active bytes differ from all reported work writes. Initial rejected-r216 costs are retained separately and not silently folded into this successful-run total. Tiny tables, repeated fixture patterns and writer overhead prevent transferring these compressed widths to8M/40M or1B/5B. CTAS has no declared NOT NULL/unique constraints and is not execution of the full DDL package. No publication manifest is advanced, canonical state is unchanged, no resize/new compute or VACUUM.

Evidence: `out/native/ashlar_mixed_r216/failure-audit.json`, `out/native/ashlar_mixed_r217/summary.json`, exact statements and final shared native histories. Next qualify mixed-corpus hash construction against the native identity expression and execute structural endpoint/origin/event checks at pinned versions before promoting this fixture into a larger generator. Complete bootstrap source width/retention and larger-range exact integrity costs still need measured estimates. UC Delta/hash LC remain selected/proposed, external mappings scoped and UMF deferred; performance and billion-scale gates remain open.

## r218: pinned native semantic links close fixture qualification

Read-only all-row checks at the four recorded UUIDs/version0 pass on DBSQL2026.38,Etc/UTC:64 node and192 edge native full-tuple identities are unique; every lookup_hash equals native SHA256(to_json(named_struct(source_system,type_id/rel_type_id,id))) including signed-int64 extreme fixture IDs. Both source/target typed endpoint anti-joins return0. Current rows resolve exact raw feed/epoch/delivery and cursor, bag/retained UTF8 and identity links. All512 raw origins are unique and their payload SHA256 matches. Every property event resolves raw cursor/version/source/id.

Full raw and event values are then fetched from native pinned tables. Local replay verifies unique contiguous event ordinals per delivery, kind/type/revision, old presence and exact old token, operation/new presence, and every resultant property token for all256 entities across512 origins/2,496 events. This is native-value local replay, not a SQL replay implementation. The earlier r217 all-field oracle comparison complements these narrower native link predicates; neither is replaced by samples. No mismatches. Evidence: `out/native/ashlar_mixed_links_r218/summary.json`, exact statements/final histories. Reported reads780,413bytes,0write/spill, within1GB ceiling. No mutation/manifest/compute changes.

This closes the bounded mixed fixture identity/hash/structure/origin/event chain under the recorded runtime and declared synthetic profile. It does not qualify NOT NULL/uniqueness enforcement, source transaction completeness, real Truss semantics/fencing, production bootstrap, external engines or any performance/scale gate. Next use the verified recipe in a streaming deterministic larger generator with independent source-role accounting, a variable opaque-byte profile and declared locality/degree skew. Preserve full oracle reconstructibility and bounded chunk handles; repeated fixture compression alone must not justify8M/40M resource admission.

## r219: streaming mixed bootstrap recipe and complete local calibration

`scale_mixed_r219.py` provides deterministic random-access carrier generation and streaming current/raw/property-journal/forward-adjacency role output. Identity allocation puts node IDs1..N and edge IDsN+1..N+E in the shared signed-int64 range. Source/type endpoint construction resolves within source-specific allocations without materializing large endpoint pools. Source ordinal residues give approximately80/20 source split;20% of edges use a source-specific hub and1/97 explicit self-loops. Eight exact semantic bags gain property901 with SHA-derived distinct64/256/1024/4096-character opaque text. The distribution is declared synthetic stress coverage, not discovered consumer frequency.

Every local calibration row passes duplicate identity, endpoint, complete raw-carrier/digest and exact bootstrap property replay checks:4,096 nodes,20,480 edges,24,576 raw records,98,304 property events and20,480 forward adjacency rows. Full stream hashes reproduce independently. Role JSONL bytes are8,994,904 node current,45,288,795 edge current,71,300,667 raw,90,467,533 journal and3,125,645 adjacency; total219,177,544. Mean property bootstrap fanout4/entity arises from the declared eight shapes. No corpus-sized role files are retained; stream hashes and accounting evidence are saved in `out/mixed-scale-calibration-r219.json`.

Boundary arithmetic checks at5/25,7/35,8M/40M and1B/5B cover selected first/last IDs/endpoints only (`out/mixed-scale-boundaries-r219.json`). They do not verify all future rows or native/billion execution. Generator working memory is proportional to one payload; this small full-row calibration keeps an identity set to check uniqueness, which must become bounded range proofs in large execution.

JSONL bytes describe wire/generated role output, not compressed Delta size. Repeated widths/source shape at8M/40M would already yield hundreds of GB before compression; the140GB native-data ceiling cannot be assumed to fit. Distinct opaque payload and journal tokens prevent substituting tiny repeated-fixture compression. Bootstrap is complete for all calibration entities but change/deletion schedule, source transaction/fencing and native SQL generator differential remain unimplemented. Next port this recipe to native bounded range generation and measure a small distinct-payload complete-role chunk against the Python oracle, then revise complete-stage bounds from native files and validation costs before growth. Keep full original scale/SLO gates; no admission follows from local arithmetic.

## r220–r221: distinct mixed bootstrap native role widths

Client-generated128-node/640-edge bootstrap from the streaming recipe is materialized into five new owned Delta CTAS tables. All fields in all128 node,640 edge,768 raw,3,072 property-event and640 forward-adjacency rows match Python oracle multisets exactly at UUID/version0. Decimal-string numeric casts, exact lexical bags/retained/raw/property tokens and presence flags survive; normalized timestamp display does not expand source timestamp semantics. Input comes from Python, so this is not native SQL range generation differential or full declared-constraint package execution. No manifest/canonical/compute changes.

Native active bytes/files: nodes117,125/1, edges523,575/1, raw658,859/1, journal582,504/3, adjacency5,487/1. Reported work reads1,887,550bytes/writes3,770,279,0spill, within1GB/100MB ceilings; all IDs final. Evidence: `out/native/ashlar_mixed_r220/summary.json` and shared histories.

`storage_projection_r221.py` hashes those results and independently accounts each role in `out/native-mixed-storage-r221.json`. Holding tiny-table widths/distribution constant gives8M/40M current7.320GB nodes+32.723GB edges,41.179GB raw,36.407GB bootstrap journal (192M events at4/entity),0.343GB adjacency:117.972GB total. Proposed140GB ceiling leaves22.028GB before staging, future before/after history, failed work, retained Delta versions, logs/checkpoints/DVs or validation reserves. A1.5× width sensitivity gives176.958GB and exceeds the ceiling. These are arithmetic scenarios with tiny-file/repeated-metadata/ordering uncertainty, not predicted compression, cost, future file count or scale admission. Earlier81GB current-only estimate describes another carrier distribution and is not substituted for this distinct mixed recipe.

Keep8M/40M proposed rather than silently narrow roles or launch under an insufficient cap. Next implement native range generation matching the streaming oracle, calibrate a larger but bounded distinct chunk and complete exact integrity/resource controllers. Measure retention/change-role overlap independently; no deletion/expiry decision here. Original ingest/scale/cold/caller gates and real producer fencing remain open; UC Delta/hash LC selected/proposed and UMF deferred.

## r222–r225: complete native range recipe matches independent Python oracle

`scale_mixed_sql_r222.py` generates bounded native carrier ranges with signed-int64 allocation, source-specific endpoint arithmetic, SHA256-distinct opaque text, eight lexical bag shapes, retained content, native identity hashes and exact string cursor components. Bounds reject invalid ranges and arithmetic overflow before SQL submission. Lexical numeric/time/property content comes from declared constant token text rather than native JSON numeric parsing. Source-specific allocation is computed directly, without huge endpoint pools.

Read-only r223 compares every field of128nodes/640edges plus16-row node/edge tails for both8M/40M and1B/5B planning ranges:832 generated rows total. All fields match Python, including native timestamps normalized only for display. Runtime DBSQL2026.38/EtcUTC. Selected large-range tails are arithmetic/token differential, not graph-scale execution or preservation proof. Reported native storage reads/writes/spill all0; generated-result compute work still occurs and zero storage counters do not mean zero cost. No materialization.

`scale_mixed_roles_sql_r224.py` adds complete raw envelope/digest, bootstrap property events and forward adjacency. Raw carriers serialize typed transport fields as decimal strings and retain nulls, matching the independent Python full envelope exactly. Property901 token is constructed from known generated opaque hex; other tokens remain lexical constants, preserving exponent/negative zero/large integers/missing-null semantics. The source profile remains synthetic set bootstrap, not native Truss events.

Read-only r225 all-field multiset comparison passes every generated role row:128nodes/640edges,768rawrecords,3,072propertyevents and640adjacencyrows. Complete Python oracle comparison covers exact raw bytes/digests, cursor/reference, retained content, event ordinals/presence/tokens and typed adjacency. Native role SQL now exists; prior pending-port limitation is closed for this bounded tested recipe only. Reported reads/writes/spill0, no mutation/compute changes. Evidence: `out/native/ashlar_mixed_sql_diff_r223/summary.json`, `out/native/ashlar_mixed_roles_diff_r225/summary.json`, exact queries and final histories.

Next materialize4,096nodes/20,480edges with all five complete roles via the qualified native recipe, using owned tables and10GB read/1GB write admission,180s per statement and persisted native handles. Keep explicit full-field range/oracle checks and all bootstrap/history role bytes; no large-stage admission from the small differential. Then revise8M/40M storage, validation and failed-work reserves from those actual native files rather than launching under the uncertain140GB cap. Change/deletion schedule, full constraints, source fencing, native publication clocks and original sustained/burst/cold/caller/billion gates remain open; UMF deferred.

## r226–r227: complete native mixed calibration materialized and verified

The qualified native range recipe materializes4,096nodes/20,480edges plus all24,576raw bootstrap records,98,304propertyevents and20,480forward adjacency rows into five new owned UC Delta tables. Every role passes an independent Python all-row/all-field UTF8 digest oracle: explicit null marker or UTF8 length+hex value per field, SHA256 per row, sorted row-digest multiset SHA256 and cardinality. This gives cryptographic exact-preservation evidence under collision assumptions; it is not described as collision-free direct value comparison. Smaller r223/r225 direct all-field differentials qualify the recipe separately. Node/edge full native identity uniqueness and both typed endpoint anti-joins pass. Runtime DBSQL2026.38/EtcUTC, each native role reader3/writer7, UUID/version0 retained.

All5 roles finish within10GB read/1GB write bounds with100MB next-phase reserve and180s native statement cancellation bound; final telemetry reads70,714,402bytes, writes59,228,575,0spill. Active role bytes equal59,228,575: nodes3,313,356,edges16,573,079,raw20,991,429,journal18,235,810,adjacency114,901,one active file each. No OPTIMIZE, canonical/publication mutation, compute change, write replay or VACUUM. Evidence: `out/native/ashlar_mixed_materialize_r226/summary.json`, exact queries and shared final histories.

CTAS is an experimental physical profile: current hash LC, raw delivery LC, journal source-delivery LC and adjacency source-id LC. Journal clustering differs from the current full DDL's producer-position profile and does not silently replace it. No NOT NULL/unique constraints or full-package support claim is made. This is complete synthetic bootstrap data, not a production bootstrap authorization/manifest or source fencing proof. The collect-list sorted-digest implementation is bounded to this calibration; larger verification must use range-wise bounded digests with complete range membership, not an unbounded billion-row aggregation.

Saved-evidence calculator `storage_calibration_r227.py` (`out/native-complete-storage-r227.json`) projects each independently measured role width to proposed8M/40M:6.471GB nodes,32.369GB edges,40.999GB raw,35.617GB journal and0.224GB forward adjacency=115.681GB. Prior tiny-chunk117.972GB arithmetic is nearby, but this is still distribution/ordering/width sensitivity rather than future capacity proof.140GB ceiling leaves24.319GB before changes, before/after history, staging, retained versions,failed work,logs/checkpoints/DVs and publication/control overhead.1.5× width yields173.521GB and exceeds cap. No arbitrary table-role omission or premature8M/40M admission.

Next implement exact property change/removal and entity-deletion roles and a bounded publication schedule on this complete bootstrap, measuring all retained/source/history overlap and validation clocks. Preserve typed endpoint rules for deletion and actual-version publication. Native fresh/hot update distinction, sustained10k/s/100k/s recovery, controlled cold/caller, full1B/5B and real producer fencing remain unproved; UC Delta architecture selected, external mappings scoped, UMF deferred.

## r228: mixed mutation/deletion oracle closes change-workload definition

`mixed_changes_r228.py` defines one deterministic2,048-edge change batch over the complete4,096node/20,480edge bootstrap. Selection is a checked coprime104729 permutation modulo edge count, ensuring distinct scattered identities rather than reusing the hot100k set. Of2,048 selected edges,1,843 replace opaque property901 with new equal-length unique text, add property902 as exact exponent token or explicit null, and selectively remove a preexisting non901 property;205 delete edges. Nodes remain unchanged, so this test cannot conceal dangling endpoints with a node-delete cascade assumption.

Complete raw change records contain exact before carrier and after carrier/null plus source cursor and unknown extension content. Independent lexical replay validates every property predecessor/presence/token, resulting bag, event origin/entity/ordinal and preserved nonproperty fields. Entity deletion uses null property_id lifecycle/delete with false presence flags and a matching tombstone; this is explicitly proposed synthetic vocabulary, not native Truss semantics. Final all-edge endpoint closure passes20,275 remaining rows. Four negative controls refuse wrong old token, wrong event entity, wrong tombstone entity and invalid delete presence flag. Full output stream hashes/accounting reproduce.

Evidence `out/mixed-changes-r228.json`:2,048rawrecords10,657,658JSONLbytes;4,437journalrows7,556,108bytes;205tombstones58,144bytes;1,843current replacements4,074,242bytes. Journal includes3,686sets,546removals,205lifecycle deletes;819sets are explicit JSON null. These are generated wire bytes, not compressed native storage or mutation/retention costs. No Databricks workload, publication, source ACK/fencing or latency admission runs in this iteration.

Next materialize immutable native change inputs and apply to fresh owned copies of the complete bootstrap. Preserve full raw/journal/tombstone/current/adjacency exact checks, unchanged-row custody, full identities/endpoints and actual-version manifest barrier before measuring publication clocks. Keep predecessor/source origin exact; refuse invalid lifecycle data rather than treating null property_id as a null-valued property. Measure both active and retained-storage/work bytes. Small-batch schedule is protocol/role-cost evidence only; sustained10k/s/100k/s burst and full1B/5B gates remain open. No baseline/canonical mutation, cascade policy, compute resize or retention expiry is authorized by this fixture definition.

## r229: complete mixed mutation input staged and native predecessor proven

The full2,048-edge change oracle is serialized into an owned native input table through eight disjoint256-index append ranges. Every range has a persisted native statement ID and final cost admission before the next range; native180s cancellation/200s observation bounds apply. No unknown write is retried. Observed physical head8 and targetUUID `e625ee90-7283-4569-80bd-4efbce44e553` are retained; subsequent consumers explicitly pin version8 rather than repoint to a later maintenance head.

All2,048native change_json records pass independent SHA256 sorted multiset/cardinality and distinct change_index checks. Records include exact before/after carriers, complete raw source payload/digest, property/lifecycle events and deletion tombstones. A native full-field predecessor join checks every before carrier against the UUID-verified complete edge bootstrap VERSION AS OF0, including typed endpoints, bags/retained strings, origin/cursor, native hash and timestamps. Local exact event/tombstone replay ran before admission. Input validation does not substitute for the required post-apply output proof.

Stage active bytes3,653,980 across8files. All work reports24,142,570bytes read,7,297,978bytes written,0spill, below10GB/1GB ceilings with100MB reserve before each256-input block. Work writes differ from final active bytes and neither is a complete retained-storage/billing measurement. Evidence: `out/native/ashlar_mixed_change_stage_r229/summary.json`, final shared histories and `statements.jsonl.gz`. Exact submitted SQL/results are losslessly gzip-compressed with a byte-identical decompression check; the original live statement file remains a recovery pointer and is excluded from package hashes. No current/raw/journal/tombstone/adjacency publisher write or manifest advance has occurred.

Next apply the pinned immutable source to fresh owned bootstrap copies, retaining the selected publication barrier and verifying the complete final current/raw/journal/tombstone/adjacency vector. Stage-generation time is separately recorded and must not disappear from source-age claims; any timed ready-input processing screen must explicitly exclude it and is not sustained throughput evidence. Verify predecessor/ownership and native statuses again before mutations, preserve old pinned versions, and do not infer runtime/billion admission from this2,048-change source. Real producer fencing, full ingest/cold/caller/scale gates remain open; UMF deferred.

## r230–r234: complete mixed apply publishes verified data; failures and corrected clock retained

Typed immutable input extraction preserves numeric/string/boolean/null roles. The first r231 publisher appends raw records, then its positional journal INSERT fails CAST_INVALID_INPUT because source event field order differs from table order. Native audit confirms exact FAILED query, raw1/current0/journal0/adjacency0/tombstone0 and empty private manifest. The attempted local stop found the controller already terminal; no stop or cancellation success is claimed. GET-only final history audit accounts12,021,315reads/3,100,598writes/0spill including post-failure checks. Failed targets remain retained and are not replayed.

Corrected r232 runs on fresh owned clones with explicit target-column lists for every append. Immutable stageUUID/version8 and all bootstrap sourceUUIDs are checked; source eligibility verified before mutations. Native commit query IDs bind actual published versions, independently of physical maintenance heads. Raw/current/journal/adjacency/tombstone all1; unchanged original nodes0. Current and adjacency each update1,843 and delete205, insert/copy0, add one file/oneDV. Full independent Python all-field multiset digests verify current20,275edges,4,096nodes,26,624rawrecords,102,741propertyevents,205tombstones and20,275adjacency rows. Complete original rows are included, not sampled; final full typed endpoints and edge identity uniqueness pass.

The original descriptor readback completes35.473s after ready-input processing starts. Its schema_revisions_json is empty, so this is a data-integrity-plus-incomplete-metadata clock, not a passing complete publication/freshness screen. r234 verifies both current-role native feed/revision pairs and appends a new immutable corrected descriptor mixed-r234 with identical data vector and schema map synthetic-scale-mixed→synthetic-mixed/1. Original mixed-r232 remains evidence. Corrected readback occurs134.907s of host wall time after original processing_start_epoch, including post-publication diagnostics, intervening audit and metadata repair; assumes no host clock jump and cannot be presented as clean monotonic service time, correction overhead or p95. No60s freshness/sustained admission follows. Earlier stage/clone setup and real source arrivals are excluded from both clocks explicitly.

Executed r232 worker source is preserved under its evidence directory. Future script text is corrected to carry the schema map from the outset; the correction does not retroactively change executed results. This is a private synthetic descriptor experiment, not production fencing/ACK, source transaction, durable recovery or complete full-contract publisher implementation. Stage/source profile and all exact retention meanings remain qualified; no UMF binding.

Successful apply+diagnostics reports108,524,334reads/7,246,434writes/0spill. Descriptor correction adds5,080,141reads/8,899writes/0spill; failure costs are separate above. All final native IDs retained. Active roles after apply: nodes3,313,356bytes,edges18,051,619,raw24,092,027,journal20,871,112,adjacency134,063,tombstones4,145. Active bytes and work writes are not a physical retained-storage bill: shallow-clone baseline dependencies, old versions,DVs/logs/checkpoints, retained failed/raw/stage tables and descriptor rows add costs. No cleanup/expiry/VACUUM or new compute.

Evidence: `out/native/ashlar_mixed_apply_r231/failure-audit.json`, complete-failure-costs/history; `out/native/ashlar_mixed_apply_r232/summary.json`, executed source/queries/histories; `out/native/ashlar_mixed_descriptor_r234/summary.json`. Next consolidate publication phases/role amplification and update concrete layout support limits, including descriptor schema metadata and journal origin clustering versus the full DDL's position profile. Prepare larger-stage reserves from complete source/history/update overlap; do not repeat narrow tests as substitutes for unresolved sustained10k/s/100k/s, controlled cold/caller and1B/5B evidence. UC Delta remains selected and external graph mappings scoped.

## r236–r237: bounded complete-role growth begins, larger reserves revised openly

First200,000-node range0..199999 of the proposed8M-node/40M-edge recipe is now materialized in three owned UC Delta staging tables. Complete200k raw bootstrap origins and800k property events are included. Independent Python all-field sorted SHA256 multiset/cardinality checks pass every role row; full node IDs are unique and span1..200000. Only this node slice is verified, not the remaining7.8M nodes, any edge range or a complete graph descriptor. No manifest/canonical mutation or service/scale admission.

Recorded existing compute remains RUNNING data-gateway2439e1f2e37ac563,serverless2XSmall,min/max1; no new compute/resize. Runtime DBSQL2026.38/EtcUTC, reader3/writer7. Active role bytes: current162,101,976,raw169,389,362,journal147,910,472=479,401,810. Reported reads482,508,865,writes479,401,810,0spill within10GB/2GB bounds with750MB next-role reserve. Independent oracle preparation15.529s; entire slice109.278s, within900s; each native statement has180s cancellation/200s observation bounds and persistent handle recovery. Evidence: `out/native/ashlar_scale_slice_r236/summary.json`, queries and shared final histories.

Each role emits one file, up to169MB. This staging CTAS sets hash/delivery/source-delivery LC and zstd, but does not set the proposed64MiB target or declare NOT NULL/unique constraints. Do not advertise target-size or singleton performance for it. Future canonical profile must measure actual writer output/statistics and keep partial stage data separate from published graph state. Oracle/digest aggregation is bounded to200k nodes/800k events, not a billion-row collect-list.

`scale_reserves_r237.py` produces reproducible `out/scale-reserves-r237.json`. Proposed larger ceiling changes openly from140GB to220GB:173.521GB bootstrap at1.5× measured width,2.120GB for four100k mixed change batches at1.5×,1.071GB input stages at1.5×,20GB assumed additional retained-file overlap,8GB assumed one failed phase and10GB assumed metadata/background margin. Estimated components214.712GB leave5.288GB unallocated. These margins are explicit planning assumptions, not measured8M/40M consumption, complete billing, a bulk-run approval or indefinite compute-time authorization. A2× bootstrap-width case already exceeds this proposal. Full-run wall/price envelope remains unqualified; forty109s node slices alone give roughly73minutes under fixed-service arithmetic, before edges and larger validation. Do not transfer that timing to future slices as measured throughput.

Next bounded node range200000..399999 retains2GB write/10GB read,180s statement and900s slice bounds, with exact UUID/range/commit-version/field checks and explicit append columns. Investigate reuse of verified immutable native carrier output for raw/history bootstrap extraction to avoid recomputing opaque generation per role, then verify against the independent oracle before extending that path. Account all owned growth work across slices, failed work and retained dependencies before each admission. Only after full8M nodes/40M edges, all source/history/adjacency roles and typed endpoint closure may any graph descriptor advance; eventual1B/5B and full ingest/read gates remain intact. No source ACK, expiry/VACUUM or UMF binding inferred.

## r238–r239: carrier reuse qualified; complete staging prefix reaches400k nodes

The native role helper adds a validated pinned-table/version path while preserving byte-identical default generated SQL fingerprints. Read-only reuse at the verified200k current snapshot reproduces every raw-record/property-event field digest from the independent Python oracle:200k raw records and800k events;309,201,983reported reads,0write/spill, within2GB. This applies only to the deterministic synthetic bootstrap recipe. It does not authorize reconstructing real raw source evidence from parsed current state or general property-event extraction. Evidence: `out/native/ashlar_carrier_reuse_r238/summary.json`, default SQL fingerprints.

Next200k ordinal range200000..399999 appends through explicitly named columns to the same owned staging identities. Before admission, all prior role rows match their original independent digests at observed physical snapshots. Current commit statement ID binds the new logical version; raw/history generation reuses exactly that snapshot filtered to the new ID range. Complete accumulated-prefix Python oracles then compare all400knodes,400krawrecords and1.6Mpropertyevents, not just the newly appended slice. Native IDs unique1..400000. Every check passes; commit-linked logical snapshots are1 for each role. No partial graph descriptor advances and no edge range exists in this staging graph yet.

Reported r239 reads1,256,397,642/writes468,234,209/0spill, within10GB/2GB ceilings with750MB next-role reserve. Oracle preparation30.962s, whole slice126.731s under900s. Current/raw/journal active physical details show320,817,963/332,723,854/294,094,202bytes,3files each. First slice plus append has947,636,019active bytes; work counters include separate reuse/preflight/validation reads and do not price billing or old retained versions. No compute change/cleanup or expiry. Growth is still400k of8M planned nodes, not graph/billion admission.

Raw r239 summary retained initial200k/version0 table-detail fields alongside new versions/checks. `audited-summary.json` separates those initial details from current commit-linked logical table metadata and later observed active physical details; measurements/results are unchanged. Native mutation/query evidence remains original. Evidence: `out/native/ashlar_scale_growth_r239/audited-summary.json`, exact append commits/queries and final histories.

Do not claim a throughput improvement from126.731s versus prior109.278s: the second controller independently verifies the old prefix and a doubled complete-prefix oracle, and reuse adds native carrier reads. This is exact-value/complete-prefix qualification plus incremental file/cost evidence, not randomized performance causation or sustained ingestion. Next measure emitted hash ranges/file pruning and plan bounded canonical file maintenance before more accumulated growth; the staging profile did not set64MiB target and append file shape cannot be assumed clustered from table declaration alone. Preserve full8M/40M and eventual1B/5B requirements and original read/ingest gates; UMF deferred.

## r240: accumulated node file ranges and full-row singleton reads

Read-only measurement pins growth_object_current_r236 to UUID ea6b0cbe-28fc-426c-a52b-15844ef14e91, version1,400k nodes. Complete live-file row counts sum to400k. Three files contain200000/103840/96160 rows and162101976/82174560/76541427 bytes; their live lookup-hash extrema span approximately100%/52%/48% of the hash domain. These are scanned live values, not stored Delta statistics. The first append file overlaps the two later range-shaped files, so the clustering declaration alone does not establish globally nonoverlapping ranges.

Twenty interleaved full17-field singleton reads (ten SHA-ranked ordinals from each200k range) match the independent oracle exactly. Sample nearest-rank p95: caller385.968ms, engine119ms, compilation163ms, one file and162615383 reported read bytes. Zero remote-query reads were reported. Cache-result reuse was disabled; preceding complete-prefix validation and file scan may warm data caches. This is a bounded staging cohort, not controlled cold or service p95, causal clustering evidence, graph publication or billion-scale admission. Warm provisional engine100ms/caller250ms targets remain unmet in this cohort; UC Delta architecture remains selected.

Final native telemetry reports1984574359 read bytes, zero writes/spill, below8GB read ceiling. Evidence: `out/native/ashlar_growth_pruning_r240/summary.json`, exact statements and final shared history; `growth_pruning_r240.py` is the reproducible controller. No maintenance, compute modification, source ACK or manifest advance. Next compare bounded maintenance/file shaping on owned staging snapshots, retaining the current result as the before measurement, then continue complete node/edge/raw/history/adjacency growth.400k staged nodes still do not constitute the planned8M/40M graph or final1B/5B scale qualification; UMF remains deferred.

## r241: owned maintenance comparison preserves full prefix and improves pruning

Fresh owned shallow clone `growth_object_shaped_r241` of original400k-node version1 (UUID8a8e73bc-e294-4488-994b-284e3d5235bd) sets delta.targetFileSize67108864 and executes OPTIMIZE FULL. Resulting logical snapshot3 passes the independently generated full400k-row/all17-field digest from r239. Original staging tables and publication remain unchanged. Maintenance caller wall12.924s; final total work reads2923760089/writes317793105bytes/0spill, within8GB/1GB comparison bounds. Native final histories and exact commands retained;180s session statement timeout and existing warehouse used.

Four emitted files contain108024/102486/99060/90430 rows, totaling317793105bytes, with actual sizes85.59/81.49/78.71/72.01MB.64MiB is a target, not a guaranteed maximum. Scanned live hash extrema show four nonoverlapping ranges spanning27.0%/25.5%/24.9%/22.7% of hash domain; these are not stored statistics. Same20 interleaved full-row keys all independently match. Sample p95 caller396.510ms, engine108ms, compilation175ms, one file/read85815324bytes; zero queries report remote reads. Result-cache reuse disabled, data may be warm after preservation/file scans.

Compared with r240, read-byte p95 drops approximately47.2% while caller p95 does not improve (385.968→396.510ms). This sequential single-cohort comparison establishes actual pruning and preservation, not causal latency significance or controlled cold/service p95. Engine100ms/caller250ms provisional gates remain unmet. Maintenance produces useful file ranges but cannot alone establish caller budget or sustain append-time pruning. Repeated accumulated-prefix growth, maintenance amortization and concurrent reads remain necessary. No full graph descriptor, real source authority, billion-scale admission, UMF binding, source ACK or expiry inferred.

Next extend complete graph growth with independently verified bounded ranges and explicit accounting of carrier/raw/journal/adjacency writes; use current before/after file results to measure maintenance cadence rather than assuming clustering declarations guarantee append file shape. Publication clocks must include the actual validation and role barrier. Keep eventual1B/5B targets intact;400k nodes and this maintenance comparison do not satisfy them.

## r242: complete accumulated node prefix reaches600k

Third200k-node ordinal range400000..599999 appends to the same owned current/raw/journal staging identities. Before mutation, prior400k complete role digests pass at observed snapshots. Named-column writes and commit-statement IDs bind new logical snapshot2 for each role; raw/journal generation reads only the new range from pinned current snapshot2. Independent Python all-field multiset digests then verify every accumulated600k current node,600k raw envelope and2.4M property event. Native identity check proves600k distinct IDs1..600000. SHA256 collision assumptions remain explicit; source recipe is synthetic, not authority for reconstructing real raw evidence.

Final reported reads1726705126/writes468134301bytes/0spill, within10GB/2GB bounds. Active current/raw/journal bytes479533315/496066840/440170165, five files each, total1415770320bytes. Oracle preparation46.100s; whole slice163.699s under900s, statement cancel bound180s. These include full-prefix validation and are not steady ingestion throughput. Original400k maintained clone remains available as a read baseline; this append does not maintain the growth tables or advance any publication manifest.600k staged nodes are only7.5% of planned8M; no edges in this new graph. Final1B/5B, controlled cold/native caller, concurrent reads and sustained freshness remain unproved.

Evidence: `out/native/ashlar_scale_growth_r242/audited-summary.json`, complete exact statements/commit histories and final shared native telemetry. Audited summary separates inherited first-slice details from current commit-linked table/version/row metadata and later active physical details. Original worker output remains intact.

Before larger admission, replace growing single collect-list digests with bounded independent ordinal-range digests plus complete membership/total-count checks at one pinned snapshot. Bound raw ranges through strict synthetic delivery identity or verified carrier linkage, explicitly reject malformed/out-of-range/duplicate memberships, and preserve exact fields. This avoids validation memory increasing with the whole dataset while retaining complete coverage. Add complete edge/raw/journal/adjacency staging and typed endpoint closure before any graph publication; do not omit roles to make scale appear cheaper. Keep storage, failed-work, retained-version and maintenance cost reserves explicit. UMF deferred.

## r243: bounded complete-range verification qualified on native600k prefix

`range_verification_r243.py` produces independent100k-entity ordinal-range oracles, freeing each range's row-hash arrays before advancing. Native queries aggregate one range at a time; each bootstrap history range contains400k events for this recipe. Full role counts at the same pinned snapshot plus exact cardinality and all-field multiset digests for every disjoint range establish complete coverage under SHA256 collision assumptions. Raw membership uses strict synthetic delivery vocabulary node:(0|[1-9][0-9]*):1 with try_cast ordinal; malformed/null/out-of-prefix rows cannot hide because full counts must equal the sum of all verified range counts. This is fixture-specific bootstrap verification, not a general producer identity contract.

Read-only qualification `native_ranges_r243.py` verifies all6 ranges over600k current/raw and2.4M journal rows at original staging UUIDs/snapshot2, independently agreeing with prior whole-prefix verification. Every role field is included, including exact JSON/retained/cursor/null content. Local partial final-range membership and unsafe-table-identifier refusal pass. Native exact query/results and final histories retained in `out/native/ashlar_range_verification_r243/`. Oracle47.446s, complete142.677s; reads3915318643bytes, zero writes/spill, below20GB cap and900s run bound. Statements have SDK180s cancellation bound; each aggregation is bounded, while native executor parallel memory is not directly measured. Full-range scans are repeated and costs must remain explicit. No ingestion, controlled cold or latency gate claimed.

Next growth admission proposal increases node prefix600k→1M (400k new entities), retaining complete raw/journal roles and pinned carrier reuse. Observed r242 new200k roles wrote468134301bytes: simple same-width400k arithmetic936268602bytes, below2GB per-iteration write ceiling with750MB next-phase reserve; not a throughput or byte guarantee. Existing range verification3.915GB scaled arithmetically to1M is6.526GB; prior-prefix verification plus new-prefix checks, carrier reads and control scans motivate20GB read ceiling rather than silently retaining10GB.900s iteration/180s statement cancellation bounds remain. Full-field oracles, commit-ID version binding, full counts, disjoint membership and identity closure required before admission advances; abort further phases when remaining bounds cannot accommodate reserve. No graph publication until full nodes/edges/raw/history/adjacency/typed endpoints verified; proposed8M/40M and owner1B/5B remain distinct. No indefinite run, compute resizing, source ACK or UMF binding implied.

## r244: complete-role node staging reaches1M with bounded field verification

Disjoint ordinal600000..999999 append adds400k nodes to the same owned staging IDs. Prior600k complete range/role count checks pass at observed snapshots before writes. Explicit insert columns and statement-ID-bound commits establish version3 for current/raw/journal; raw/history generation uses exactly current3 filtered to the appended range. All10 independent100k-entity ranges match every field for1M current nodes,1M raw envelopes and4M property events. Complete role counts exclude missing/extra/malformed-range memberships, and native IDs are unique1..1000000. No graph manifest advances; this new staging graph still has no edges. Original400k maintained clone remains untouched.

Evidence `out/native/ashlar_scale_growth_r244/audited-summary.json` preserves original worker summary and adds native result-cache audit.21 queries reused cached results on pinned prior-prefix checks/counts. These remain valid for immutable-snapshot correctness but preflight timing/read counters are not fresh-scan or ingestion performance. New append/final-version evidence retained with exact queries and final native histories. Oracle78.181s; whole353.899s. Final reads10045163157/writes936802512bytes/0spill, under20GB/2GB and900s bounds with750MB phase reserve. SDK cancellation bound180s per statement. Active current797270256/raw822868976/journal732433600bytes,9files each, total2352572832bytes. Work counters do not fully price retained versions, compute or billing.

1M is12.5% of planned8M nodes; eventual1B/5B and full8M/40M role/endpoint/publication qualification remain unproved. No read/ingest SLO inferred from the bootstrap controller. Warm engine/caller, controlled cold, concurrent reads, sustained freshness and real producer version fencing remain open. UC managed Delta architecture stays selected; UMF deferred.

Next reduce validation read amplification before extending growth. Range-at-a-time aggregation bounds individual arrays but can repeatedly scan raw/history files whose clustering does not directly prune ordinal expressions. Compare one native grouped scan producing the same independent range cardinalities/digests, with complete membership counts and explicitly bounded number/size of groups; record actual executor spill/work and do not claim total native memory is bounded merely because one group is bounded. Retain the current range outputs as the oracle for comparison. This change must preserve exact complete coverage, not substitute sampling or omit role fields. Then resume node growth and complete edge/raw/history/adjacency coverage with typed endpoint closure, measured maintenance and publication barriers.
