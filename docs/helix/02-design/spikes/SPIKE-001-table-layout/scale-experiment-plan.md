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
