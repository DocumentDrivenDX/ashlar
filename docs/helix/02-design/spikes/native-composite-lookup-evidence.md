# Composite physical lookup-key baseline

2026-10-05; SPIKE-001 experiment, not generic layout approval or goal completion.

## Physical candidate

The canonical source/type/native-ID tuple stays unchanged. An additional `lookup_hash STRING` is SHA256 hex over native SQL's fixed-order JSON struct `(source_system,type_id,id)`, with BIGINT numeric inputs. The table clusters on this one physical column; statistics also cover the native tuple. Hash equality is an auxiliary pruning predicate and **must always be accompanied by exact source/type/id predicates**. Hash must not become graph identity, an endpoint join key, or a uniqueness constraint. A collision can add candidate work but cannot authorize another identity's row. Invalid derivation can hide a row from pruning lookup, so the publisher must validate every derived value before declaring a publication valid.

Exact experimentally executed DDL is retained separately in [composite candidate](SPIKE-001-table-layout/sql/composite-lookup-candidate.sql); proposed normative CONTRACT-003 remains unchanged. This adds one explicit physical column to the 13-column canonical surface. Graph projections continue to use native identity and the contract's graph encoding; the hash is not a graph property or a UMF binding.

## Mixed-domain correctness

`native_composite_lookup.py` creates 10M synthetic nodes from a fixed 1M-row carrier pool: five source names (`pilot:0`..`pilot:4`), two types per source, native IDs 1..1M in each domain. This deliberately overlaps every ID across ten source/type domains. Domain/feed labels are synthetic; no real Truss catalog or producer transformation is claimed.

Native checks prove 10M distinct typed identity tuples, 1M distinct native IDs, and zero wrongly derived physical keys. All 10M property/retained/logical-key/schema carriers match the fixed source pool. Seven independent Python/native derivation controls pass, covering delimiter variations, Unicode, apostrophes, empty source text and signed64 minimum/maximum. These test physical derivation, not authorization of those source strings/catalog values in every profile. A forced-collision virtual fixture shows that exact native predicates distinguish three rows sharing the same artificial hash.

The initial p1 attempt stopped at the apostrophe control before any table DDL/population: SQL literal escaping changed the intended source text. The transport was corrected to UTF-8 hex decoding, and a fresh p1r1 schema passed all controls. Original evidence is retained; no unknown write was retried.

## Native layout and singleton measurements

Existing dbw-aidev-cus data-gateway serverless Photon PRO 2X-Small warehouse; unchanged settings. Population takes **105.32s**, explicit OPTIMIZE FULL **15.50s**, outside read timing. Baseline version **2** has **256 files / 6,776,161,947 bytes**, clustered by lookup_hash with a 16MiB target. These are setup/active-file observations, not attributable cloud cost.

The same 51 keys across all ten domains run in prime/repeat/repeat2; zero-index rep is excluded, giving 50 measured reads per phase. Each query returns exactly its native tuple and identical carriers across repetitions. Every measured singleton reads **one file**; read-byte p95 **30,510,720**. Result-cache hits are zero. Metrics are refreshed on the same 171 completed statements; queries are not rerun to fill telemetry.

| Phase | Engine p95 ms | Caller p95 ms | Remote-reading samples |
| --- | ---: | ---: | ---: |
| Prime | 211 | 451.3 | 4 / 50 |
| Repeat | 92 | 367.4 | 0 / 50 |
| Repeat2 | 81 | 381.5 | 0 / 50 |

Both fully cached repeats pass the 100ms engine screen. Both fail the 250ms caller target. Prime is mixed-cache, not a cold population. This is not a clean paired comparison with other layouts: domain/key/projection/file states differ. It proves bounded native mixed-domain correctness and pruning, not generic or billion-scale admission.

Raw evidence: `SPIKE-001-table-layout/out/native/ashlar_composite_20261005_p1r1/`; original boundary failure under `ashlar_composite_20261005_p1/`.

## Next gate

Run scattered multi-domain publications with correct per-event origin keys, exact canonical/journal checks and hash integrity, then measure reads during publication and immediate post-write pruning. Compare atomic replacement and MERGE on this candidate rather than assuming the favorable static layout survives ingest. Preserve the real node and edge identities; extend to edge identity and adjacency only with dedicated evidence.

Sustained ingest, burst, caller latency, concurrent-read p95, full edge scale, native source feed/recovery, protocol support in external engines, resource/cost bounds and 1B-node/5B-edge performance remain open. A fixed 10M snapshot does not complete the goal. UMF binding remains deferred.
