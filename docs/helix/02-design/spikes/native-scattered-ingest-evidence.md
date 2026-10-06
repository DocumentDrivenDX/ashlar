# Scattered-update ingest evidence

2026-10-05; SPIKE-001 experiment, not layout approval. Synthetic data only, existing dbw-aidev-cus data-gateway serverless Photon PRO 2X-Small warehouse; no compute or global preview setting changes. Full CONTRACT-003 canonical object, property-journal and manifest schema; catalogManaged transactional fixture remains experimental.

## Workload and result

`native_scattered_ingest.py` populated an isolated 10M-object table from i1 object_current version 2, then ran OPTIMIZE FULL. Canonical clustering is source_system,type_id,id with a 16MiB target; journal target is 128MiB. Baseline detail reported **419 files, 6,432,571,512 bytes**. Copy took 64.79s and clustering 77.80s, outside publication timing. Background OPTIMIZE operations appear in table history; the publication pins actual version 6 rather than assuming a fixed version increment.

The batch selects `pmod((id-1)*104729,10000000)<200000`. The multiplier is coprime to the domain size, giving 200k distinct IDs spread across all 100 contiguous 100k-ID segments. Verified minimum ID 1, maximum 9,999,907. IDs preserve fixture identity; the selector does not remap them. Producer payloads were prebuilt before arrivals; timed staging copied them into a new stage. New property 103 uses 32 SHA256 hex chunks (~2KB varied text); other properties and retained content remain exact.

One batch models 200k changes accumulated over 20s at provisional 10k/s. Timed staging, exact prior-state checks, canonical MERGE, journal insertion and decoding checks, changed-set checks, receipt, version discovery and manifest are included. Publication completed **90.96s after batch readiness**, or **110.96s from the oldest modeled arrival**, failing the 60s freshness target. Staging wall time was 4.02s. The atomic block took 84.62s wall; query history reported 84.13s execution, 17,742,174,092 read bytes and 53,604,938 rows scanned. These are aggregate scripting metrics, not isolated MERGE cost or a physical candidate-work count.

Object history version 6 reports the intermediate MERGE separately: **6,749ms execution**, 2,000ms scan, 4,740ms rewrite, 200k updated rows, zero copied/inserted/deleted rows, **419 deletion vectors added**, **8 data files / 214,418,424 bytes added**, zero data files removed. This rules out attributing the entire atomic duration to full data-file rewriting. Remaining block cost includes repeated integrity queries and journal work; this run does not isolate their individual costs. Post-run detail: 427 files, 6,646,989,936 bytes.

## Preservation evidence

The timed transaction checks all changed values and exact old/new journal JSON tokens. Published object version 6 and journal version 1 were captured in the manifest. Post-publication hydration matched all **200k changed rows** with zero mismatches, including full property/retained carriers, logical keys, schema revision, source ordering and scalar projection. `verify_scattered_ingest.py` separately proved:

- 10M distinct canonical IDs, range 1..10M.
- All **9.8M untouched rows match all 13 canonical columns** against the fixed seed snapshot.
- 200k distinct journal event keys.
- All 100 key-range segments contain changed IDs.

Both harness processes completed successfully. Raw statements, IDs, result arrays, timing and history are retained under `SPIKE-001-table-layout/out/native/ashlar_scattered_20261005_n1/` and `ashlar_scattered_verify_20261005_n1/`.

## Disposition

The earlier 1M-table contiguous batching result does not admit scattered 10M-table ingest. This is one batch, not sustained or population-p95 evidence; no concurrent reads, edges, replay/deletion, native Truss feed, failure recovery or billion-scale admission is claimed. The source fixture reuses its varied payload pool across 10M nodes and remains a synthetic entropy model. No attributable cloud cost is available; setup/runtime/file totals are not billed spend.

Next intervention: combine redundant cardinality and mismatch validations, preserving prior-state checks, exact carriers, journal tokens and the publication boundary. Measure its full freshness clock against this scattered baseline. The measured MERGE is already much faster than the surrounding block; an append overlay is not justified as the next intervention merely from aggregate atomic timing. Keep canonical and adjacency physical choices proposed until ingest, native singleton, concurrent-read and full-scale gates pass.
