# Actual current-file overlap and conditional edge capacity (r487–r488)

One bounded read-only native query measures E8 live physical file paths/sizes/row counts/hash extrema.606unique files,39.95Mlive rows,32,318,236,099physical file bytes. Native statement successful/final, exactSQL/result receipt independently audited;7.726s/2,192,505,498read bytes/0write/spill, within5GB/180s bounds. Current warehouse settings unchanged; Small approval remains pending. No production/data/compute mutation.

Six files span>90%of the hash domain, totaling217,690,053bytes. Sum of all live interval spans15.356domains;100sorted-rank sampled fifth hashes intersect4–16intervals (median16). Exact100k fifth hashes intersect all606intervals/32.32GB. This supports the complete-target coverage conclusion behind rejecting range scheduling; fine arrival microbatches cannot promise low total scanned bytes merely because individual source ranges prune.

These are live-row extrema, not persisted Delta statistics, native query-selected files, or exact physical pruning evidence. Deleted-row stats can be wider; full typed tuple/other-column predicates can prune beyond hash intervals. Do not equate modeled median16intervals with native singleton median8files, blame all extra files on six wide intervals, or treat physical sizes as projected-column read bytes. Exact carrier preservation remains qualified separately by full publication/CDF evidence.

Conditional linear extrapolation of this same row mix/entropy/file geometry to5Bedges yields~75,845current-edge files/4,044,835,556,821bytes (4.04TBdecimal). This excludes1Bnodes,24B-scale bootstrap/property history, raw records, adjacency, Delta logs/deletion vectors/retained versions/staging/replicas/maintenance and compute. It is not a measured5Btable or a revised fullgraph storage budget; earlier complete-role capacity planning remains separately qualified. Actual1B/5B and steady/burst/latency gates are still open.

Next requires a material ingest/maintenance/capacity intervention with complete-source and publication qualification, not another unmodified100kbatch or a claim that arithmetic meets the goal. Current233.213s publication and repeat131msengine/481mscaller fail provisional targets. The reviewed bounded Small read-capacity trial r484 awaits authorization to change shared settings/billing; no implicit resize. Existing-settings design work can continue meanwhile. Truss exact semantics, scoped graph mappings and deferred UMF unchanged.

Evidence: out/native/ashlar_current_file_ranges_r487/ and out/file-overlap-model-r488.json.
