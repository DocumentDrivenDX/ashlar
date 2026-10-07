# Conditional throughput envelope for1Bnodes/5Bedges (r489)

This executed local arithmetic and deterministic virtual-file simulation binds exact native E8geometry, complete fifth publication, native input-stage and local byte receipts by SHA. It does not create5Bdata, run sustained ingestion, alter compute, relax gates or approve architecture changes. Simulation1.904s; analytical uniform occupancy agrees with deterministic simulated fractions within1percentage point. This validates the model implementation, not its applicability to production.

The complete100k ready-input clock233.213s gives428.793changes/s when divided into a single serial batch.10k/s is23.321times that rate. This is not measured steady throughput: source accumulation/preparation, concurrent publishers, backpressure and real producer authority remain untested. CurrentMERGE reported read rate0.511GB/caller-second also mixes engine/driver/storage-cache/metadata effects, not a calibrated hardware bandwidth limit.

Conditional5Bcurrent-edge shape is75,845files/4.04TB. Assume ideal uniform changes over disjoint equal-size files, less overlap than the measured live interval structure; this is a simplified planning case rather than a guaranteed bound. At10k/s,600kchanges in60s touch99.9633%expected files (simulation99.9578%). Full-file coverage divided by60s yields67.39GB/sconditional demand, not projected-column/remote read demand or a proven lower bound. At1k/s,60kchanges touch54.665%; burst100k/s over60s touches effectively all files. Burst duration is still unspecified;60s is an explicit modeling scenario, not an agreed requirement.

Measured source/write ratios imply the following **conditional** rates if the same complete update/delete mix and entropy persist:

| Rate | Exact source JSONL | Normalized stage writes | Canonical role writes |
|---|---:|---:|---:|
|10kchanges/s|110.06MB/s|73.40MB/s|50.33MB/s|
|100kchanges/s|1.101GB/s|734.02MB/s|503.30MB/s|

These exclude source accumulation, nodes, growing property/raw history, retained versions, logs/deletion vectors, replica/maintenance/projection/validation traffic and other costs. Do not combine these ratios with read bandwidth as a guaranteed required warehouse size or monetary price. Payload mix and hot-key concentration from a real consumer remain unknown. Hash clustering randomizes even identity-local change sets, so locality must be modeled from actual keys rather than assumed from arrival batches.

Keep UC Delta and exact Truss/current/source/journal/tombstone/typed projection design selected/proposed. Do not silently revise60s/10k/s/100k/s/1B/5B targets or declare them achieved. The next material comparison should quantify capacity sensitivity under reviewed Small trial r484 (approval still pending), followed by an ingest intervention qualified at full ready-input/publication scope. A Small read test cannot itself admit fresh publication, sustained/burst performance or billion scale. Larger scale advances require measured full-role resource/retention bounds and an owner-approved cost envelope.

Potential interventions must retain exact semantics and separately prove their tradeoffs: narrow validated mutation metadata vs complete carriers; parallel independent role validation with submitted-commit custody; asynchronous immutable deltas/compaction with bounded overlay point-read cost; workload-specific identity locality where a producer supports it. Existing overlay and range experiments do not meet caller/freshness gates, so none is promoted by this model. Production writer fencing and external engine limits remain explicit. UMF stays deferred.

Evidence: throughput_envelope_r489.py and out/throughput-envelope-r489.json. All sources are previous successful native/local receipts; no unexecuted plan is counted as performance evidence.
