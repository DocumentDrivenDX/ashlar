# Current carrier file fanout: r297 disposition

Governing artifacts: ADR-001, CONTRACT-003, CONTRACT-001 and CONTRACT-002.
This supplements their proposed physical design; source semantics, publication
boundaries and the selected Unity Catalog Delta architecture remain governing.

The r296 matched read test preserves every predecessor field for100k scattered
keys over40M edges. Default join reads33.502GB in47.534seconds; broadcast reads
32.423GB in29.976seconds. Both report zero pruned files. Different cache, remote
reads and cloud retry time prevent causal latency attribution. Broadcasting is
not yet qualified for actual MERGE or wide replacement-carrier memory use.

The local r297 analysis reuses all614 live file ranges from the separately
qualified post-r281 snapshot. Every range contains at least one selected hash;
their total active bytes are32,038,095,270. Eleven files span over99% of the hash
domain and total72,709,215bytes. There are1,999,726 key/range intersections.
Range inclusion does not establish actual row membership, physical reads,
Delta stored statistics or original pre-MERGE scan coverage. The source snapshot
is explicit; this is an explanatory comparison, not a matched causal test.

## Physical design consequence

Keep exact native source/type/id predicates with lookup_hash as a physical
access key, never semantic identity. Keep full opaque property and retained
strings, versioned typed endpoints and separate raw/journal history. Serving
projections must not replace these carriers or erase exact values. UMF remains
deferred; graph tools continue to consume explicit compatible bounded exports.

Treat bounded current-file overlap and repeated write/maintenance cost as a
joint layout requirement. A scattered batch can legitimately touch most files;
a join hint cannot turn that workload into a singleton. Initial liquid-clustered
append files and newly emitted MERGE files must be measured separately. Smaller
files may improve singleton work but enlarge metadata and batch fanout; larger
files reduce file count but increase singleton read bytes. Neither quotient nor
an isolated OPTIMIZE latency establishes the final operating point.

Do not promote broadcast,64MiB file settings or bucket partitioning from this
read-only result. Prior maintenance cancellation provides no qualified compacted
carrier. A maintenance experiment must retain its own candidate, all before/after
fields and exact Delta versions, followed by both matched singleton and mixed
publication measurements; charge maintenance and retained overlap explicitly.
Avoid repeating a canceled full rewrite under the same budget without a new
resource rationale. No automatic VACUUM or source ACK follows these experiments.

## Next comparison and limits

Before another40M rewrite, use the observed file ranges to choose a limited
maintenance scope and establish whether the runtime supports it on liquid
clustering. Compare a pinned baseline and qualified candidate, retaining native
terminal receipts, all changed fields, unaffected custody, file ranges, warm
singleton cohorts and full publication clocks. A partial-range maintenance
result does not qualify an unmaintained whole-table singleton distribution.
A hash-bucket comparison, if pursued, must include the entire equivalent graph
and ingestion/maintenance cost rather than a favorable subset.

The occupancy arithmetic in out/file-fanout-r297.json is explicitly hypothetical:
equal disjoint files, uniform independent keys and64MiB candidate bytes. It is
not evidence of billion-node admission or actual physical I/O. Remaining work
includes repeated batches/concurrent reads, controlled cold-data singleton
measurements, production writer fencing, direct graph-reader feature support and
realistic retained storage. Existing provisional latency/freshness targets are
still unmet or unproved; no architectural gate is inferred from those misses.
