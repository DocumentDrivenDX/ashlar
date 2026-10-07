# Current MERGE plan observation — R559–561

Two bounded EXPLAIN FORMATTED statements completed on existing compute, against
current edge head9 and immutable sixth inputs0. No MERGE executed. The inputs'
before-images are stale against head9; these retained SQL statements must never
be executed as another batch. The historical simple comparator lacks the full
predecessor guard and is not an admissible publisher.

Both plans expose Execute MergeIntoCommandEdge rather than the internal executed
scan/join tree. They therefore do not support attributing the historical cost to
a specific join, filter placement or Photon operator. Four native statements,
exact SQL/result text hashes and final metrics pass audit. Observation wall is
6.593957750 seconds; reported read/write/spill are0/0/0. This does not prove zero
catalog I/O or zero charge.

The completed sixth MERGE reports63975ms execution,61997ms metadata,
35545538173 bytes read (31861052206 remote),1226 read files,9 pruned files,
80633840 rows scanned and219668744 bytes written. Timings are nested, not
additive. The counters include query-wide work and potentially repeated scans;
1226 must not be described as distinct target files or compared directly to the
608-file final table as a coverage ratio. Metadata time does not establish that
catalog latency alone caused the delay. The full historical metrics are preserved
in the independently audited receipt, including native runtime and statement ID.

This rules out treating command-only EXPLAIN as sufficient evidence to choose a
MERGE hint or drop guards. Next isolate the normalized source relation's complete
100k-row materialization and initial relational plan, retaining exact before/
after carriers, delivery links, update/delete partition and source digests. A
bounded matched pilot may then compare a pre-materialized source with the inline
source, charging preparation to publication; a scan-only improvement cannot be
promoted to a full MERGE claim. Prior typed42 preparation and hash subdivision
results remain negative evidence, so repeat only with a distinct justified change.
The full221.83-second publication clock and all open performance/scale gates
remain unchanged. UC Delta remains selected and no UMF binding is introduced.
