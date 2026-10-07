# Full current-table partition comparison: r308

Governing artifacts remain ADR-001, CONTRACT-003 and CONTRACT-001/002.
The owner-selected UC Delta architecture and exact Truss-compatible carriers
remain fixed. This candidate changes only physical access, not semantic identity,
property/retained strings, typed endpoints or independent raw/journal history.

Compare the entire39.99M-edge maintained version4 with32fixed contiguous
lookup_hash range partitions and ZORDER(lookup_hash) within each partition.
The partition is the first hash byte divided by8; bind it alongside fullhash
and original source/type/id predicates.32partitions are about1GB each by observed
byte arithmetic. This avoids one partition per type, entity or changing batch.
Current-table hash distribution supports hard partition pruning, while ZORDER
is tested as within-partition file layout; never combine it with liquid clustering.

The measured two-reader cohort had624ms caller/412ms compile p95. A partition
boundary is a hypothesis for reducing candidate metadata/planning, not a claimed
speedup. Existing small bucket results also showed costly scattered ingest;
repeat mixed publication and include maintenance before selecting this layout.
Fullwidth carrier output is mandatory; a narrow key-only benchmark is insufficient.

The SQL and JSON budget describe a full-table CTAS and staged maintenance on
existing compute. All32partitions and all39.99Mcarrier digests must pass before
claiming a whole-layout result. An interrupted or favorable single partition
cannot qualify the table. Actual protocol features must be recorded: partitioning
alone does not establish PuppyGraph/GraphFrames/Fabric reader compatibility.
Those tools still use the bounded compatible projections and feature qualifiers;
UMF binding remains deferred.

Costs are larger than the scoped experiment and are explicit: up to80GBreported
writes/400GBreads/10GBspill/2400seconds, with180-secondstatement guards. The
additional retained overlap sensitivity is76.1GB, distinct from the prior280GB
proposal; there is no physical inventory, price or billion-scale admission claim.
No new compute, resizing, real producer ACK, pointer publication or VACUUM is
part of the comparison. Continue stages only with exact native receipts and
remaining measured headroom. Success still requires singleton, cold/concurrent
and subsequent ingestion/publication evidence against the original targets.
