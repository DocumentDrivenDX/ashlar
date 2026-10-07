# Private index/carrier layout disposition R514

Experimental evidence; canonical ashlar-delta/0.3 remains unchanged.

R513 separates immutable complete 20-field carriers from an 11-field current index. The index preserves full typed identity, lifecycle version, carrier version/digest, deletion state and source origin. A guarded index MERGE rejects changed predecessor tokens; append-only Delta rejects carrier mutation. The version-pinned join reproduces all 2,498,028 reference current rows without a full-field mismatch. Carrier CDF contains exactly 5,609 expected inserts; index CDF contains exactly 12,454 expected pre/post images. Index retains 618 deletion markers. Both refusal controls leave the tested heads unchanged. R514 checks all 31 native statement texts, terminal outcomes, final metrics, code digest and complete result receipts.

For 6,227 changes, carrier append plus index MERGE reads 641,205,464 bytes and writes 5,996,898 bytes with zero spill. Combined engine time is 9.637s; combined caller time is 14.230s. The earlier wide fingerprint MERGE reads 2,354,369,643 bytes, writes 14,419,251 bytes and takes 6.533s engine / 7.240s caller. Thus this pilot reduces mutation I/O but does not improve elapsed latency. Different physical layouts and fixed execution order preclude a causal speed or p95 claim. Include both phases when comparing; do not report index MERGE alone as publication cost.

The entire experiment, including setup and proofs, reads 7,418,380,218 bytes and writes 202,873,091 bytes in 143.514s. Final index occupies 197,356,068 bytes; carrier occupies 2,090,120,482 bytes. The carrier shallow clone shares baseline files, so this is logical table inventory rather than newly allocated baseline storage. Immutable carrier history grows with changes; retention and compaction need an explicit design.

This remains a candidate physical layout. The additional join affects singleton latency and external consumer mapping. Cross-table append/index publication requires explicit pins, orphan handling and writer fencing; the index digest is not a native foreign-key constraint. Generated-column protocol support adds consumer compatibility work. The fixture's delivery IDs are globally unique; its raw-origin join does not establish a general multi-feed join contract. Permanent journal/raw/tombstone contracts remain required. No production ACK, canonical publication, consumer qualification, 60-second freshness, or billion-scale admission follows from this test.

Next: measure complete singleton reads through the pinned index/carrier join, then decide whether the I/O reduction justifies its latency, storage and consumer costs before testing an integrated publication. Keep UC Delta as the architectural commitment and keep UMF binding deferred.

R512 separately reobserves existing R509 query IDs. All executions finished; four metric records remain nonfinal. Their 1,213,787,131 reported read bytes remain provisional and are not silently discarded.
