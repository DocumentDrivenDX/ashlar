# Private index maintenance disposition R525

Experimental physical-layout evidence; canonical ashlar-delta/0.3 remains unchanged.

R522 submitted one OPTIMIZE of current_index_r513 after UUID/head1, hash clustering and64MiB-target verification. The command succeeded but the harness stopped because it assumed exactly one new version. Native history contains two OPTIMIZE commits, versions2 and3, with the same exact statement ID. Version2 rewrites the index; version3 is a post-optimize compaction commit with zero added/removed files. R524 inspects the existing handles and both commits without replaying OPTIMIZE. It compares every11-field index row at version1 versus3, verifies2,498,646 unique typed identities and618 deletion markers, and checks that CDF across both maintenance versions is empty. Exact lexical digest/origin fields remain intact. The original stopped whole wall clock is not captured; its recorded zero cost was preflight-only and is not a final cost claim.

Final maintenance-plus-proof costs are964,201,039 read bytes,181,284,194 written and zero spill, all inside5GB/1GB bounds. R524 recovery wall is12.587s; it is not the total maintenance wall. Index inventory falls from197,356,068 to181,284,194 bytes and remains two files. The two output files are89,132,840 and92,151,354 bytes: the declared64MiB target is not an enforced physical maximum. No original files were deleted or vacuumed by the harness.

R523 repeats the same complete singleton query as R517, changing only the pinned index version1 to qualified version3. Carrier remains version2 and wide current version3. All32 full20-field paired responses and deletion absences match, with identical opening/closing three-table custody. Cache results are disabled; all point queries have zero remote bytes. Same eight identities/order and warm storage history remain, so this is a small serial sample and not a causal latency or cold-data claim.

| Repeat cohort | Caller sample p95 | Engine sample p95 | Eight-query reads |
| --- | ---: | ---: | ---: |
| R517 index before maintenance |858.891ms |468ms |2,016,141,568B |
| R523 index after maintenance |733.463ms |333ms |1,204,026,160B |
| R523 wide reference |358.832ms |82ms |515,962,992B |

The aggregate join reads decrease40.28%, consistent with the physical intervention; total read counters do not attribute individual bytes to an operator/table. The new join still misses both warm engine100ms/caller250ms targets. The wide sample meets the engine threshold but misses caller. Nearest-rank sample p95 over eight queries is their maximum and does not establish a production distribution. R523 whole read experiment costs3,444,467,573 read bytes, zero write/spill and33.941s.

R525 independently audits all57 native query texts, terminal/final outcomes, code/source digests, two maintenance commit bindings, all11-field parity/count/CDF receipts, complete singleton responses and closing custody. Maintenance and read costs are separate; combined reads are4,408,668,612bytes and writes181,284,194bytes. Private index head3 is now observed; prior pinned versions1/2 remain historical evidence. No canonical role, manifest, ACK, consumer profile or warehouse setting changed.

Disposition: the private immutable-carrier/index split preserves meaning and reduces mutation I/O, but adds retention, maintenance, cross-table publication/fencing and joined singleton cost. It remains an experimental alternative. Do not scale/backfill it solely from the lower I/O numbers. Keep full wide canonical current as the default candidate and exact identity/property/retained/endpoints/journal/raw/tombstone contracts intact. GraphFrames/PuppyGraph direct-UC limits, bounded Fabric projection and deferred UMF binding remain unchanged.

Next prioritize an integrated publisher iteration using the qualified concurrent validation stage, with real phase costs and an explicit complete publication clock. Query-only join changes and routine repeated singleton cohorts have not closed the latency gap. All cold-data, steady10k/s and100k/s-burst freshness, production singleton p95, writer fencing, external engine interoperability and actual1B/5B-scale admission remain unproved. UC Delta remains the architectural commitment.
