# 100k accepted overlay and full-carrier read comparison r400–r407

CONTRACT-001–003 and proposed ADR-001 govern the existing current-table design. This is a separate physical experiment under the selected Unity Catalog Delta architecture, not a Contract revision or production publication. UMF stays deferred.

## Append and preservation

An independent local oracle qualifies all100k complete21field markers and90k live20field carriers, preserving exact fields and matching the independently generated third-batch postimage multiset. A new private appendOnly, hash-LC/ZSTD Delta table is created empty at0 and receives one100k INSERT at1. Actual UUIDfccb6a2f-182f-4745-82ee-73d08cd23ef2 and both commit IDs are captured. Predictive maintenance is disabled. No current/raw/journal/tombstone/adjacency/production-manifest writes occur. Existing accepted third-batch history remains in the qualified r391 role vector.

Append caller8.023s/engine7.128s reads233,378,747bytes and writes80,370,684bytes, no spill. One resulting80.37MBfile exceeds the64MiB target: target is not a hard file maximum. Separate setup/preflight/validation/telemetry and previously completed source/admission/publication costs are retained;8s is not incoming-source publication freshness or a sustained rate. The data is the already accepted third batch, not a new producer feed.

Complete marker/live/deletion digests,100k unique typed identities,90k-update/10k-delete partition, positive/non-null version/identity/flag checks, exact raw origin links and canonical ten-field deletion projection pass. Marker payload fields are diagnostic predecessor data and never returned as live current. The attempted extra full-carrier outer join to E6 reaches its120s observation/submission guard and native-cancels at132.661s, after33,059,506,847reported read bytes/0write/spill. Same handle is verified native-final; no INSERT or comparison is replayed. This join is a failed plan shape, not a passing validation.

A separately bounded20.292s read-only follow-up verifies39.97M unique effective typed keys from E5 anti/new union, all10k new deleted identities absent at E6 and both typed endpoints for90k live replacements. All90k complete native overlay carrier hashes equal independently audited E6 update_postimage hashes, with the exact E5→E6 qualified commit/predecessor lineage. This transitive multiset proof (SHA256 collision assumption) establishes complete changed-carrier equality without claiming the canceled join succeeded. Remaining inherited carriers/history use prior immutable-vector qualification; no fresh full39.97M carrier sweep is claimed. Private overlay stays at1.

## Native singleton tradeoff

Same prior32 stratified keys plus16third-batch keys (4deleted12updated), repeated twice in interleaved family order, produce192 actual complete20field/absence reads:96direct E6 and96E5+overlay1. Every value matches an independent source reconstruction. Persistent SQL connector has native result cache disabled; all192 native final queries are uncached. The overlay query checks full accepted-carrier conflicts per version, then resolves the winner. Timing includes actual full carrier return, not digest-only responses.

| Descriptive cohort | Caller p95 | Engine p95 | Compile p95 |
| --- | ---: | ---: | ---: |
| Direct E6, all96 |701.6ms |355ms |224ms |
| Guarded overlay, all96 |1250.8ms |627ms |578ms |
| Direct E6, repeat48 |442.1ms |112ms |205ms |
| Guarded overlay, repeat48 |1215.5ms |624ms |535ms |

Direct repeat has zero remote reads; overlay repeat includes12remote queries. Its36zero-remote repeat queries still have caller1050.1ms/engine343ms/compile543ms p95. These are descriptive I/O-counter subcohorts, not a fully controlled warm/cold working set or a service tail. Direct and overlay warm caller/engine provisional gates remain unmet. No cold≤1s admission is inferred from first-touch results.

Point workload182.375s reads27,726,610,547bytes with no write/spill. All build, canceled comparison, recovery and reads total65,508,658,432reported read bytes/80,370,684write/0spill, below aggregate80GB/1GB byte budgets. Original integrated600s plan is not reported successful after its canceled comparison; read-only follow-up and point clocks are separately disclosed. Costs include every submitted native query, including cancellation; no price/DBU or retained-storage inventory claim.

## Next intervention

Retain canonical direct current tables as the qualified design while testing this tradeoff. The simple overlay is not admitted: it improves isolated append work but worsens measured singleton work, and compaction/backlog/publication are unqualified. First inspect native plans and compare a simpler winner query on a small matched subset. Per-read full-carrier conflict hashing may only be removed for an immutable pair whose publication already proves positive versions, identity/field uniqueness, no conflicting identity/version and complete predecessor/source custody. It must not silently turn arbitrary same-version content into an arbitrary winner. Production admission/fencing remains unresolved; the current private E5+overlay1 pair is specifically qualified.

Only after query-shape evidence, test bounded overlay file shaping/maintenance with full carrier preservation and old pin retention. One wide80MBfile adds read amplification, but file tuning alone does not remove535ms repeat compile cost. Do not schedule compaction per batch or claim a sustainable backlog policy from this single overlay. Any future logical-current version multiplicity needs an explicit Contract/ADR revision; consumer PuppyGraph/GraphFrames/Fabric releases need qualified logical-current/materialized projections and cannot be claimed to understand the overlay. Fabric remains bounded; native UC reader-feature limits remain explicit.

Evidence: out/overlay100k-evidence-audit-r406.json; out/overlay-read-io-breakdown-r407.json. Native terminal records are under out/native/ashlar_overlay_build_r401, ashlar_overlay_validate_r404 and ashlar_overlay_points_r405. No compute provisioning/resize, source ACK, retention cleanup or1B/5B admission occurs.
