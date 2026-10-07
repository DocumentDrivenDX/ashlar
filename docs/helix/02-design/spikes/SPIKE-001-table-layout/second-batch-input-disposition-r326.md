# Second batch input qualification — r323–r326

This synthetic batch is ready for a private ingest/publication comparison against the full range32 candidate. It contains 100k distinct, scattered identities untouched by the first batch: 90k version1→2 updates and 10k deletes. Full original input JSON and every declared field match the independent r318 oracle. This is input qualification, not measured publication freshness.

The local r319 files total 1,100,633,476 bytes in 35 parts. Native r323 transport uploaded and downloaded all bytes through the existing owned UC volume in 314.499s, with every length and SHA verified. Files remain mutable; native processing must consume the qualified Delta snapshots instead.

Native r324 stages four private input tables at version0 with exact CREATE statement and UUID receipts. All original-JSON and known-field digests, identity/event uniqueness, raw payload hashes, batch IDs, cursor/delivery links and update/delete partition pass. All36 statements have final successful native history; digest and batch checks are uncached. Staging took120.477s, read2,196,234,678 bytes, wrote733,949,482 bytes and spilled0.

Tombstone input has an explicit `apply_batch_id` extension. The stage retains the original JSON and extracts this input metadata column. Canonical CONTRACT-003 tombstone DDL remains unchanged. Any subsequent canonical append must explicitly document the input-only metadata disposition and preserve the raw delivery linkage; no silent field removal is permitted.

Native r325 compares all20 carrier fields for every100k predecessor against `edge_range32_r313` version52, UUID7c76c985-b802-41b6-97e8-a76a809e2a01. All match, with unique prepared mutation identities and exactly90k updates/10k deletes. All5 statements are native-final and qualification reads uncached. Wall62.881s, read33,287,019,755 bytes, write0, spill0. This is nearly a whole edge-table scan; it does not establish effective batched partition pruning.

Next use separate private candidates for range32 and liquid-clustered apply comparisons. Preserve existing first-batch custody, qualify complete changed images and retained raw/journal history, and time the complete apply/validation/manifest readback path. Include the physical partition predicate in range32 mutation matching alongside full hash and native identity; measure its actual scan behavior. Preparation costs above remain separate and visible. No apply, manifest, ACK, production pointer, concurrent writer fence, sustained10k/s or billion-scale admission is claimed here.

Evidence: `out/native/ashlar_second_batch_upload_r323/audited-summary.json`, `out/native/ashlar_second_delta_stage_r324/audited-summary.json`, `out/native/ashlar_second_predecessor_r325/audited-summary.json`. Offline receipt auditor: `second_input_receipt_audit_r326.py`.
