# Native immutable descriptor guard r363–r364

Governed by CONTRACT-001–003 and proposed ADR-001. This is a bounded private manifest experiment, not a complete graph publication or performance admission.

A fresh private shallow clone of the r360 manifest at version3 contains exactly its two unchanged descriptors. The new table `client_dev.ashlar_entropy_20261006_r86.manifest_atomic_guard_r363` has UUID`95c9bc5f-1c8d-4f20-b4f4-d839b45097da`. Clone0 and exact replay MERGE1 are the complete owned commit interval. The guard compares both exact descriptor text and SHA256 inside MERGE; conflicting matched content evaluates raise_error rather than updating the descriptor. Target INSERT columns are explicitly named. Canonicalization is the existing reference serializer, not a general JSON equivalence implementation.

Exact replay succeeds. Two native controls independently change descriptor content/progress and digest. Each source includes a novel rollback-sentinel row alongside the conflicting existing ID. Both MERGEs fail with ASHLAR_PUBLICATION_ID_CONFLICT; complete two-row readbacks remain exact, sentinel is absent and head remains1 after each failure. All12 successful and2 failed native statements have finalized history, exact tagged SQL custody and reported costs. This improves the previous client-only conflict guard for existing IDs; no speculative conflict update is committed.

The20.946s run reports72,835read bytes/2,491write bytes/0spill, including failed attempts. Reported attempted writes are not a physical orphan inventory. No VACUUM, production pointer, canonical carrier mutation, source ACK, compute provisioning or resize occurs.

The guard does not enforce uniqueness when simultaneous writers create an absent ID. It does not fence writers across current/raw/journal/manifest roles or establish source authority. Two-row serialized rollback does not qualify complete source/history vector publication, crash/expiry recovery, active retention or ingest/read SLOs. Existing source/graph mapping limits remain unchanged. Next qualify concurrent absent-ID creation on an owned reference, then choose the authority mechanism from demonstrated behavior; never promote this matched-row guard to a global fencing claim.

Evidence: [audited receipt](out/native/ashlar_manifest_atomic_guard_r363/audited-summary.json), reproducible native worker and independent saved-evidence auditor. All original250ms/100ms/60s and1B/5B targets remain open.
