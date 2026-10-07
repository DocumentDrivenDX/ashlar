# Next physical design experiment after scattered third-batch publication

Proposed experiment, not a replacement for CONTRACT-003/ADR-001 or a production schema revision. Unity Catalog managed Delta and all Truss identities, exact property bags/retained content, typed endpoints and permanent property journal remain requirements. No UMF binding.

## Why change the experiment

The integrated8M/40M test takes257.486s for100k changes. Complete predecessor qualification plus current MERGE reads67.87GB of71.68GB total. Scattered changes touch much of the existing file set; copying fewer files via deletion vectors does not remove the read/validation cost. The exact1B/5B footprint remains unadmitted, and a linear forecast is not a capacity result.

An inline complete before-carrier guard could remove a redundant scan while retaining fail-closed exact preconditions. It must compare all20 current fields for updates AND deletions, including binary JSON text/null distinctions, and abort the whole MERGE on mismatch. Missing/duplicate matches must still prevent publication. Exact CDF preimage/postimage cardinality and digests remain necessary. A version predicate alone is insufficient. Qualify tiny clean/update/delete/mismatch/rollback cases first; no full extra batch merely to demonstrate syntax.

## Append overlay candidate

Use separate experimental UC Delta tables: a pinned compacted base and append-only accepted whole-entity replacements/deletion markers. Keep full semantic tuple keys and native versions, source origin, exact20field carriers, and raw/journal/tombstone roles. A manifest selects an immutable base version plus bounded overlay versions and accepted progress. Logical current selects the highest accepted version per complete tuple; deletion is an explicit winning state. Hash is pruning metadata only. Conflicting same-version values must refuse acceptance, not win by arbitrary sort. Canonical current contract uniqueness applies to the logical result; multiple physical overlay versions would require an explicit future Contract/ADR revision, never silent compliance with current unique-row claims.

Native singleton reads probe the hash AND complete tuple in base/overlay, resolve versions and return the exact winning carrier. Bound the number of overlay tables/files/versions examined; declare incomplete/refused outcomes when backlog exceeds admission. Keep old manifests readable throughout compaction. Compaction creates a qualified successor base and exact progress-equivalent vector; only publish after complete preservation and commit custody. No per-batch full compaction. Retention must include every active old base/overlay pin, raw and permanent journal; no TTL/VACUUM inference.

PuppyGraph/GraphFrames/Fabric require explicit release projections or a qualified logical-current view. Do not claim their engines understand overlay/tombstone resolution, Delta reader features, or unmaterialized views. A consumer release may lag canonical acceptance only under an explicit publication/read-boundary contract; otherwise it must be part of complete publication and its cost is charged. Fabric stays a bounded projection at this graph scale. Preserve scalar typed identities/endpoints and exact content companions when tools cannot represent native values.

## Bounded next run

First run a small native structural/semantic control on private tables, then an already-staged100k overlay append/read comparison using pinned E5/E6 as reference (no fourth synthetic batch needed). Assert exact current-vs-overlay winners for all changed keys and unchanged/cross-type/null/lexical/deletion controls, uniqueness and typed closure; verify full origin/history retention independently. Capture all versions, submitted handles, native bytes, compile/engine/caller timing and cached status. Reuse existing compute, no resize. Independently measure at least the previous stratified singleton cohort through the combined query and compare with direct current E6; do not substitute a cached lookup for ingest admission.

Before a native run, write explicit byte/time bounds based on current staged/current-table evidence and disable uncontrolled maintenance on new private tables. A small overlay result does not satisfy1B/5B, steady10k/s or100k/s burst gates. Sustained backlog/compaction/read overlap remains necessary if this candidate shows a useful tradeoff. Retain the current LC design and qualified vector while the alternative is tested.
