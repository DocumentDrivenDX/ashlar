---
ddx:
  id: ashlar.evidence.managed-a1-retention-provider-assessment-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.managed-a1-live-holder-owner-proposal-20261029
      kind: informed_by
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
    - id: TD-009
      kind: informed_by
    - id: ashlar.evidence.managed-a1-ashlar-dev-readonly-discovery-20261029
      kind: informed_by
---

# Managed A1 retention provider assessment — 2026-10-29

## Disposition

No independently admitted provider or currently documented Azure Databricks
control was found that can enforce availability of the four pinned Unity Catalog
managed Delta versions through an ACK interval while preserving the existing
table contract and keeping predictive optimization enabled. This assessment is
based on current Microsoft documentation; it is not a live workspace probe or
an assertion about the unevaluated `aidev-cus` account configuration.

The blocker is specific to the managed-table boundary. Unity Catalog's
`delta.deletedFileRetentionDuration` is a `VACUUM` eligibility threshold, not a
hold. The documented default is seven days; the transaction-log default is 30
days. Manual `VACUUM` can override the table threshold, and the safety check for
short retention can be disabled. Predictive optimization itself runs `VACUUM`
on managed tables, normally using the table threshold and a documented seven-day
minimum for its own `VACUUM FULL`. That normal window may be ample for a short
trial; it does not fence a separately authorized manual cleanup or table
replacement during the source ACK interval. A successful `VERSION AS OF` read
or metadata inspection proves availability at that observation time only. It
does not reserve files for a later source ACK.

## Provider and alternative assessment

| Candidate | Finding for this four-pin boundary |
| --- | --- |
| `delta.deletedFileRetentionDuration` plus `delta.logRetentionDuration` | Useful retention configuration, but not an enforceable interval hold. The first governs when `VACUUM` ordinarily considers unreferenced files; the second governs log cleanup. Both must cover the desired history, but a manual `VACUUM` may override the data-file threshold. |
| Disable or avoid `VACUUM` | Not compatible with the stated requirement to keep predictive optimization enabled: predictive optimization automatically runs `VACUUM` on Unity Catalog managed tables. A separate job prohibition does not fence the predictive-optimization actor. |
| Azure Blob immutable storage (WORM) | Azure supports container-level WORM, including on hierarchical namespace accounts, but Azure Databricks explicitly says an ADLS Gen2 container with an immutability policy cannot be used as a Unity Catalog external location. Unity Catalog managed storage is defined through an external-location object; therefore this documented Azure WORM mechanism cannot be admitted as the hold for these managed tables through the supported UC storage path. Applying an undocumented out-of-band policy directly to the managed location is not an admitted integration and could interfere with managed lifecycle operations. |
| UC table owner / maintenance grants | Unity Catalog ownership and `MANAGE` confer broad table-management authority. Grants do not provide a native interval lease that fences the platform's predictive-optimization service or all storage deletion paths. A read of grants is not proof of future file retention. |
| Shallow clone | Not a hold: it refers to source data files. UC's special clone-aware `VACUUM` behavior tracks clone references but remains a maintenance policy, not an independent lease over the original four-version vector. |
| Deep clone | Copies data and metadata into new tables, so it can preserve a snapshot independently of the source table's files. But each clone has new table identity/history and is not the original publication's `(table name, UUID, version)` vector. Substituting clones would change CONTRACT-001/003/004 and receipt custody semantics; it is not a transparent provider for the existing A1 request. |

The Azure WORM finding is deliberately limited to the documented Unity Catalog
ADLS Gen2 integration. It does not establish that no custom storage-side
mechanism could exist outside this supported path. Such a mechanism would need
to be independently admitted, prove its precise storage scope and maintenance
coverage, and be compatible with Databricks managed-table lifecycle before it
could change this disposition.

## Smallest reopening change

Keep the current A1 contract held until one of these two changes is selected and
reviewed:

1. **Admit a managed-table retention provider.** Add a separately owned,
   renewable provider whose admission binds the original receipt and exact
   complete four-role name/UUID/version vector. It must establish a native
   hold that fences every file-removal path, including predictive optimization,
   manual `VACUUM`, table drop/replacement and administrator/storage actors,
   for a finite interval; renewal and close must verify that hold still covers
   all four pins. It must leave predictive optimization effectively `ENABLE`.
   The documented UC/Azure features reviewed here expose no such provider, so
   this requires a new supported platform capability or a separately verified
   table-maintenance service integrated with every maintenance actor.
2. **Amend the publication/verification contract to use retained images.**
   Before verification, create a complete deep snapshot of the four roles and
   make that frozen image—not the original managed-table vector—the verifier's
   read authority. Version the descriptor/receipt contract to bind both the
   original publication vector and the new image UUID/version vector, and admit
   an immutable store/provider for the images. The original vector remains
   provenance; it cannot silently be replaced by clone identities. No such
   image-store provider or contract amendment is currently implemented or
   qualified. A shallow clone does not satisfy this route.

Neither option is authorization to change retention, create clones, edit source
or ACK, or start compute. On current evidence, the minimum accurate A1 decision
is to retain the holder gap and withhold protected ACK.

## Focused qualification path after provider admission

Use a small, dedicated test catalog with exactly four managed Delta roles and
predictive optimization effectively enabled. Capture each role's exact UUID,
version and complete file/log availability witness before admission. Exercise
provider open, renewal, and close across the bounded ACK interval while
independently attempting every covered maintenance path, including
`VACUUM`/short-retention override, `ALTER TABLE` retention changes,
`DROP`/replacement, and a predictive-optimization run. Require refusal of any
uncovered actor/path; require a fresh exact-version read of all four roles after
each renewal and after close; verify PO remains `ENABLE`. Also test expiry,
provider outage, partial-vector admission, UUID/version drift, lost renewal
response, and cleanup. The test passes only if the hold is enforced by the
independent provider across all paths, not because four reads happened to work.

If the retained-image contract is selected instead, test exact four-role
deep-copy completeness and multiplicity, source-to-image comparison, new image
UUID/version custody in the immutable receipt, and image readability after
source-table `VACUUM`/drop tests in a disposable environment. Do not describe
that evidence as retention of the original publication vector.

## Primary documentation reviewed

- [Azure Databricks: configure data retention for time travel](https://learn.microsoft.com/en-us/azure/databricks/tables/history) — separate log/data retention requirements and properties.
- [Azure Databricks: table properties](https://learn.microsoft.com/en-us/azure/databricks/tables/table-properties) — `VACUUM` override of the deleted-file retention threshold.
- [Azure Databricks: VACUUM](https://learn.microsoft.com/en-us/azure/databricks/tables/operations/vacuum) — predictive optimization performs VACUUM; thresholds, override safety configuration and effects on time travel.
- [Azure Databricks: predictive optimization](https://learn.microsoft.com/en-us/azure/databricks/optimizations/predictive-optimization) — automatic OPTIMIZE, VACUUM and ANALYZE for UC managed tables and its own seven-day `VACUUM FULL` minimum.
- [Azure Databricks: ADLS Gen2 external locations](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations-adls) — WORM-protected containers cannot be UC external locations.
- [Azure Databricks: managed versus external assets](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/managed-versus-external) — UC controls managed storage location and file lifecycle.
- [Azure Storage: container-level WORM](https://learn.microsoft.com/en-us/azure/storage/blobs/immutable-container-level-worm-policies) — storage-side append/write/delete policy semantics.
- [Azure Databricks: shallow clone for UC tables](https://learn.microsoft.com/en-us/azure/databricks/tables/operations/clone-unity-catalog) and [clone a table](https://learn.microsoft.com/en-us/azure/databricks/tables/operations/clone) — shallow references source data; deep copies data and metadata.

These are mutable service documents, reviewed 2026-10-29. Runtime-specific
behavior must be checked against the exact A1 runtime once that runtime is
admitted. No Databricks API, storage policy, job, table, cluster, source feed,
ACK, or schema was mutated for this assessment.
