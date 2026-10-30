---
ddx:
  id: ashlar.evidence.managed-a1-retained-image-proposal-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.prd
      kind: informed_by
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
    - id: TD-009
      kind: informed_by
    - id: ashlar.release-closure-20261029
      kind: informed_by
    - id: ashlar.evidence.managed-a1-retention-provider-assessment-20261029
      kind: informed_by
    - id: ashlar.evidence.managed-a1-live-holder-owner-proposal-20261029
      kind: informed_by
    - id: ashlar.evidence.managed-a1-jobs-admission-packet-20261029
      kind: informed_by
---

# Managed A1 retained-image proposal — 2026-10-29

**Disposition: retained images are not a near-term A1 solution by themselves.** A deep image changes table identities and still needs an independently admitted provider to guarantee image retention through ACK. It adds a full four-role copy plus a second vector and custody path. This remains a contract/design option only; no image provider, source authority, runtime, or cost is admitted.

## Contract changes required

Amend TD-009 and the selected receipt-read profile to carry two separately named vectors:

- `originalPublicationVector`: exactly the original four data roles, each with the manifest's original three-part table name, native Delta UUID, and literal version. This remains immutable provenance and must equal the original request and manifest. Attempt and manifest tables remain outside this vector.
- `retainedImageVector`: exactly one image role for each original role, with the image's three-part name, native UUID, and literal version, plus an explicit one-to-one role mapping back to the original vector. Image UUIDs and versions are distinct identities and must never replace, rewrite, or be reported as the original vector.

CONTRACT-003 should define the versioned image-table and receipt profile; CONTRACT-004 should define resolver admission: selected profile/version, both vectors, source-to-image role map, image creation/freeze record, image schemas/protocols and exact snapshot versions. All verifier reads and complete bag comparisons use `retainedImageVector` at explicit literal versions. Before admitting the image, compare each role's complete bounded logical bag to the source-derived oracle read at `originalPublicationVector`, preserving absence versus null, exact scalar and payload values, source-qualified identity, multiplicity, history and tombstones. Refuse incomplete roles, vector drift, row/cell/byte overflow, or any mismatch. Resolver callers cannot select or substitute image tables through an untrusted vector.

CONTRACT-001 should make image lifecycle and ACK custody explicit. A separately owned image provider must create the deep copies from the independently admitted original vector, verify source and image UUID/version identities and all four role mappings, freeze the images, and return a renewable handle proving that the exact image versions remain readable through ACK commit, reconciliation and close. Renewal/close recheck image identity, version and provider retention; missing/expired authority withholds success. The external source holder remains the only ACK actor. Its protected ACK binds exact original request/manifest and receipt bytes, both complete vectors and the image-provider custody identity/digest. It must keep source registration/checkpoint and protected ACK custody independently live through commit, exact reconciliation and all closes. Publisher/verifier tasks receive neither ACK credentials nor image-provider authority. If the existing source API requires an original publication pin during ACK, that obligation remains separately admitted; image retention does not silently satisfy it.

The A1 contract profile must state that the image vector is the verifier's read authority, while the original vector is provenance and the source comparison basis. This is an explicit profile-specific alternative to original-file retention, not a change to ordinary publication reads or the existing strict retention-backed profile. It does not authorize a campaign transition until these contract changes and provider behavior are reviewed and implemented.

## Small A1 qualification and cost envelope

After independent provider, source-holder, workspace, runtime, UC-mode and billing admission, use one dedicated Jobs run, one bounded slot, four tiny source tables and four deep images in an owned disposable schema. Keep predictive optimization enabled. Run a publisher task and then a distinct verifier process/session. Capture original vector and exact request/manifest first; create/freeze images; verify exact image vector and source-to-image role bijection; compare complete bounded bags; then renew image and source custody immediately before the external holder commits and reconciles the protected ACK. Exercise refusal for a missing/duplicate role, changed source or image UUID/version, partial clone, value/null/multiplicity mismatch, provider expiry/outage, lost renewal/ACK response, and cleanup. Independently verify exact ACK correspondence and close every acquired handle. Do not advance to B1 or retry uncertain effects. Keep original and image table evidence until reconciliation; delete only exact trial-owned objects after successful close and verify absence.

The admission ceiling is **$100 total**, with **$10 preferred**. TD-009's 40-minute D4ds_v5 sketch is about $0.90 compute-only, but it excludes startup/idle billing, DBUs, storage, image-copy duration, provider charges, predictive-optimization work, ancillary meters and cleanup; it is not a total estimate or cap. Price the actual admitted two-node Jobs shape, runtime and billing increments, image storage/copy, provider and cleanup before launch. Include prior spend and a caller deadline with independently observed cancellation/termination and terminal-state readback. If a reliable whole-trial estimate and hard operational stop cannot fit under $100, do not launch. Keep predictive optimization enabled.

## Risks, alternatives and open gates

Deep copies may preserve logical rows while losing required native history, transaction metadata, protocol features or exact physical semantics; those must be declared and tested. A provider that merely creates clones or reports a successful read is not a retention authority. Source-vs-image comparison also requires the original versions to remain readable through image creation and comparison. Provider loss, partial creation, orphan cost, UUID drift and uncertain ACK require recovery custody and no blind retry. The copy adds four tables, storage, a second vector and custody path. Without an admitted image-retention provider, this option does not solve A1's retention blocker.

A potentially smaller third path is a **versioned, time-bounded operational retention profile over the original UC managed tables**. For one A1 slot, it would require both `delta.deletedFileRetentionDuration` and `delta.logRetentionDuration` to be at least seven days, predictive optimization effectively enabled, a maximum 15-minute ACK interval, an inventory of effective manual-maintenance privileges and identities, and a prohibition on manual `VACUUM`, table replacement/drop or retention changes during the interval. Platform predictive optimization remains an admitted maintenance actor governed by its documented retention window, not a service identity the holder claims to fence. Open, every renewal, and close must perform fresh exact-version reads for all four original role UUID/version pins and verify the selected retention settings and maintenance inventory. The contract would explicitly exclude administrator and storage-level actions from the protected threat model; this profile cannot guarantee against admin override, hidden actors, or out-of-band deletion. It is plausible as a low-edit A1 experiment only if the owner accepts that weakened threat model and approves a profile/contract revision. It is not currently authorized or admitted, and observations of grants/settings alone cannot establish exclusion.

Alternatives are: (1) an enforceable native provider holding the original four versions through ACK, preserving current semantics; (2) the dual-vector image profile plus independently admitted image creation/freeze/retention, not a near-term route by itself; (3) the operational profile above, with explicit admin/storage exclusions and owner approval of the weaker guarantee; or (4) keep A1 held and withhold protected ACK. Shallow clones do not qualify because they depend on source files. No option is currently admitted, and no A1 compute or source/ACK operation is authorized by this proposal.
