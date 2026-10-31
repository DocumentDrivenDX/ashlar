---
ddx:
  id: ashlar.evidence.sdk010-delta4-bridge-candidate-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.umf010-upstream-adoption-20261031
      kind: informed_by
    - id: ashlar.evidence.g03-sdk058-delta4-realization-candidate-20261030
      kind: informed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
---

# SDK 0.10.0 Delta 4 bridge candidate — 2026-10-31

Status: **effect-free proposal candidate; subsequently packaged in a separate versioned
Ashlar runtime module. MiniRacer parity remains pending.** The proposed profile is
`ashlar-local-current-delta4-umf-source-sdk010/0.1`. The candidate reuses the
SDK 0.10.0 `readDocument` and `realizeSparkDelta4DDL` source APIs behind the
existing Ashlar-owned Delta 4 bridge interface. It leaves the installed SDK
0.5.8 resource and all prior 0.5.8 receipts unchanged.

The official SDK 0.10.0 release tarball `umf-sdk.tgz` was downloaded and its
SHA-256 verified as
`2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c`. Its
public entry `dist/umf.js` exports both required functions. The release package
does not include the TypeScript source modules used by the existing Ashlar
bridge. Bun cannot bundle the package-root ESM directly as a browser IIFE
because it contains top-level `await init_src()`.

The candidate entry therefore imports `src/model/document.ts` and
`src/adapters/delta/ddl.ts` from the exact tag source commit
`96230e0eef44b255d33575ba496402555b76800e`. Bun 1.4.3 built the source entry
to a browser IIFE. The source API bridge and the SDK package's public API each
produced the same six Delta 4 carrier proposals as the frozen SDK 0.5.8 receipt:
`object_current`, `edge_current`, `tombstone`, `whole_source_history`,
`publication_manifest`, and `publication_attempt_phase`. The runner checks the
SDK tarball, package entry, bridge entry and bridge bundle digests before
comparing outputs.

The reviewable candidate consists of [bridge-entry.ts](bridge-entry.ts), the
compiled [bridge.iife.js](bridge.iife.js), [build inputs](build-inputs.json),
and [qualification output](outputs/qualification.json). To rerun the comparison
with the extracted `package/` directory and verified release tarball:

```sh
node docs/helix/04-build/evidence/current-upstream-release-20261031/sdk010-delta4-bridge-candidate/qualify_sdk010_bridge.mjs \
  /absolute/path/to/extracted/package \
  /absolute/path/to/ashlar \
  /absolute/path/to/umf-sdk.tgz
```

This evidence covers SDK API identity and six pure proposal outputs only. The
candidate IIFE ran in Node's isolated VM; MiniRacer compatibility and native Spark or Delta behavior, publication, and managed UC remain
unqualified. Ashlar runtime packaging and admission pins are documented in the
[sibling runtime integration evidence](../sdk010-delta4-runtime-integration/README.md). The existing R18 packet admits its earlier `.4` realization
profile and resource hashes; it does not admit this candidate identity.
