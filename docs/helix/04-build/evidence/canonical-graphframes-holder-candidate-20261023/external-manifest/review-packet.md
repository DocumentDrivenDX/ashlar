# Closed manifest review packet — canonical `.4` GraphFrames action

Status: frozen candidate for independent Sol review, not action authorization. Repository base is Ashlar commit `79aad7c734c36720be8cde85c9d7dec3dfba8447`. `manifest.json` uses the exact closed top-level schema accepted by `ReviewedCanonicalActionDescriptor.from_reviewed_bytes`; its bytes bind the generator revision and bytes, source bytes, independent oracle bytes, carrier definition, runtime selection, formula/proofs, selected seed, cap and output schema. Do not edit a bound byte without producing and reviewing a replacement manifest and anchor.

`manifest.anchor.json` is the canonical JSON review anchor with profile `ashlar-canonical-action-review-anchor/0.1`. `manifest.anchor.sha256` contains only its lowercase SHA-256 digest; pass that digest separately as `selected_review_anchor_sha256` to the loader. The anchor binds the manifest, generator, source, oracle and carrier digests and the review ID. The digest is deliberately a separate selection input, not embedded in the anchor or substituted by a `sha256sum` record.

## Closed action

One selected outgoing one-hop action over canonical `.4` `object_current` and `edge_current`: 3 object rows, 2 edge rows, seed `("ashlar-canonical-graphframes", 1, 101)`, degree 2, cap 2, collect bound 3, nine ordered output fields, exact multiset comparison preserving edge IDs 9001 and 9002. The independently authored oracle is separately byte-pinned. Formula/proof declarations cover node and edge identity uniqueness, endpoint membership, parallel-edge multiplicity, signed-64 bounds, null/payload rules and the fixed source fields. These are finite-set claims from the frozen source literals and oracle; they do not claim that any native tables were materialized.

The manifest pins UMF revision `2ede4c879bea5db13794a6fa30e67c821306c17f`, profile `ashlar-delta/0.4-xxhash64`, and selected Spark 4.2.0, Delta 4.4.1, GraphFrames 0.12.3 and JDK 21.0.13. The exact runtime JAR pins are fixed by the hashed generator module at `src/ashlar_host/pinned_delta_graphframes.py` (`CANONICAL_RUNTIME_ARTIFACTS`) and enforced by its loader/runtime admission checks. They are not extra manifest fields because the loader rejects any top-level member outside its exact closed schema. The fixed pins are:

- `delta-spark_4.2_2.13-4.4.1.jar`: `2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be`
- `delta-storage-4.4.1.jar`: `f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f`
- `graphframes-spark4_2.13-0.12.3.jar`: `a78958f7ea3ac89ed1ea9dbbd1546f403d94151e969bb0de96e33697a5f49f50`

Live admission must verify resolved class JAR paths and bytes against those pins; package metadata or a configured version string alone is insufficient. The manifest runtime selection does not attest that a runtime was observed.

## Native preflight and custody

Expected carrier schema field order/types/nullability and Delta protocol are in the source fixture declaration, while the readiness packet states the required persisted checks. They remain expected values only: no canonical fixture tables have been inspected. Before any action, compare both persisted schemas including nullability, protocol tuples, timestamp exact microseconds and stored lookup hashes recomputed with the admitted Spark expression. Any mismatch refuses before GraphFrame construction/action.

The fixture must live in one fresh user-owned canonical local mode-0700 root. A stable mode-0600-or-stricter single-link regular lock inode is acquired with nonblocking exclusive OS advisory lock before opening tables and held through close. Record root and lock canonical path/device/inode. The completed owner receipt must bind root identity, table path/inodes, both Delta UUIDs, and source/generator/carrier digests. Every owned write and cleanup takes the same lock. Keep the root private, retain active data and log files, and verify bounded pre/post inventories (100,000 entries / 16 MiB). This is cooperative ownership among owned processes, not a global filesystem fence.

## Static review verification

From the repository root, verify all frozen artifact bytes with:

```sh
sha256sum -c docs/helix/04-build/evidence/canonical-graphframes-holder-candidate-20261023/external-manifest/artifacts.sha256
```

The loader call must supply the anchor path and the digest read from `manifest.anchor.sha256` as the separate selected SHA-256. No Spark/native command is supplied here. This preparation did not start Spark or GraphFrames and did not authorize the action.

## Limits and claims

One run, no retry. Hard limits: 300 seconds, 1 GiB retained plus attempt bytes, 128 MiB diagnostics. At deadline/bound, terminate and retain partial diagnostics without pass status. `sourceProgress`, `protectedAck`, `publication`, `unityCatalog`, and `productionAuthority` are absent. This fixture gives no production or scale claim; the 1B/5B trial remains held by R05.
