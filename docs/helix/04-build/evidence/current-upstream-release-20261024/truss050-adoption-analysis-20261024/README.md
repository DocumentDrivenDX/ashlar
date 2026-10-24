---
ddx:
  id: ashlar.evidence.truss050-adoption-analysis-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
---

# Truss 0.0.50 Ashlar adoption analysis

Status: **public release and migration metadata reviewed; adoption is a candidate; no Ashlar pin, install, PostgreSQL, Spark, migration, feed, publication, or ACK was changed or run.** This is effect-free release evidence, not a new R03 lifecycle result or execution approval.

## Published release custody

The official `DocumentDrivenDX/truss` GitHub API identifies `v0.0.50`, published 2026-10-24 06:32:49Z. The exact assets are:

| Asset | Bytes | SHA-256 |
| --- | ---: | --- |
| `truss_toolkit-0.0.50-py3-none-any.whl` | 6,237,543 | `7b0483bb0c624c98515cf38ec30bbbf1ca43c1250e8880e5d00d520b4b7eec00` |
| `truss_toolkit-0.0.50.tar.gz` | 6,709,423 | `60204fbb2b420e4600ebafa397e2742b46f06d6085936411dfa81f19aa7c5aaa` |
| `release-pins.json` | 5,077 | `ed35eb49b453f1ad9178e3875d9d7c3b762b24dc0697e98038f8fcabbaad5b88` |

The release is the official [v0.0.50 GitHub release](https://github.com/DocumentDrivenDX/truss/releases/tag/v0.0.50). The official wheel and `release-pins.json` were downloaded and independently hashed against the GitHub release API metadata; the wheel's `RECORD` and selected public APIs were also inspected. The sdist was not downloaded because the wheel supplied the required sources and metadata. See the [focused package/cohort receipt](../truss050-package-cohort-qualification-20261024/README.md) for the bounded result; it does not qualify lifecycle behavior.

## Public API and migration comparison

The official release notes describe a fresh-install batching change, retaining existing public application interfaces and leaving native installation sources and published migration recipes unchanged. GitHub source metadata agrees for the checked interfaces: `capabilities.py`, `capability_contracts.py`, `feed.py`, `migrations.py`, `migration_planning.py`, `_migration_recipe.py`, `_migration_verify.py`, and `_migration_execute.py` have identical Git blob SHAs at tags `v0.0.49` and `v0.0.50`. The `catalog_runtime.py` blob changes (`c41372b01f0836e3628f6569784185a4c27f21ad` to `132c9eb452f9090109b0994792fc37ce4642ce5a`); its Python class method names, including `read_feed_in_transaction`, `reread_feed_in_transaction`, `acknowledge_feed_with_receipt_in_transaction`, `reconcile_feed_ack_in_transaction`, and `RuntimeMigrations`, are present in both versions. This is source/API-surface evidence only; no installed .50 behavior was exercised.

The migration index `packages/python/src/truss/_migrations/manifest.json` changes from Git blob `f4258370551d635acf600c581f9cf4625f595389` at .49 to `270125a653dba8f714f5a783df3b9c2e0f9c231f` at .50. Comparing its declared route graph shows the same five forward edges and same target native manifest `288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4`; .50 adds the exact-current fixture manifest named `96addcdb95523f478d8f50b0b47198e47a860680c7400dd359711930e837e535.manifest.json`. The archived .49 migration index remains a declared package file. The release adds no .49-to-new-native-target edge because the native target is unchanged.

The selected .48 source manifest remains `c4339ac7f09ab5a004c272ab00962107a970938004c1765ec4163e4d5ca9b6f0`. The existing explicit edges are .48 (`c433…`) → intermediate (`35fd797b5cf48dd888c68e53b9e599df20cffe00f8f3d33e1563820ea808d59f`) → .49/current target (`288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4`). Therefore a latest-only .50 runtime can migrate the accepted .48 database through the existing two-edge route in one administrative migration operation. It is not a sequence of installing .49, migrating, and then migrating .49 to a new .50 schema. The supported source is UMF 0.10; pre-0.10 and protected-profile migration remain outside the published qualification.

This matches the older [0.0.49 adoption preparation](../../current-upstream-release-20261022/truss-0.0.49-adoption-preparation.json), which records the same .48 source and two edges. R12's current selected-source packet rechecks that accepted source with its frozen .48 read-only helper before any migration; its selected .49 migration manifest is the .50 target's exact-current fixture. These remain preparatory claims; R12 has not run migration or lifecycle effects.

## Ashlar cohort fit and decision

R01's selected Ashlar cohort and R12's passed .49 effect-free preflight bind Truss .49, UMF core 0.10.1/access 0.1.0.dev7, Weft 0.4.53, Spark 4.2.0, Delta 4.4.1, psycopg 3.3.6, and PostgreSQL 18.6. The .50 release notes identify the same declared upstream cohort as .49: PostgreSQL 18.6, psycopg 3.3.6, UMF 0.10, and Weft 0.4.51. The .50 feed/receipt declarations are unchanged from .49, and its migration route targets the same native manifest. These facts make a focused .50 adoption and new packet preparation reasonable.

The focused .50 package/import check passed against the current Ashlar tuple: Truss imports from the verified .50 wheel beside UMF core 0.10.1/access 0.1.0.dev7, Weft 0.4.53, and psycopg 3.3.6 in the read-only R12 runtime. It checked wheel `RECORD`, origin paths, and feed/receipt/migration API signatures; it invoked no Truss methods. `supportedUmfVersions: ["0.10.0"]` names the schema/model semantics, which remain 0.10.0 in Ashlar's core package 0.10.1. The wheel's access .dev5 and Weft .4.51 constraints are conditional on Truss's optional `[typed-sdk]` and `[query]` extras, which this integration does not select. If a packet selects either extra, its exact pin conflicts with Ashlar's current cohort. A new .50 selected-source packet is viable at this package qualification stage with those extras unselected; it still needs its own source-bound preparation, independent review, and separate execution authorization.

## Next hash-bound packet plan

For the next step, prepare a separate `.50` selected-source packet under a new single-use identity. Bind the exact selected Ashlar source commit/tree and source Git objects; official wheel and release-pins digests; downloaded wheel and runtime receipt; UMF/Weft/Spark/Delta/PostgreSQL tuple; bridge and selected current-source/helper hashes; frozen .48 accepted source/report/view/consumer checkpoint observations; target native manifest and exact two-edge migration route; and regenerated worker, supervisor, preparation, preflight, and approval-payload proposal hashes. Do not select Truss's `query` or `typed-sdk` extras without resolving their exact-version conflicts. Recompute all gates and payload parity for that packet. Keep the existing R12 files and private state unchanged. The packet should remain unapproved and effect-free pending independent review and a distinct owner authorization; migration, source/feed/publication/ACK, Spark, and PostgreSQL execution require later gates.

## Source observations

- [Focused .50 package/cohort receipt](../truss050-package-cohort-qualification-20261024/README.md)
- [Official v0.0.50 release and assets](https://github.com/DocumentDrivenDX/truss/releases/tag/v0.0.50)
- GitHub API release record: `https://api.github.com/repos/DocumentDrivenDX/truss/releases/tags/v0.0.50`
- GitHub API source comparison: tag contents for `v0.0.49` and `v0.0.50` under `packages/python/src/truss/` and `_migrations/manifest.json`
- Ashlar [R01 selection row](../../../toolkit-delivery-plan.md#release-gate-order--2026-10-23) and [0.0.49 package adoption receipt](../../current-upstream-release-20261022/truss-0.0.49-adoption-preparation.json)
- Ashlar [R03 readiness packet](../../current-upstream-release-20261023/truss049-r3-readiness-20261023/README.md) and [R12 selected-source packet](../../current-upstream-release-20261023/truss049-r12-current-selected-source-lifecycle-20261024/README.md)
