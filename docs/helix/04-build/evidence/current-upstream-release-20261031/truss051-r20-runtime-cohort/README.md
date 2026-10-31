---
ddx:
  id: ashlar.evidence.truss051-r20-runtime-cohort-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss051-r19-pre-effect-failure-20261031
      kind: informed_by
    - id: ashlar.evidence.truss051-r19-runtime-cohort-20261031
      kind: informed_by
    - id: ashlar.evidence.sdk010-delta4-runtime-integration-20261031
      kind: informed_by
    - id: ashlar.evidence.umf010-upstream-adoption-20261031
      kind: informed_by
    - id: ashlar.evidence.truss051-r16-native-candidate-20261029
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# Truss 0.0.51 R20 runtime cohort candidate

**Status: proposed, not approved; no native action.** R20 is a fresh local
full-lifecycle candidate derived from the reviewed R19 cohort. R19's one-shot
failed before native effects because its isolated child could not import the
current SDK 0.10.0 Ashlar bridge. The exact supervisor receipt and child logs
are preserved in the [R19 failure evidence](../truss051-r19-failure-evidence-20261031/README.md).

R20 binds current Ashlar source at Git commit
`d0c2791fa0478784cb6cf6d6509f743c021ef362` as a clean `git archive` snapshot.
Its 590-file `src` inventory is pinned by [the source manifest](ashlar-source-inventory-r20.json)
(SHA-256 `42390b7ef4b2ff5ab29eae186e9fa8531217d8fe011a0c037058dd61e2ac9d20`)
and the archive is pinned by SHA-256
`330d6b2582a377537ed6721bfde18757bff647f88834eb785663330c2b5986e8`. Before
any prefix worker can preserve data, copy PGDATA, or start PostgreSQL, the child
validates the exact archive, manifest and source inventory, and requires the current
committed `src` tree to equal the pinned commit's `src` tree. It imports only the
clean archived snapshot, so uncommitted source files cannot enter the child. The child
inserts the pinned `src` path ahead of isolated interpreter paths,
imports the SDK 0.10.0 table bridge and publication/continuation routes, rejects
preloaded `ashlar_host` modules from other roots, and verifies the installed
Truss 0.0.51, Weft 0.4.56, Core 0.10.4 wheel digest, PySpark and Delta versions.
The same check is exposed in effect-free `--source-preflight` mode. A focused
test launches it with `-I -B` from an unrelated working directory and an
environment with no `PYTHONPATH`.

The old R16 source verifier is used only to qualify the retained `.48` inputs.
R20 removes exactly the `sys.path[0]` insertion made by that verifier after it
returns, preserving all prior path entries; the custody prefix applies the same
rule. This prevents the older package from shadowing current Ashlar modules.

The scope keeps R19's 900-second ceiling, 2 GiB process-group RSS cap, 2 GiB
retained-plus-attempt cap, 512 MiB attempt-root cap, sequential local execution,
zero retries and no cloud resources. It reuses only the retained R16 custody
inputs and the pinned R19 Python runtime. It has a new attempt UUID, destination,
output root and supervisor directory. `approval_artifact_created` is false; no
R20 approval file, output root or supervisor directory exists. No PostgreSQL,
Spark, Delta, publication, ACK, seed, continuation or cloud action was run.

Current proposal SHA-256: `8365c414a5ca8f4b50bda8b67b6d047d563bc0c8d7d2f618e7277b879989f7e0`. Worker SHA-256 pins: custody prefix `45a78785bd1aca95e27b41de328e5e096533821b2972778ef161bd75e746c81f`, migration/readback `34fb13a52ff8e00feeb04a20729fa4ec3990f61f075e849702dc9aa45058034d`, supervisor `2d3b5258ce71385e03b7937f9f7686acf0da1ed7e339b1bf3aefa7a56634c84e`.

Focused checks are effect-free: 37 focused tests passed; isolated child source import/runtime preflight,
frozen R16 import-path restoration, R20 scope/source/runtime admission, and the
R20 supervisor/custody/migration admission tests. Before any separate approval,
the repository commit, `/private/tmp` source archive and extraction, R16 retained
inputs, Python runtime, wheel/source pins, worker hashes and immediate pre-start
process inventory must be revalidated.
