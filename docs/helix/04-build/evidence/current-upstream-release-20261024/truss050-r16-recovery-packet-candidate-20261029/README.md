---
ddx:
  id: ashlar.evidence.truss050-r16-recovery-packet-candidate-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss050-r15-selected-source-lifecycle-20261024
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# Truss 0.0.51 R16 latest-runtime adoption candidate

**Disposition: effect-free candidate only; native path remains blocked.** The
historical R15 `.48` to `.50` recovery proposal is no longer selected: `.50` is
not the latest runtime, and its ephemeral runtime has disappeared. This packet
records the discrepancy and a controlled restore proposal for historical review,
then pivots qualification to a fresh `.51` candidate. It is not a runnable
native packet or execution authorization. Do not launch the R15 worker or
supervisor, restore the cluster, start PostgreSQL, or reuse the consumed R15
approval.

## Read-only observations

`state-observation.json` records the exact read-only inventory captured on
2026-10-29. The selected PGDATA path has 11 files (17,197,691 bytes), no
`PG_VERSION`, and no `postmaster.pid`; its content tree hash is
`b979795a32e5b6c9f0d9bbda7849f130e20aa2846af96220cbbcbdfce6937c75`. The
read-only `pg_ctl status` command returned exit 4 with “not a database cluster
directory.” The first `ps` process inventory was denied by the sandbox. A later
parent-run filtered read-only inventory at 2026-10-29 22:35:50 UTC found no
process using this PGDATA path. Other PostgreSQL processes existed at unrelated
paths; their details are intentionally not retained. This is a time-bounded
observation, not final admission; the replacement supervisor must recheck before
any effect.

The earlier R15 cold backup still exists and its contents match the previously
verified receipt: PostgreSQL 18, 1,248 files, 63,997,052 bytes, tree hash
`67f15dff3198ba6b28bbe93a35cb7b595fe0439f84bcb4c14b9ef6d1f4c9effc`. Its
presence and matching hash do not by themselves authorize restore or establish
that it is still the correct source for a new attempt. The old R15 migration
registry path is absent. No contents were changed during this inspection.

The prior execution receipt described the same live data path immediately after
R15 as a complete 1,248-file tree of 63,998,352 bytes. The present 11-file tree
does not match that receipt or the old backup. The reason and timing of the
change are unknown. Preserve this discrepancy as evidence and resolve custody
before any native launch.

## Controlled recovery proposal

The next packet must use a new, unique attempt root and new worker/supervisor
copies. The R15 scripts are tied to the spent approval, old attempt ID, old
source snapshot, and old read-only helper; they are not reusable launch files.

After source freeze and separate execution authorization, the proposed recovery
sequence is:

1. Recompute the observed 11-file tree inventory and require exact equality with
   `state-observation.json`. Require a separately admitted process inventory to
   prove no server is running. Stop if either check differs or is unavailable.
2. Make a new, uniquely named, mode-0700 forensic copy of the current 11-file
   tree. Never overwrite or remove the original. Verify byte counts and the
   content-tree digest against the captured inventory. Stop on any mismatch.
3. Verify the preserved cold backup against its full tree manifest and the
   historical receipt. Copy it to a fresh, unique PGDATA target; do not restore
   over either the incomplete original or the preserved backup. Verify the new
   target byte-for-byte against the cold-backup manifest before PostgreSQL can
   start. Any owner-selected database identity or recovery-registry discrepancy
   stops the packet.
4. Create and verify a new attempt-scoped recovery registry and its pre-effect
   backup. The old registry is absent, so no prior registry may be represented
   as backed up or reused.
5. Re-run the exact read-only `.48` source custody gate with the corrected
   helper. The registrar role uses ordinary `READ COMMITTED READ WRITE` for
   Truss's public catalog view; the source-reader role remains `READ ONLY`.
   Accepted-source identity, exact source bytes, report, complete view,
   installation, feed, consumer, and pending checkpoint must all match retained
   evidence. Any unavailable view reason, mismatch, or uncertain result stops
   before migration.
6. This historical `.50` execution sequence is superseded and must not be run.
   Any future native packet requires a newly reviewed `.51` worker, a new exact
   one-run approval, and a fresh preflight. Its migration gate must verify both
   the outer `.51` index and embedded `.50` artifact before following the
   declared `.48` manifest `c433…` → intermediate `35fd…` → native manifest
   `288801…` route. Verify source and complete view after migration, then perform
   the already planned bounded `.4` genesis publication, independent complete
   verification, and protected ACK. Only after that ACK may the exact approved
   seed be submitted once; verify its actual continuation and protected ACK.

Any failed or uncertain precondition halts all later stages. Do not retry,
replay, restore again, restart, or infer a successful stage from a partial log.
Retain all original and new attempt receipts. Reconcile uncertain migration or
publication state from public receipts before proposing any new attempt.

## Exact limits and future approval scope

A future replacement supervisor must bind the exact post-review source
snapshot, corrected helper, new worker and supervisor, `.51` runtime receipt, bridge
readiness, seed request, migration route, target PGDATA manifest, discrepancy
preservation manifest, and recovery-registry backup. Until those bytes and
hashes are frozen, no approval payload can be prepared.

The historical `.50` approval proposal is retired. Any future `.51` approval
would need fresh exact scope; no such approval payload is prepared. The old
proposal described one controlled preservation copy of the 11-file discrepancy
tree; one copy of the verified cold backup to a fresh target; one PostgreSQL
start; one latest-only `.50` migration/resume; the verified genesis publication and protected ACK; one
approved seed submission after that ACK; and one actual continuation with
independent verification and protected ACK. Bounds remain one attempt, zero
automatic retries, 600-second child deadline, 1 GiB combined retained/attempt
bytes, 16 MiB logs, and no cloud resources. Any failure consumes the approval
and leaves later stages unauthorized.

The approval does not cover a second start, migration retry, source or consumer
registration, changes to the retained source, alternate migration route, seed
payload edits, broader Truss support, cloud/managed activity, or release claims.
The previous R15 approval remains spent and cannot authorize any of these
actions.

Public Truss 0.0.51 is the latest release. Its immutable
[official release](https://github.com/DocumentDrivenDX/truss/releases/tag/v0.0.51)
was published 2026-10-27. The universal wheel is
`umf_truss-0.0.51-py3-none-any.whl`, SHA-256
`eb59141c48e94bb93ab7053e9e8725012eee1749e4432e9c71804d23abbdff57`. The
release says its migration index is byte-identical to 0.0.50
(`4c6baf0282e3d0b5492f070c9c07d89ff0c9a819e748f3820f60b11c1f51c7aa`), the
0.49/0.50 native tuple is still current, and no physical migration edge is
fabricated for the package rename. It also says only the latest application
runtime is supported and limits populated native qualification to Linux x86_64,
Python 3.11, PostgreSQL 18.6. These are upstream release claims; Ashlar has not
qualified them.

The official 0.51 wheel is available at
`/private/tmp/umf_truss-0.0.51-py3-none-any.whl`; its SHA-256 matches the release
asset metadata. An isolated Python 3.12 runtime installed only that wheel with
`--no-deps --no-index`. Import and distribution inspection succeeded, and
Ashlar's candidate profile resolver identified `umf-truss==0.0.51` with
`ashlar-truss-current-catalog-evidence/0.6`. Focused tests cover the pinned
wheel/runtime evidence, current report reader, graph admission profile and
local `.4` continuation guards. The `.51` versioned local feed adapter delegates
to the already selected public page-shape validator after wheel/profile checks.
These checks establish package/profile compatibility only. No native catalog
method, source feed, migration, publication or ACK was invoked.

The release's outer `.51` migration-index manifest hash is
`d1672e85ae0e89137986fc57669186af47e5826366bd430284aba9b7cf52c37d`; its
embedded `.50` index artifact remains `4c6baf0282e3d0b5492f070c9c07d89ff0c9a819e748f3820f60b11c1f51c7aa`.
The required `.48` source route remains exactly two declared edges, through
`35fd…` to `288801…`; package rename does not add a migration. Ashlar's prior
worker binds the `.50` outer manifest directly to the embedded `.50` hash, so it
cannot be reused unchanged. A new worker must check both `.51` outer manifest
and embedded `.50` artifact hashes before preserving the route.

The prior `.50` environment path is not a usable isolated environment: its
Python symlink resolves to the shared Codex Python, `pyvenv.cfg` is absent, and
`truss-toolkit`, `umf-truss`, `weft-sql`, and `psycopg` are not installed there.
A fresh `.51` runtime and dependency lock must be created for any future native
proposal. The release's populated native qualification remains Linux
x86_64/Python 3.11/PostgreSQL 18.6; this macOS arm64/Python 3.12 effect-free
candidate does not qualify that tuple. Managed/cloud pathways and production
authority remain outside this candidate.

## Exact-current packet gates

The `truss-local` dependency line now points to the official
`umf-truss[native]==0.0.51` wheel with the release SHA-256. The isolated
macOS Python 3.12 native cohort receipt is
[`truss051-package-integrity.json`](truss051-package-integrity.json): the
installed distribution and `direct_url.json` carry the exact wheel hash, the
catalog runtime manifest is SHA-256 `288801f7…`, all 482 hashed `RECORD`
entries match, and `pip check` reports no broken requirements after installing
the base and `[native]` dependencies (`psycopg` and `psycopg-binary` 3.3.6).
The package/driver environment is effect-free; this did not invoke PostgreSQL
or Spark.

Truss 0.0.51's [release notes](https://github.com/DocumentDrivenDX/truss/releases/tag/v0.0.51)
state host-supplied PostgreSQL connections do not require the `[local]` extra;
local PostgreSQL installation is a separate optional runtime flow. Ashlar's
`truss-local` optional-dependency key is retained for compatibility, with its
content narrowed to the official `.51[native]` wheel. The exact wheel and
`[native]` dependencies pass `pip check`. Installing the former combined
`[local,native]` request failed because the configured package index does not
contain `pgserver-pg18==18.6.1`; Ashlar does not claim that extra or Truss's
packaged LocalPostgres runtime is installed or qualified.

The old R15 launcher is specifically incompatible with this dependency change:
`run_supervisor_r15.py` hardcodes `PG_CTL` under the venv's
`site-packages/pgserver/pginstall/bin/pg_ctl`, and `campaign_worker_r15.py`
requires `pgserver-pg18==18.6.1` in its cohort. Those historical scripts stay
unchanged and are not reusable. A newly versioned R16 supervisor must use an
explicit externally supplied PG 18.6 binary path, verify its version, realpath
and hash during preflight, and bind its ordinary psycopg connection factory to
the same owned local cluster. The observed Homebrew executable is
`/opt/homebrew/opt/postgresql@18/bin/pg_ctl` (resolved to
`/opt/homebrew/Cellar/postgresql@18/18.6_1/bin/pg_ctl`, SHA-256
`3021087af95fd33bfd83106c85de47c4756e44aa7edde78446e7593604d1b451`); its
read-only `--version` output is `pg_ctl (PostgreSQL) 18.6`. This host observation
does not authorize starting it or qualify a populated native migration. Source
freeze remains blocked, so no replacement worker or supervisor has been made.

The versioned effect-free helper
[`truss_local_postgres.py`](../../../../../../src/ashlar_host/truss_local_postgres.py)
admits a packet-supplied canonical `pg_ctl` path and SHA-256, then runs only the
bounded `--version` probe. R16 preflight must call it with canonical path
`/opt/homebrew/Cellar/postgresql@18/18.6_1/bin/pg_ctl` and the hash above. Unit
tests cover hash/version mismatch and the no-start boundary; the actual binary
passed the read-only check. The future supervisor still needs exact source and
worker binding before it may use this observation to authorize a process action.

Source freeze is also still blocked. At receipt capture, HEAD was
`6b5834ed0c5ec6be0ff2b66db25103de23023639`, with 31 modified/untracked status
entries in the shared checkout. These include concurrent source, test, and
evidence work, so there is no exact R16 source archive or worker/supervisor
binding yet. The receipt lists the observed status entries. After the shared
changes are reviewed and settled, freeze a fresh source archive and manifest,
then regenerate the candidate worker, supervisor, and preflight from that
snapshot. Until then, no worker or supervisor is prepared.

The repository was observed at commit `c83ae1a2b1b2a9b23e9ce56e19dd2de5f43a7a66`
and tree `d818d6444bcd8f68bf08e19067ca39776d5f2774`. A concurrent tracked source
edit was under review during packet preparation, so this observation is not the
selected-source binding. Freeze the source snapshot only after that review ends;
then rebuild its archive and manifest and regenerate all worker/supervisor
preflight receipts and approval parity from those exact bytes.

After that observation, the AS2 change was committed locally at
`6b5834ed`; upstream dependency review has since settled and the `.51` pin is
updated. The working tree still contains unrelated in-flight changes. Freeze
source and dependency pins together only after those changes are reviewed and
settled, immediately before the final effect-free preflight and independent
review.

The corrected helper SHA-256 is
`b99cd1edb0855de64c68322603d4ba16be1c697d21d925775e4b9ea007dec6e4`; focused
non-native controls passed 3 tests. The versioned `.51` profile, reader, feed
adapter and external PostgreSQL helper passed 34 focused unit tests across five
suites; compile checks and `pip check` also passed. The official wheel profile
resolved in the isolated runtime. The exact source hash freeze remains pending
because the shared tree is still changing; do not bind transient hashes.

Effect-free checks already run on the current working tree (repeat after the
final source snapshot is frozen):

```sh
PYTHONPATH=src:tests .venv/bin/python -m unittest -v test_truss_local_postgres test_r04_schedule test_truss_current_catalog test_truss_catalog_reader test_truss_continue
PYTHONPATH=src /private/tmp/ashlar-r16-truss051-effectfree-312/bin/python -m pip check
PYTHONPATH=src /private/tmp/ashlar-r16-truss051-effectfree-312/bin/python -c "from ashlar_host.truss_local_postgres import inspect_external_postgres_18_6_pg_ctl; print(inspect_external_postgres_18_6_pg_ctl('/opt/homebrew/Cellar/postgresql@18/18.6_1/bin/pg_ctl','3021087af95fd33bfd83106c85de47c4756e44aa7edde78446e7593604d1b451'))"
.venv/bin/python docs/helix/04-build/evidence/current-upstream-release-20261024/truss050-r15-selected-source-lifecycle-20261024/catalog-view-context-fix-r1/test_context_compatible_helper.py
<new isolated runtime>/bin/python <new packet>/check_packet.py
<new isolated runtime>/bin/python <new packet>/run_supervisor.py --preflight
```

The final two commands are placeholders until the R16 worker/supervisor have
been generated with fresh paths and exact hashes. Do not substitute the R15
preflight receipt. The required final supervisor process and IPC admission
checks must pass in the explicitly allowed execution context before an owner
can review a concrete approval payload.

The observations recorded here were gathered with these read-only commands:

```sh
ps -Ao pid,ppid,lstart,command
/private/tmp/ashlar-truss050-current-cohort-9c745278-r13/runtime/venv/lib/python3.12/site-packages/pgserver/pginstall/bin/pg_ctl status -D /private/tmp/ashlar-truss048-source-acceptance-attempt-01-r4/postgres/data
python3 -m json.tool docs/helix/04-build/evidence/current-upstream-release-20261024/truss050-r16-recovery-packet-candidate-20261029/state-observation.json
```

The first command's initial attempt was denied by the sandbox; a later filtered
parent-run read-only process observation is recorded above. The second returned
exit 4 and identified no valid cluster. The third is a packet-format check only.

## Open evidence

- Final post-review source archive/hash after the shared checkout changes are
  reviewed and settled.
- New effect-free worker/supervisor copies and preflight proving the `.51` outer
  migration index contains the exact `.50` index asset and that source `.48`
  selects the same two-edge route. The old worker is incompatible at this
  check because it expects the outer index hash to equal the embedded `.50`
  hash.
- Effect-free confirmation of the exact required Truss native method/API
  behavior in the platform tuple the R16 packet will actually use. The `.51`
  base plus `[native]` dependency cohort passes on macOS/Python 3.12, while the
  release only qualifies populated native migrations on Linux/Python 3.11. The
  Homebrew PostgreSQL 18.6 `pg_ctl` is observed but not yet bound to a launched
  owner-controlled server or connection factory.
- Independent Sol review and, only after that review, a new exact one-run owner
  approval for any future native execution.
- Why the selected PGDATA directory was reduced from the R15 1,248-file state
  to 11 files, and whether this path remains the intended owner-selected source.
- Final process admission immediately before any effect; the parent-run
  filtered process observation above is only a prior snapshot.
- Current source/commit/archive identity after concurrent source review.
- New attempt paths, preserved discrepancy-copy digest, restored-target digest,
  and new recovery-registry backup identity.
- Fresh effect-free packet preflight and independent Sol review.
- A new explicit one-run owner approval after those items are complete.
- Whether the public `.48` catalog view is available under the corrected
  transaction context; no PostgreSQL query was run here.

No database was started, copied, restored, or modified. No migration, feed,
publication, ACK, seed, continuation, or cloud operation was run.
