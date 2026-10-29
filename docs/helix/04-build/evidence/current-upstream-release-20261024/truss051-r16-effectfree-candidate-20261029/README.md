---
ddx:
  id: ashlar.evidence.truss051-r16-effectfree-candidate-20261029
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

# Truss 0.0.51 R16 effect-free candidate

**Disposition: static candidate preflight passes; native execution is blocked.**
This packet is bound to the exact committed Ashlar snapshot
`48fb9cb047f95854d59d0882af31156aab5f1f99` (tree
`5776238063057a36b2b2dff9b19de9f264d7ac21`). The narrow executable-source archive
contains only `pyproject.toml`, `src/`, and `sql/`; it is 8,755,200 bytes with
SHA-256 `cb57fdb7d338f345cfad157463fdbe94a2be7dcf69e728c474954c2939c14a0b`.
It is stored outside the repository at
`/private/tmp/ashlar-r16-source-48fb9cb0.tar`; unrelated working-tree entries
and historical evidence are excluded. This snapshot was selected after the parent reported the merge
complete. The binding identifies this archive regardless of later repository
HEAD movement; changing any source bytes in this archive requires a new archive
and review.

After this freeze, repository `HEAD` advanced to
`fa601db54183e56318d35de6c5a07e9ba4de5028` with a TD-009 source-holder
proposal change in three files. That delta is outside the frozen archive and
does not touch the Truss/native execution path; R16 remains intentionally bound
to `48fb9cb0`, not current `HEAD`. The delta is recorded in
`r16-input-freeze.json`.

The candidate uses official `umf-truss` 0.0.51 wheel SHA-256
`eb59141c48e94bb93ab7053e9e8725012eee1749e4432e9c71804d23abbdff57`, from the
isolated Python 3.12.14 environment recorded in
`../truss050-r16-recovery-packet-candidate-20261029/truss051-package-integrity.json`.
The runtime manifest hash is
`288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4`; its
embedded 0.0.50 migration-index asset hash is
`4c6baf0282e3d0b5492f070c9c07d89ff0c9a819e748f3820f60b11c1f51c7aa`. The
0.0.51 outer index hash is
`d1672e85ae0e89137986fc57669186af47e5826366bd430284aba9b7cf52c37d`. The
declared route remains source manifest `c4339ac7f09ab5a004c272ab00962107a970938004c1765ec4163e4d5ca9b6f0`
→ intermediate manifest `35fd797b5cf48dd888c68e53b9e599df20cffe00f8f3d33e1563820ea808d59f`
→ current manifest `288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4`.
No artificial migration edge is introduced. The effect-free package receipt
reports 482 wheel RECORD hashes checked, zero mismatches, 213 unhashed entries,
and a successful `pip check`.

The only admitted external PostgreSQL tool candidate is the Homebrew PostgreSQL
18.6 `pg_ctl` at
`/opt/homebrew/Cellar/postgresql@18/18.6_1/bin/pg_ctl`, SHA-256
`3021087af95fd33bfd83106c85de47c4756e44aa7edde78446e7593604d1b451`, reporting
`pg_ctl (PostgreSQL) 18.6 (Homebrew)`. The package cohort has `psycopg` and
`psycopg-binary` 3.3.6; `pgserver-pg18` is absent. Truss 0.0.51 supports a
host-supplied PostgreSQL connection without the `[local]` extra, but the host
connection factory and live server compatibility have not been exercised.
The `pg_ctl --version` check is read-only and is not server qualification.

## Fresh worker and supervisor

`r16_preflight_worker.py` verifies only regular-file presence and SHA-256 of the
source archive, official wheel, and host `pg_ctl`; runs `pg_ctl --version`; reads
the frozen archive's exact `truss-local` 0.0.51 native pin, installed Truss
`direct_url.json`; and invokes `pip check` in the isolated runtime.
`run_supervisor_r16.py` verifies the worker SHA-256
`f03a190bbac08cf4caa59ed8927c0f48b9d346eeecd1260e03546b7b8a243fb2` and invokes
only this worker with a 90-second
deadline and rejects any receipt that does not explicitly say both
`native_effects=false` and `execution_admitted=false`. Both CLIs require
`--preflight-only`; neither contains a native mode, restore, database start,
migration, source/feed operation, publication, ACK, seed, or continuation path.
Neither script uses the R15 worker, supervisor, or spent approval.
The supervisor SHA-256 is
`a82c42f454889849147df5ab1126325781bed91629df0ff0bc6489e54816aba9`; the
test file SHA-256 is
`347a73e15d5bcbbdb341fcf374e176ae2b9354b3c95c83ca62558d7f63e7cb06`. These
inputs are collected in `r16-input-freeze.json` (SHA-256
`9c53cfcafbef4d7055cc0e68ac75aa6925dd94edc9e69ec1f2a1a885dd3cfbf3`).

The fresh effect-free run is preserved in `preflight-receipt.json`. It verifies
the exact narrow archive/wheel/pg_ctl hashes, the tracked exact seed request, installed direct URL and successful
`pip check`, and declares execution not admitted. Three focused inert tests pass
in `test_r16_preflight.py`: wrong source hash stops before external commands,
preflight mode is mandatory, and the frozen source/runtime/tool constants are
fixed. No database process or native Truss method was invoked.

## Native state discrepancy and custody blocker

`state-observation-r16.json` freshly inventories both retained trees by file
hash. The selected PGDATA directory remains 11 files / 17,197,691 bytes with no
`PG_VERSION` or `postmaster.pid`, tree hash
`b979795a32e5b6c9f0d9bbda7849f130e20aa2846af96220cbbcbdfce6937c75`. The
preserved R15 cold backup is PostgreSQL 18, 1,248 files / 63,997,052 bytes,
tree hash `67f15dff3198ba6b28bbe93a35cb7b595fe0439f84bcb4c14b9ef6d1f4c9effc`;
this matches the historical cold-backup receipt. The prior R15 post-attempt
tree was 1,248 files / 63,998,352 bytes, hash
`abb464cc096dfce6bc6bd2720f6f6a17aa92c08af5a601146736ca71a308e6d2`. The
reduction to 11 files is unexplained. The old R15 migration registry is absent.
No restore is proposed against the original path: a later separately reviewed
packet would first preserve its exact current 11-file state to a new path, then
copy the verified cold backup to a different fresh target. Neither action is
performed here.

A fresh filtered process inventory was attempted with
`ps -Ao pid,ppid,command | rg -i postgres`, but the sandbox denied it with
`operation not permitted`. The prior parent-run observation at
2026-10-29T22:35:50Z found no process using the selected PGDATA and did find
other PostgreSQL processes. It is stale and cannot admit a future action; a
fresh exact-path process check is mandatory immediately before any native
effect.

R15 stopped at its initial `.48` source-custody gate because its helper got an
empty view and raised `JSONDecodeError`. No accepted-source identity, full
catalog view, report/view equality, feed, consumer, or checkpoint comparison was
completed. No seed was submitted. The exact historical seed request remains
tracked at `../truss-049-lifecycle-ready/seed-request.json` (1,657 bytes, SHA-256
`56aed4d7a4789f45cdaa7af55a834ff38ca2b4bcb75f44658b3031ba5f8aa771`, Git blob
`93951954795ce8fa376871626d6c4def5454dfa7` in `48fb9cb0`); its request key is
`ashlar-current-048-lifecycle-seed-r1` and it carries seven operations. The former
`/private/tmp` copy is absent. R15 authorization is spent and does not authorize
resubmission. Historical source, origin, feed-registration, and readback receipt
files named in R15 preparation are absent from their old `/private/tmp` paths.
The expected source and feed identities and hashes are preserved as strict pins
in `future-scope-template.json`; current .48 database custody must be observed
after a future backup restore/start and match those pins before migration. The
exact source content hash can be verified from the Truss read object without
retaining a second source-file copy. The prior `.50` bridge-readiness receipt
binds an older Ashlar commit and cannot qualify this `.51` snapshot.

## Future packet boundary (proposal only)

This document and its preflight are not an execution packet and contain no
authorization payload. A future native packet must be freshly reviewed and bind byte-identically in
worker, supervisor, and a new one-run approval: exact
source commit/archive and R16 code hashes; `.51` wheel, installed runtime and
outer/embedded migration-index hashes; Homebrew `pg_ctl` and psycopg versions;
both retained-state inventories; fresh discrepancy-preservation and restored
target paths/digests; a new registry pre-effect backup; current accepted-source
identity, complete public catalog view, feed/consumer/checkpoint custody; exact
seed request bytes/hash and post-ACK stage order; and all output paths. Any
changed byte or unknown value stops admission. R15 approval hash
`239bc4e2a500aeb5804ba071da0daf3cada9b84a1a5a1151e7cb6684f730304a` is spent
and must not be reused.

`future-scope-template.json` separates fixed approval inputs from outputs that
cannot exist until the authorized run reaches their stage. Before approval, the
native worker/supervisor hashes, external connection factory, exact fresh paths,
limits, historical expected source/feed/checkpoint identities, seed bytes, and
one-run authorization must be reviewed and bound. The `effect_free_preflight_code`
hashes identify the current read-only scripts only; they are not native code
pins. The native code hash fields are intentionally null until reviewed R16
lifecycle code exists.

Runtime-derived receipts are intentionally null before execution. Their stages
and fail-closed gates are explicit: preserve and verify the current 11-file
tree; copy and verify the cold backup at a new path; create and verify the
registry backup; start restored PostgreSQL once; then observe source/view/feed/
consumer/checkpoint and compare all values to the pre-run pins before migration.
Bind the actual genesis-page hash at that gate and require the same bytes after
migration before publication. Any mismatch stops before the next effect. The
current worker/supervisor remain effect-free preflight only; a reviewed native
worker/supervisor and qualified runtime cohort are still required.

The backup was additionally checked read-only with the hash-bound Homebrew `pg_controldata` binary at `/opt/homebrew/Cellar/postgresql@18/18.6_1/bin/pg_controldata` (SHA-256 `0856cb839c35b55d1ef5abd51e94188a1d1b67d96d5e3ff134791104efef61fa`). It reports PostgreSQL control version 1800, system identifier `7699627355613432738`, cluster state `shut down`, checkpoint time 2026-10-22 19:59:03, checksums version 1, and no backup end required. This confirms cold-backup readability only; it says nothing about source or feed authority.

A single future approval can safely cover one staged run if it binds the expected historical identities and hashes in advance and the worker makes every stage conditional: after copy/start, it freshly reads and compares source identity/hash, catalog revision, full view/report, feed, consumer registration, and genesis checkpoint against those frozen values; it then records the actual genesis-page hash before migration and requires the same page bytes after migration before publication. Any discrepancy exits before migration. This does not need a second approval because no unbounded value is accepted: dynamic observations must equal the pre-bound expected values. If any expected view/page cannot be bound or observed exactly, stop and prepare a new proposal.

The selected external Homebrew runtime has PostgreSQL 18.6 and JDK 21.0.13; however, the old Spark/Delta JAR paths from R15 are absent and no matching Spark/Delta jars were found under `/private/tmp`. The `.51[native]` environment contains Truss and psycopg but not PySpark/Delta. Consequently a native lifecycle worker is not yet runnable: the exact local Spark/Delta runtime and compatible connection/publication execution cohort must be reassembled and hash-qualified before implementation can be frozen.

Proposed hard ceilings for a separately authorized single local attempt are:
one attempt, zero automatic retries, 600 seconds child runtime, 512 MiB maximum
attempt root, 16 MiB combined logs, 2 GiB aggregate retained-plus-attempt
accounting (including the 8,755,200-byte executable source archive and 126,228 KiB
isolated Truss runtime observed now), sequential execution, and zero cloud resources.
The supervisor must compute device/inode-aware actual retained bytes and reserve
the full attempt cap before effects; if the aggregate would exceed 2 GiB, stop
and require a newly bounded proposal. These are design ceilings, not approval.
The proposed single-run operation order, which requires a fresh exact human
approval after Sol review, is: preserve the incomplete tree; verify/copy the cold
backup to fresh PGDATA; verify the new registry backup; start PostgreSQL once;
perform fresh source/view/feed/consumer/checkpoint custody and stop before
migration on any mismatch; bind the observed genesis-page hash; run the exact
`.48` → `.51` route and verify source/page again; one bounded
genesis publication with independent full verification and protected ACK; then
only the exact admitted seed once, followed by independent continuation
verification and protected ACK. Failure or uncertainty consumes that one run;
no restart, replay, second migration, or later-stage action follows.

## Commands and checks

Recreate the source archive only from the exact commit and compare its SHA-256
before using this candidate:

```sh
git archive --format=tar 48fb9cb047f95854d59d0882af31156aab5f1f99 \
  pyproject.toml src sql -o /private/tmp/ashlar-r16-source-48fb9cb0.tar
shasum -a 256 /private/tmp/ashlar-r16-source-48fb9cb0.tar
```

The effect-free preflight command used for the receipt was:

```sh
python3 run_supervisor_r16.py --preflight-only \
  --archive /private/tmp/ashlar-r16-source-48fb9cb0.tar \
  --wheel /private/tmp/umf_truss-0.0.51-py3-none-any.whl \
  --runtime-python /private/tmp/ashlar-r16-truss051-effectfree-312/bin/python \
  --pg-ctl /opt/homebrew/Cellar/postgresql@18/18.6_1/bin/pg_ctl \
  --seed-request docs/helix/04-build/evidence/current-upstream-release-20261022/truss-049-lifecycle-ready/seed-request.json
```

Focused tests:

```sh
.venv/bin/python -m unittest discover \
  -s docs/helix/04-build/evidence/current-upstream-release-20261024/truss051-r16-effectfree-candidate-20261029 \
  -p 'test_r16_preflight.py' -v
```

Do not add an execution subcommand to these scripts. A native worker and
supervisor would be a separate artifact after source/seed custody, filesystem
discrepancy, process admission, runtime/source freeze, and exact new one-run
approval are resolved. No native effect was attempted in R16.
