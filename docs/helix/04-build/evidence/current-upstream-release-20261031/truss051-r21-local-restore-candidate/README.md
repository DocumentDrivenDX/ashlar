---
ddx:
  id: ashlar.evidence.truss051-r21-local-restore-candidate-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss051-r20-runtime-cohort-20261031
      kind: informed_by
    - id: ashlar.evidence.truss051-r20-start-failure-20261031
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# Truss 0.0.51 R21 PostgreSQL 18 layout preflight

**Status: draft candidate; effect-free only.** R21 is a small read-only layout
verifier for the R20 PostgreSQL startup failure. It does not restore or alter
PGDATA, start PostgreSQL, run migrations, publish data, or exercise ACK behavior.

R20's `postgres.log` says `FATAL: could not open directory "pg_notify": No
such file or directory`. The retained R16 cold-backup PGDATA and R20 restored
copy both lack `pg_notify`, and their recorded tree inventories match. The
missing directory predates R16/R20; R16 points to the R15 cold backup as its
source. The copy check proved equality, not PostgreSQL layout completeness.

The candidate compares PGDATA with all 23 directories in PostgreSQL 18
`initdb.c` `subdirs[]`. Its gate runs before an attempt directory is created.
It checks PG_VERSION 18, real directories without symlinks, and absence of a
`postmaster.pid` marker. It never changes the input. Lack of a PID marker alone
does not prove no process is using a directory; a later approved attempt must
still take a fresh process inventory.

The R16/R20 copy is missing 13 of those 23 directories:

- PostgreSQL's physical backup protocol says these six missing entries are
  copied as empty directories: `pg_dynshmem`, `pg_notify`, `pg_replslot`,
  `pg_serial`, `pg_snapshots`, and `pg_stat_tmp`. (The seventh documented path,
  `pg_subtrans`, is present.) This protocol evidence distinguishes directory
  structure from their contents; it is not evidence that other omitted
  directories are empty.
- The other seven missing entries are `pg_wal/archive_status`,
  `pg_wal/summaries`, `pg_commit_ts`, `pg_twophase`, `pg_tblspc`,
  `pg_logical/snapshots`, and `pg_logical/mappings`. Their emptiness is
  unproven. PostgreSQL describes `pg_twophase` as prepared-transaction state
  and `pg_tblspc` as tablespace links. R21 refuses the input and proposes no
  directory synthesis.

The read-only search covered the R16/R20 copies, R16's named R15 cold-backup
predecessor, the surviving R15 backup directories, the original R4 selected
path, and R12/R13/R14/R17/R18/R19 paths named by local evidence. R16 and R20
are the only present full copies, each with 1,248 files and 63,997,052 bytes;
both lack the same 13 directories. R12/R13/R14/R17/R18/R19 paths are absent,
the current R15 backup directories contain no files, and R4 lacks PG_VERSION.
The two available 731 MB R16 `.tar` files list Ashlar source files, not PGDATA.
No matching complete, stopped copy with proven Truss `.48` source/consumer
state was found.

The effect-free verifier's exact output for the retained R16 copy is preserved
in [`r16-layout-check.json`](r16-layout-check.json), with the interpreted
custody summary in [`r16-layout-audit.json`](r16-layout-audit.json). The check
exited 2 and confirms all 13 missing paths and no `postmaster.pid`; it did not
run a process inventory, so it does not claim the path is currently stopped.

R15's initial `.48` custody helper discarded the reason when it received
`CatalogViewUnavailable`. The later R15 R2 diagnostic recovered the exact
reason `context`; it did not return an available view or establish source,
consumer, or checkpoint custody. That R2 run records cleanup status
`postmaster_pid_absent`, no matching process rows, and an empty runtime socket.
R20's failed start has no verified stop receipt: its failure receipt says the
owned postmaster PID file was missing and `effects_may_remain` was true. These
are different source snapshots and neither qualifies the current R16/R20 copy.

R21 is executable only as an effect-free layout check and fixture test. It does
not make the current copy safe for a startup probe. The missing non-empty-capable
paths prevent a justified reconstruction, and the `.48` custody state is
unproven. The next step is to obtain an authoritative complete PostgreSQL 18
backup plus custody of the `.48` source, consumer, and checkpoint. Then a new
scope can review one local PostgreSQL start, one read-only `.48` custody request,
and one owned stop. No such run is authorized or prepared here.

For review, the future probe could use fresh output root
`/private/tmp/ashlar-truss-r21-custody-probe-20261031-49c4a8c7-96f1-4e12-a351-8d5d7d86ed3d`
and a sibling supervisor directory ending `-supervisor`. Proposed limits are
one attempt, zero automatic retries, sequential local only, 180-second child
deadline, 30-second PostgreSQL startup timeout, 2 GiB process-group RSS,
512 MiB attempt-root bytes, 2 GiB retained-plus-attempt bytes, 16 MiB combined
logs, 2 MiB helper output, and one `.48` read-only source/consumer/checkpoint
request. The probe would exclude migration, publication, ACK, seed,
continuation, and cloud actions. These values are review material only; the
future scope and owner approval must bind exact source, runtime, paths, hashes,
and process-custody rules before any run.

Focused tests use temporary fixtures and cover complete PG18 layout admission,
the exact 13 missing paths, wrong major version, symlink rejection, and a
present PID marker. They do not exercise PostgreSQL. R16/R20 artifacts were
read-only inputs and were not modified. Layout references: PostgreSQL 18
[`initdb.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/bin/initdb/initdb.c),
[physical backup directory handling](https://www.postgresql.org/docs/18/protocol-replication.html#PROTOCOL-BACKUP-BASEBACKUP),
and [database file layout](https://www.postgresql.org/docs/18/storage-file-layout.html).

Candidate files:

- `r21_pg18_layout_preflight.py` — small effect-free PG18 layout verifier.
- `test_r21_preflight.py` — focused fixture-only tests.
- `r16-layout-audit.json` — read-only audit of the retained R16 copy.
- `r16-layout-check.json` — exact rejected verifier output (exit 2).
