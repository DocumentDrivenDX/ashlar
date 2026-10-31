---
ddx:
  id: ashlar.evidence.truss051-r20-start-failure-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss051-r20-runtime-cohort-20261031
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# Truss 0.0.51 R20 PostgreSQL startup failure

The owner approved exactly one local R20 lifecycle under scope SHA-256
`8365c414a5ca8f4b50bda8b67b6d047d563bc0c8d7d2f618e7277b879989f7e0`.
The approval artifact is retained with the [R20 packet](../truss051-r20-runtime-cohort/approval-r20-exact-one-run.json)
(SHA-256 `7cc615440992734104ff148097f007a5ea952626cb2b07319127d388ed1c83c7`).
The isolated source preflight and effect-free scope admission passed before the
single invocation. The supervisor ran the child for 6.246 seconds; the child
exited 2 and the supervisor exited 1. Its last observed process-group RSS was
274,694,144 bytes, within the 2 GiB cap; no child process-group member remained.

The retained `postgres.log` records PostgreSQL 18.6 refusing startup at
2026-10-31 19:36:26 EDT: `could not open directory "pg_notify": No such file or
directory`. The selected R16 cold-backup PGDATA and the byte-matched R20 restored
copy both lack `pg_notify`. Thus the immediate defect is in the inherited cold
backup's directory shape, before the R20 copy, not a divergent R20 copy. R20
created the bounded attempt, preserved the selected discrepancy, copied the
cold backup, wrote registry-absence and pre-start inventory receipts, and
attempted PostgreSQL startup. No migration, genesis publication, seed,
continuation, or ACK receipt exists. The exact one-run approval is consumed.

The child's durable failure receipt conservatively says
`server_may_be_running` and `effects_may_remain: true` because the owned PID file
was absent and its cleanup path refused an unbound stop. Immediate read-only
reconciliation at 2026-10-31 19:37 EDT found no process owning the R20 restored
PGDATA in `ps -Ao pid=,ppid=,command=`, `pg_ctl status -D` returned `no server
running`, and `pg_isready` against the R20 private socket on port 55441 returned
`no response`. The R20 PID file and socket endpoint were absent. These
observations resolve the startup uncertainty at that time; they do not rewrite
the original failure receipt or assert that a PostgreSQL server never briefly
started. No stop or cleanup was attempted. The attempt root remains intact at
`/private/tmp/ashlar-truss-r20-full-lifecycle-a8be62bc-34f9-4a97-8dc3-dc948c752123`.

The byte-for-byte small receipts and logs are retained here. SHA-256:

| File | Digest |
| --- | --- |
| `supervisor-receipt.json` | `535f68249495626a4b1d827789d5f42448a86b1d5d3f7f0b8ded7714932a8c40` |
| `supervisor-failure.json` and `child.stderr.log` | `0e75d00cf175f29b3a9b032134f4b798ab7abbc14a364360c5626b5d7f548578` |
| `child.stdout.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `postgres.log` | `b90a2e984d234af5c9a021d7a0b5b0d6063f9b06a4d1c6346d5445c4e448edb2` |
| `postgres-start-intent.json` | `06937679555aed25c4a363433cc0275f4b7b52eae89ee180749f89a01c117b8d` |
| `cold-backup-restored.json` | `18264888e9b0feec6cc516a1a4f373f0dcf5fd03a2ea5bbdd53b20ea753fd412` |
| `pre-start-process-inventory.json` | `46e570b75f6da24fed20426a6907d6a83aefd68daf8f67200eb3d683002f4c6f` |

The next lifecycle requires a complete, stopped PostgreSQL source and a new
reviewed scope. This evidence does not qualify Truss source acceptance or any
publication/consumer behavior.
