# G03 local `.4` transaction-1 one-shot packet

This separately versioned r2 packet is an inert candidate for one private, fresh local Delta installation followed by the reviewed `.4` transaction-1 runtime bridge. This packet has not been executed. Sol should review the packet digest, exact current commit, source/oracle and command before anyone supplies an execution permit. The claim is limited to local G03 effects evidence; this does not qualify the selected UMF SDK 0.5.8 and Core 0.10.3 release cohort.

The r1 attempt initialized the six-role installation and failed in the second
Spark session before any transaction graph effects because ZSTD was not
effective. Its retained directory is
`/private/tmp/ashlar-g03-delta4-txn1-attempts/ashlar-g03-txn1-19c69741b58c4e46a15d4f3645ba7e23`;
it must not be retried. The reviewed bridge fix applies and verifies the ZSTD
setting before admission in every new transaction session, while admission
continues to refuse later drift without repair. This r2 packet binds source
commit `cd74a5d0157047e909ffce2079254e8ee1b588bc` and the updated exact
`src/ashlar_host` tree digest.

The prior r1 attempt initialized the six-role installation and then failed before transaction graph effects because a second Spark session did not have the required ZSTD writer setting. The retained failed attempt is `/private/tmp/ashlar-g03-delta4-txn1-attempts/ashlar-g03-txn1-19c69741b58c4e46a15d4f3645ba7e23` and must not be retried. The reviewed bridge fix applies and verifies that setting before admission in each new session; close admission still only verifies. This r2 packet binds the reviewed source commit `cd74a5d0157047e909ffce2079254e8ee1b588bc` and the updated exact source-tree digest.

The source is the frozen eight-event `typed-graph-1` JSONL graph in `inputs/`. The matching synthetic source oracle preserves the exact large quantity as JSON token text. The runtime uses the independently retained R16 Python 3.12.14, PySpark 4.2.0, Delta Spark 4.4.1 and Java 21 cohort. Each worker rechecks the packet, source tree, interpreter/runtime pins and input digests before entering its stage. The earlier Oct. 24 interpreter path failed its byte pin and is superseded; the packet no longer invokes it. An earlier UMF producer path also failed because it had no Git metadata; it has been replaced by the clean exact-commit clone named below. The R16 PySpark identity is recorded against that cohort’s retained PyPI receipt, source tarball, artifact inventory and JAR inventory. The UMF producer checkout is a clean local clone at commit `2ede4c879bea5db13794a6fa30e67c821306c17f`. The selected Delta4 bridge candidate is `259bf1ed91466ff7e9782d7517c14f2112282026`. Both are historical relative to the selected UMF SDK 0.5.8 / Core 0.10.3 cohort, so even a successful run remains local effects evidence only.

The supervisor starts one isolated child for six-role initialization, waits for its entire process group to exit, then starts one child for transaction 1. It enforces a 450-second total execution deadline, a 2 GiB process-group RSS ceiling, a 512 MiB attempt-directory ceiling, and 1 MiB combined stdout/stderr across both stages. Spark is constrained to `local[1]` and 512 MiB driver memory. The reviewed code commit must be an ancestor of current HEAD, source-directory hashes must match exactly, and the prepared attempt captures current HEAD; the execution permit binds both. A `dispatch.json` is written before the first child starts; any later invocation for that attempt refuses. Failure preserves the attempt directory, diagnostics, journal and Delta logs. A partial attempt requires separate reconciliation and cannot be rerun through this packet.

The only allowed graph activity is the native `.4` attempt phases `prepared` and `applying`, followed by the three frozen transaction-1 graph INSERT effects. Expected terminal versions are `object_current=2`, `edge_current=2`, `whole_source_history=2`, `tombstone=1`, `publication_attempt_phase=3`, and `publication_manifest=1`. Expected native row counts are 4 objects, 4 edges, 8 history rows, zero tombstones, two attempt-phase rows and zero manifest rows. The runtime bridge must validate complete four-role bags and hashes before emitting its `effects-validated` result.

This r2 packet does not create a publication manifest, mark the attempt `applied`, acknowledge the source, exercise Weft reads, or claim Truss/production acceptance. Its execution remains pending review and a separately supplied one-shot permit. The earlier r1 attempt is not reusable and is retained only as pre-effect failure evidence.

## Effect-free packet checks

Run these using the pinned interpreter:

```sh
cd docs/helix/04-build/evidence/current-upstream-release-20261029/g03-weft056-delta4-consumer-candidate/one-shot-native-attempt-r2
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python -B -m unittest -v test_packet
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python -B packet_digest.py
```

The digest command prints the immutable packet identity to be reviewed. A prepared attempt is created only after review with:

```sh
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python -B packet_supervisor.py prepare \
  --attempt-parent /private/tmp/ashlar-g03-delta4-txn1-attempts-r2 \
  --packet-sha256 '<reviewed packet digest>'
```

Preparation verifies runtime and source inputs but starts no Spark session and creates no Delta tables. It prints the private attempt path. After review of that exact `prepared.json`, the sole execution permit has this closed shape, with the digest calculated from the retained file:

```json
{
  "profile": "ashlar-g03-delta4-txn1-execution-permit/0.2",
  "packetTreeSha256": "<reviewed packet digest>",
  "sourceCommit": "cd74a5d0157047e909ffce2079254e8ee1b588bc",
  "currentCommit": "<current HEAD captured by prepared.json>",
  "preparedSha256": "<prepared.json SHA-256>",
  "execute": true
}
```

The single supervised execution command is:

```sh
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python -B packet_supervisor.py execute \
  --prepared '<private attempt>/prepared.json' \
  --prepared-sha256 '<prepared.json SHA-256>' \
  --permit '<reviewed one-shot permit JSON path>' \
  --packet-sha256 '<reviewed packet digest>'
```

Do not execute this command until Sol has reviewed the finalized packet. No retry is allowed after `dispatch.json` exists, including after a failed or interrupted child.
