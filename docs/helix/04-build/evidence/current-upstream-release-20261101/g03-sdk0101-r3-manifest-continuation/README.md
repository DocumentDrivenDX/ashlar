# G03 SDK 0.10.1 R3 retained manifest continuation

This packet prepares a same-installation, manifest-only continuation after R3
completed transaction one but stopped before manifest finalization. It binds the
original R3 installation, source inputs, SDK 0.10.1 producer, runtime assets,
and retained result receipts. The installation stays at its original path
because its SQLite source scope is bound to that path and inode.

R3 completed in 617649 ms: initialization took 79389 ms and transaction one
took 538102 ms. The verified six-role vector is edge/object/history v2,
attempts v3, and manifest/tombstone v1. The journal has exactly three committed
graph operations, two committed prepared/applying metadata operations, one
transaction-one output receipt, no publication artifact, and no manifest
operation. R3's terminal receipt records `completed-verified`, an empty process
group, and no publication or ACK. Evidence is bound by digest in
[`invocation.json`](invocation.json); the original receipts remain in the
retained attempt.

The candidate source commit `f16cc4ed` adds a fail-closed path for a complete
committed graph prefix. It validates the original plan, all three committed
operation receipts, the exact output receipt, and a fresh six-role observation,
then finalizes the manifest without calling graph prepare, apply, or recovery.
The path refuses missing, extra, augmented, or noncommitted operation rows.

Follow-up commit `0693f1a4` binds SDK 0.10.1 Git custody checks to the process's
existing private temp root. The checker found that macOS Git otherwise emits a
`confstr()` temp-path warning to stderr in this restricted host, which the
producer correctly treats as refusal. The fix validates the existing temp
directory's ownership and permissions before passing it as `TMPDIR`; it creates
no directory and accepts no shared writable temp root.

Run the effect-free checker with the selected Python 3.12 interpreter and this
checkout's source files available. The checker performs immutable SQLite and
receipt checks, verifies exact source and runtime files, then calls the real
R3 transaction-one runtime until its actual Spark boundary. It uses the real
SDK 0.10.1 source admission and output writes only into a disposable temporary
directory, while replacing the Spark opener with a sentinel. It verifies that
no Spark module loads and the original installation bytes remain unchanged.
The same exact `--preflight` command then copies the immutable invocation into a
disposable private attempt and launches the actual worker with the recorded
path/hash arguments. The worker checks the source, SQLite journal, six Delta
heads, runtime config, and fresh disposable output, then returns before the
native API call. The planned R5 attempt path remains absent. It also parses the
retained transaction-one receipt and verifies that the current observation
projection reproduces the original `snapshot_observation_json` bytes exactly.
The completed checker output is retained in
[`preflight-result.json`](preflight-result.json).

The executable continuation is a small worker plus a one-shot supervisor.
The supervisor runs the effect-free checker before consuming the fresh
namespace, refuses an occupied Java slot, starts one worker in its own process
group, samples aggregate process-group RSS, and stops the whole group at the
900-second or 2 GiB limit. The worker verifies the invocation and all retained
custody, calls `run_configured_local_delta4_transaction1` exactly once, and
checks the completed manifest vector plus unchanged graph and metadata journal
rows. It never retries. Any failure after dispatch is classified uncertain;
the installation and attempt are preserved for read-only reconciliation.

The native envelope uses one local `[1]` Spark process, a 512 MiB Spark driver,
and 60-second SDK producer checks. Its only planned native change is one local
manifest commit after full revalidation. No graph operation is replayed; no
source ACK, cloud, Truss, or publication artifact is included. A post-run
receipt binds the new full manifest vector and confirms the process group is
empty.

Effect-free checks are available from the exact argv in
[`invocation.json`](invocation.json): use its `effectFreeArgv` with the
repository root as the working directory, or run the supervisor with `--check`
to validate the invocation and limits without process inspection. The
`dispatchArgv` field is the sole native entry point; default supervisor mode is
dispatchable and must be used only after independent review.

The first continuation worker dispatch stopped at 4,455 ms before the native
API call: the head checker indexed the metadata roles as `publication_attempt_phase`
and `publication_manifest`, while the retained carrier profile names those
physical roles `attempts` and `manifest`. Its supervisor recorded
`uncertain-after-dispatch` and an empty process group. Read-only reconciliation
confirmed the original heads remained object/edge/history v2, attempts v3, and
tombstone/manifest v1; `operations.sqlite` SHA-256 is
`10754c3d2b2e947b16b0770f5af13f90353925728a553290314fba403c00c016`, with 3
committed graph operations, 2 committed metadata operations, 1 output receipt,
0 publication artifacts, and no WAL. The first attempt is preserved and must
not be replayed. The fresh R4 invocation has a separate namespace and its
effect-free worker preflight now exercises the physical-role mapping, journal,
head checks, exact source bindings, and runtime config construction before
stopping ahead of the native API. Its exact output, including both the retained
runtime boundary check and the actual worker pre-call check, is retained in
[`r4-effect-free-preflight-result.json`](r4-effect-free-preflight-result.json).

R4 reached the output-receipt retention boundary after 139348 ms and refused
before any manifest operation or manifest Delta commit. The supervisor recorded
`uncertain-after-dispatch` and an empty process group. Read-only reconciliation
confirmed the original SQLite hash and journal counts remain unchanged, with
the six heads at object/edge/history v2, attempts v3, and tombstone/manifest v1.
The receipt mismatch came from `_observe_effect_prefix` adding current manifest
reconciliation fields to a receipt schema written by R3 with its original eight
transaction-one fields. The source fix validates the full current observation,
then projects its stable transaction-one fields into receipt profile 0.1 so a
reopened R3 receipt remains byte-identical. A focused fake-authority test adds
later manifest fields and verifies retained receipt replay remains exactly
equal; the R5 worker preflight also compares the projection against the actual
retained R3 snapshot string. The R4 attempt remains preserved.

The exact R5 `effectFreeArgv` passed after the receipt projection and both
prior-attempt bindings were in place. The result confirms
`receiptProjectionExact: true`, `nativeCall: false`, `outputCreated: false`,
and that the planned R5 attempt remains absent. The retained SQLite prefix and
six-role heads are unchanged. Its complete output is recorded in
[`r5-effect-free-preflight-result.json`](r5-effect-free-preflight-result.json).
The R5 continuation then completed successfully. Its retained result, final
manifest vector, bounded receipt copies, journal counts, and receipt hashes are
documented in [`r5-completion.md`](r5-completion.md) and
[`r5-completion-receipt-index.json`](r5-completion-receipt-index.json).
