---
ddx:
  id: ashlar.evidence.g03-sdk010-txn1-weft056-packet-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk058-txn1-timeout-r3-20261030
      kind: informed_by
    - id: ashlar.evidence.g03-sdk010-source-admission-candidate-20261031
      kind: informed_by
    - id: ashlar.evidence.g03-weft056-delta4-consumer-candidate-20261029
      kind: informed_by
    - id: ashlar.evidence.sdk010-delta4-runtime-integration-20261031
      kind: informed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: TD-009
      kind: governed_by
---

# G03 SDK 0.10.0 transaction 1 and Weft 0.4.56 packet candidate

Status: **R7's one-shot initializer ran and created the six local `.4` tables; its worker then failed in a post-initialize receipt predicate**. The original supervisor status remains `uncertain-after-dispatch`. A read-only verifier checked the exact runtime report, installation/carrier/reservation receipts, process-group closure, and all six Delta log heads; the independent [R7 reconciliation](r7-init-reconciliation/README.md) provides a usable initialization prerequisite receipt for a future reviewed transaction-one gate. The frozen [R7 packet](r7-sdk010-initialize-only/README.md) is retained unchanged. R6's earlier pre-Spark source-custody failure and original uncertain status remain in [R6 reconciliation](r7-sdk010-initialize-only/r6-attempt-reconciliation.md). R3's SDK058 `.2` installation remains ineligible. No transaction, publication, Weft compilation/query, or ACK was run.

The SDK 0.10.0 source-admission change is present as commit `fe0ae9e0`
(SDK010 source admission). The attempt must bind the admitted source profile to
the already initialized six-role `.4` installation and then reopen the actual
publication and complete vector. The source change does not itself establish
transaction-1 or consumer success.

## R4 timing instrumentation

[`r4_timing.py`](r4_timing.py) supplies an attempt-local, 8 KiB / 32-event
monotonic timing receipt and an installer for the existing SDK010 transaction
boundaries. It measures SDK010 source opening, metadata reservation,
prepared/applying phase total, each metadata mutation or recovery, and named
metadata commit observation. It records only closed stage names, start offsets,
durations, and success/error; it retains no model, request, SQL, or source
payload. The method wrappers preserve return values and exceptions and can be
restored in `finally`. It modifies no Ashlar source. An R4 worker would install
these hooks after importing the pinned runtime modules, run the already admitted
transaction, restore the hooks, and retain the timing bytes inside its attempt
receipt even on failure.

The instrumentation distinguishes a slow reservation call from a slow native
metadata submission/observation, which r3 could not. It does not infer a stall
cause and does not change deadlines, retry behavior, or operation order. The
focused inert suite is:

```sh
PYTHONPATH=src:docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet \
  python3 -m unittest discover \
  -s docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet \
  -p 'test_r4_timing.py'
```

It passed 3 tests. The suite uses fake source/coordinator/transport classes and a
controlled monotonic clock; it exercises no Spark or Delta code.

## Effect-free R4 preflight result

The earlier `r4_preflight.py` exercised the exact SDK010 source route against the frozen G03
configuration and request. It verified source commit `fe0ae9e0` is an ancestor
of checkout `e9591b4ff89aba9ab10bb4cae8c5b7aea24e9b0b`, with `src` matching that
source commit; configuration SHA-256
`040545b8cc96701a158b8936750c89f97ce991df9652ceeacf6c66fb9d9fc168`; model
SHA-256 `19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b`;
request SHA-256 `cd1bd2654c8fcd2ecd1246e0be44b8f94026faa2ef58cf75d7293f8652c990e7`;
and SDK checker receipt SHA-256
`20ccae34ef3ee1a4d95a5c487492941e0afaa3eeefa7fdb2467efaa2efb31e69`. It also
verified SDK archive SHA-256
`2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c`, public
entry SHA-256
`b32bce2fdcf928bc5b9fd8376b4c7cab9beb1b63d90cb1d20b9ad0ec94595951`, and the
Python 3.12.14 / Spark 4.2.0 / Delta 4.4.1 / MiniRacer 0.14.1 versions and
Delta jars. This is effect-free source/runtime evidence only.

The retained r3 installation has carrier profile `.2`, SDK058 source revision
`umf-sdk-0.5.8/071544fcc83c5abcf1e280890e1516613f679a58cf245b31a7bf16f0b6dd24fd`,
physical `.4-xxhash64` and installation digest
`735eba9e3348381dbcb3c53271216de100070a1bcbf5ce4d62927c0e9f5ce686`. It is
not eligible for the SDK010 `.3` route and its attempt journal is uncertain.
At that earlier preflight, the proposed transaction-one R4 attempt parent
`/private/tmp/ashlar-g03-sdk010-delta4-txn1-r4-attempts-20261031` was absent.
The process-inventory command failed with `Operation not permitted` under that
sandbox, so that preflight produced no process-slot receipt. A later read-only
inventory under the permitted process view found no Java process at its capture
time. The separate init-only R4 attempt and empty slot receipt are recorded
below; its slot must be refreshed immediately before dispatch.

These facts hold transaction 1: a transaction worker cannot reopen or invoke
the existing transaction-1 route until a fresh, separately admitted SDK010 `.3`
six-role installation receipt and a permitted immediate process-slot inventory
exist. Creating that installation requires the separately reviewed native
initializer below. The r3 installation cannot serve as a substitute. The current
Weft consumer candidate also still lacks the final `.4`-bound original Weft
0.4.56 compiler artifacts and publication vector.

## R4 init-only candidate

The adjacent `r4-sdk010-initialize-only/` directory contains the bounded
initializer. It freezes the five G03 source inputs, SDK010 archive/public-entry
pins, exact Python/Spark/Delta/Java/JAR versions and hashes, and the existing
producer checkout. Its invocation is constrained to
`ashlar-configured-local/0.3`, `initialize`, carrier `.4-xxhash64`, one Spark
session, `local[1]`, 512 MiB driver memory, a 300-second wall deadline, one
attempt, no retry, 512 MiB attempt storage, and 1 MiB combined logs/receipt.
Output is confined to the absent, private
`/private/tmp/ashlar-g03-sdk010-delta4-init-r4-attempts-20261031` root. The
worker checks for six unique role targets and the exact SDK010 `.3` source
policy; it does not enter transaction 1 or consumer stages. Source admission is
bound to the exact Git tree object for `src` at the SDK010 source commit; later
documentation commits are allowed while `src` remains byte-identical. The
current checkout commit is recorded for custody, not pinned as the admission
criterion.

Once independently reviewed and separately permitted, the packet entrypoints
are `r4_packet.py prepare`, `r4_packet.py slot ATTEMPT_ROOT`, and
`r4_packet.py execute ATTEMPT_ROOT PERMIT_JSON`. Only `prepare` is effect-free
and creates a private attempt. `slot` must capture an empty Java process
inventory; `execute` binds that receipt, prepared bytes, manifest, packet digest
and reviewed permit, then captures and retains a second immediate inventory
before dispatch. It rechecks source/runtime admission and refuses a changed or
occupied slot. The monitor retains the attempt on timeout, nonzero exit, or
storage/log overflow and never retries. It caps aggregate resident memory for
the owned child process group at 2 GiB, records cleanup, and checks for and
terminates surviving group members after normal or failed child exit. A refusal
before `dispatch.json` is recorded as `refused-before-dispatch`; any failure
after dispatch is conservatively recorded as `uncertain-after-dispatch`.
Source tree identity and cleanliness are verified before importing repository
modules. If process inventory fails during cleanup, the worker still signals
the owned group and records closure as unverified; it cannot claim success.
Sol's reported read-only inventory
found no Java process at its capture time; that observation is not the packet's
fresh receipt and must be repeated at dispatch.

The R4 one-shot permit was consumed. Sol reviewed the source-tree gate,
process-group cleanup, exact packet/manifest binding, and refused/uncertain
outcomes. The owner authorized R4 after its prepared attempt and fresh
process-slot receipt were bound. That dispatch failed during isolated worker
module import before initialization outputs were created. The local pinned
Python 3.12 environment ran `test_r4_init_packet.py` (7 inert tests) and
`test_r4_timing.py` (3 inert tests); both pass. Bytecode compilation also
passes. These checks do not prove initialization behavior. The actual fresh
six-role registry, runtime report, and initialization timing receipt can only
exist after the permitted native init. Without those runtime receipts, the
transaction-one and Weft consumer packet cannot be finalized or run.

The effect-free `prepare` step created the private attempt
`/private/tmp/ashlar-g03-sdk010-delta4-init-r4-attempts-20261031/ashlar-g03-sdk010-init-5b8b61df4b044481afb9f6fc157720a2`.
Its exact [prepared receipt](r4-init-prepared-receipt.json) has SHA-256
`a9b2481b6a8d7ec73fe3a9875bc429345d1eb6c7c3dfffd7b10b8207bceb7303`,
and its reviewed [empty process-slot receipt](r4-init-process-slot-receipt.json)
has SHA-256 `c8d6f22b8175921caf39029b0e0060d28e5849f310f9ca2683fb2adcb43e9a90`.
The selected packet tree digest is
`a6e18fb93b2f69d4ed305b4c5be93f9c5966758f26fadf49f5c912da272c2b43`.
R4 consumed one dispatch and the isolated child exited before it could import
the sibling packet module. Its worker traceback is at line 17; no installation,
initialize output directory, init result, or Spark log exists. The supervisor
retained `uncertain-after-dispatch` while recording verified empty process-group
cleanup. The bounded read-only reconciliation supports a pre-native-effect
conclusion from the traceback, empty worker stdout, absent native outputs, and
empty group, without changing that conservative status. Exact receipt and log
hashes are retained in [R4 failure reconciliation](r5-sdk010-initialize-only/r4-attempt-reconciliation.md).

The [R5 init-only candidate](r5-sdk010-initialize-only/README.md) fixed the isolated-child import path and bound a fresh attempt parent. Its approved one-shot was consumed and failed before the initializer: the R5 producer map used camelCase keys where the closed parser requires snake_case. The supervisor retained `uncertain-after-dispatch`; read-only evidence supports pre-native refusal, and R5's original status remains unchanged. Its prepared and empty process-slot receipts are retained as [copies](r5-init-prepared-receipt.json) and [process-slot evidence](r5-init-process-slot-receipt.json). Exact failure hashes and no-effect evidence are in [R5 reconciliation](r6-sdk010-initialize-only/r5-attempt-reconciliation.md).

The [R6 init-only candidate](r6-sdk010-initialize-only/README.md) mapped the producer schema explicitly and passed an isolated-child parse of its generated invocation. Its approved one-shot was consumed; source admission then timed out during full-tree UMF Git custody before Spark startup. The separate [R6 reconciliation](r7-sdk010-initialize-only/r6-attempt-reconciliation.md) preserves its hashes and conservative status. [R7](r7-sdk010-initialize-only/README.md) bound a sparse checkout of the same UMF commit and required one full configured-source open/close preflight before preparation. Its attempt created the six tables and failed only after runtime initialization when the worker compared the carrier adapter-policy field to the distinct UMF policy revision. The independent [R7 receipt](r7-init-reconciliation/r7-init-reconciliation.json) verifies the six heads while preserving the original uncertain execution status.

The effect-free [R4 transaction-one release gate](r4-transaction1-gate/README.md)
binds the consumed R4 initializer and Weft 0.4.56 wheel; it remains dispatch-disabled
and does not consume the independent R7 reconciliation. A reviewed gate update must bind that receipt before any transaction-one dispatch.
The null publication vector is retained; exact `.4`-bound Weft requests and
responses remain downstream consumer evidence. No transaction-one dispatch is
authorized by that gate.

## r3 disposition and timeout finding

The consumed r3 attempt is
`ashlar-g03-txn1-f66791eded7f449eb4b21ef60cd50bca`, retained at
`/private/tmp/ashlar-g03-sdk058-delta4-txn1-attempts-20261030/ashlar-g03-txn1-f66791eded7f449eb4b21ef60cd50bca`.
Its packet, permit, and prepared-input SHA-256 values are recorded in the linked
r3 evidence. Durable state shows `prepared` and `applying`, one local plan, two
metadata reservation rows, no `local_operation`, and no
`local_delta4_metadata_operation` table. Six Delta logs stop at version 1 with
metadata actions only. The latest durable boundary is after reservation and
before the first metadata operation. This is an uncertain/partial attempt and is
not eligible for rerun or reuse as a fresh installation.

At 12:48:26 local time, about ten seconds before the supervisor deadline, the
SDK058 checker left its temporary model and request files. Those files show a
checker invocation was underway; no retained counter or trace establishes its
caller or total delay. Three current effect-free SDK058 checker measurements
were 2.871s, 2.474s, and 2.517s with identical receipt bytes and digests.
Those measurements do not reconstruct r3. Repeated checking remains a plausible
contributor, not a proven cause. This packet therefore records stage timings and
monotonic durations around existing phase boundaries rather than changing the
source-admission semantics or adding retries.

## Frozen source and consumer scope

The candidate source is the exact G03 typed-graph configuration, model, source,
binding, and `typed-graph-1` transaction from the linked Weft consumer candidate.
That transaction contains eight events: four objects and four directed edges,
including the isolated `person-3` and two independently identified parallel
`knows` edges. Transactions 2 and 3 remain out of scope. The linked source
oracle is authoritative for complete bags, exact values, and page expectations.
The existing transaction runtime owns the three graph operations and its
completion receipt; this packet does not alter or reconstruct its operation plan.

After transaction success, a separate read-only consumer stage must bind the
freshly realized four-role UUID/version vector and its canonical digest. Its
five reads are: native singleton; original Backend03 grouped query; positive
Backend03 multiplicity query; Backend02 first page; and Backend02 continuation.
The continuation request is derived from the returned page-one token, frozen,
and reviewed before page two. Both stages bind the same vector and runtime
identity. Original request/response bytes and emitted SQL are retained unchanged.
Weft compiles; Ashlar's adapter executes its emitted SQL. No `.3`, Weft `.53`,
or stale `.4` compiler artifact may be substituted.

The direct singleton oracle preserves the exact integer token
`9007199254740993` as quoted JSON text and the unknown retained JSON text. The
page oracle preserves the Unicode name `雪`, two-row page size, and continuation
after `person-2`. The grouped oracle is `[["person-1",2,1]]`. These values are
assertions from the source oracle, not values to derive from native results.

## Bounded attempt envelope

Use one private local Spark 4.2.0 / Delta 4.4.1 process on the selected macOS
arm64 runtime, with SDK 0.10.0 and Weft 0.4.56 pinned by exact installed package
and executable identities. The existing one-shot envelope is 450 seconds total,
512 MiB attempt storage, 16 MiB retained evidence, 1 MiB combined process output,
1 MiB per compiler/result artifact, 10,000 native rows per result, and 65,536
cells per result. Run transaction 1 and consumer reads under one bounded owner
interval only if the runtime can retain all closing checks within this envelope.
Otherwise stop before native dispatch and prepare a separately reviewed split
packet; do not silently extend the deadline.

A packet-specific effect-free preparation must freeze and hash the exact launch
descriptor, transaction request, source/model/config/binding custody, runtime
inventory, original Weft requests/oracles, and null publication-vector template.
The null template stays immutable. A realized vector is created only from a
successful native publication receipt plus independently observed Delta UUIDs and
versions; it includes a canonical digest. Query compilation and all execution
bind that digest. Never synthesize UUIDs or versions from table heads or reuse the
`.3` R02 vector.

## Instrumentation and stop behavior

The next packet revision should record monotonic start/end timestamps and elapsed
milliseconds for: owner/runtime open; SDK source admission; journal prepare and
`prepared` commit/readback; reservation acquisition; `applying` commit/readback;
metadata-operation dispatch, named-commit observation and full readback; each of
the three graph operations; complete bag/hash comparison; local completion
receipt; runtime close; and each native closing snapshot. Record a stage only
when the existing boundary is observed. Keep telemetry in the private attempt
receipt, cap its size, and exclude source values, credentials, and unrestricted
SQL text. Include operation IDs, predecessor/observed versions, outcome class,
and monotonic duration so a stalled boundary can be localized without guessing.

At the fixed deadline, stop dispatching new work, preserve journal and all six
Delta logs, and classify any submitted but unverified operation as uncertain.
Do not retry, overwrite the attempt, delete evidence, or produce success output
for an uncertain phase. A new run requires a fresh, independently admitted
six-role installation with verified empty heads, a new output root, a fresh
packet digest and permit, and explicit reconciliation that the r3 retained
namespace is not being reused. A pre-dispatch failure may be corrected in the
candidate and re-reviewed; any dispatched or uncertain native operation closes
that attempt permanently pending separate reconciliation.

The transaction result is not a publication. Consumer reads require an immutable
manifest and complete four-role vector, current source and publication custody,
independent complete-bag comparisons, and successful opening and closing checks.
No source acknowledgement or protected ACK is in this scope. A failed close,
vector drift, partial capture, timeout, or result mismatch releases no consumer
result and leaves evidence for reconciliation.

## Review gates still open

Before an execution permit can be considered, a worker must integrate the
instrumentation above and freeze exact hashes for the supervisor/worker,
prepared manifest, launch descriptor, realized runtime inventory, actual empty
process-slot receipt, transaction source and request, independent source oracle,
and immutable vector template. The
post-publication consumer artifacts must then be compiled from original requests
with Weft 0.4.56 and bound to the realized vector. Sol's independent review and
a fresh one-shot permit over those exact bytes remain required. This draft grants
neither.

The separate SDK010 admission/source-routing/runtime and `.4` transaction,
coordinator, and Weft consumer adapter tests remain the existing focused checks
for the underlying route, as identified in the linked candidate receipts. Those
checks and this instrumentation suite establish only pure admission and mocked
control flow. The R7 six-role installation is independently reconciled, but no
transaction-one completion, publication, Weft execution, or timeout performance
evidence exists. No additional stress or repeated initialization is proposed.
