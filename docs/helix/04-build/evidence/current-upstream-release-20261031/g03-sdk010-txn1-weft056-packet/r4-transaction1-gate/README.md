---
ddx:
  id: ashlar.evidence.g03-sdk010-txn1-effect-free-gate-r4-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk010-txn1-weft056-packet-20261031
      kind: informed_by
    - id: ashlar.evidence.g03-weft056-delta4-consumer-candidate-20261029
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

# R4 SDK010 transaction-one release gate

Status: **effect-free packet candidate; transaction-one native dispatch is not
authorized**. This packet binds the current SDK010 source admission route,
the exact R4 initialization attempt and prepared receipt, the Weft 0.4.56 wheel
identity, the frozen G03 source/oracle, and the immutable null publication
vector template. It does not create a publication vector, invoke a Spark
session, or execute transaction 1.

The exact initialization attempt is
`/private/tmp/ashlar-g03-sdk010-delta4-init-r4-attempts-20261031/ashlar-g03-sdk010-init-5b8b61df4b044481afb9f6fc157720a2`.
Its prepared receipt and empty preflight process-slot receipt are copied and
hash-bound at the parent packet. The R4 `init-result.json` is not present. The
gate requires that result, with the SDK010 six-role receipt profile, exact
`.3` source-policy revision, and six unique role UUIDs, before reporting the
transaction-one prerequisites satisfied. It also requires the supervisor
`execution-status.json` to report the exact status profile, `completed`, an
empty process group, and the matching result digest. It cross-checks the
original installation registry, carrier profile, runtime report digests,
profiles, and exact six role names. A worker result alone cannot satisfy the
gate. The retained R3 SDK058 `.2` installation is not an input.

The focused effect-free command is:

```sh
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python \
  docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r4-transaction1-gate/r4_txn1_gate.py inspect
```

It verifies the committed and working `src` tree before importing repository
code, runs the existing SDK010 source/runtime preflight, checks the exact R4
prepared receipts, and binds the installed Weft 0.4.56 wheel through both its
wheel digest and installed `direct_url.json`. It emits an effect-free report;
the current expected gate is blocked because the native initialization result
does not yet exist.

Weft identity is pinned to source commit
`2f62e1148d1b2030558550e4f6f2ffe518bac62d`, wheel SHA-256
`68f9297b4b55df483b86d3041040991c4dadfb9b4d63afcff1420c43f93ae41d`, and the
installed direct-url record digest in `packet-manifest.json`. Original
`.4`-bound compiler requests and responses are still absent. The only retained
positive Backend03 request is a `.3` compiler-only candidate and is preserved
as evidence of that limitation; it is not admitted as a `.4` target. The null
publication template remains byte-identical. Since transaction 1 produces no
publication manifest, no realized publication vector can be asserted yet; the
vector and exact `.4` compiler responses remain downstream consumer gates.

The timing contract is in `stage-timing-contract.json`. The existing R4 helper
can measure source admission, total transaction call, reservation, phase
preparation, metadata mutation/recovery, and commit observation. Individual
graph operations, readback splits, bag comparison, close, and closing-snapshot
timings remain unobserved. No timing values or native transaction result are
claimed in this candidate. Its one-shot uncertainty rule is explicit: before
dispatch is a refusal with no effect claim; any post-dispatch failure without
verified completion is uncertain and closes the attempt to retry. A successful
result would still require the existing six-role head vector, bag/hash check,
completion receipt, runtime close, and closing snapshot.

Focused inert checks:

```sh
cd docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r4-transaction1-gate
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python -B -m unittest -v test_r4_txn1_gate
```

The gate has no `prepare` or `execute` command. A native transaction requires a
separately reviewed worker, an actual R4 initialization receipt, a fresh
process inventory at dispatch, and a new exact one-shot permit. This candidate
provides none of those permissions and makes no claim about Weft consumer
execution, publication, or ACK.
