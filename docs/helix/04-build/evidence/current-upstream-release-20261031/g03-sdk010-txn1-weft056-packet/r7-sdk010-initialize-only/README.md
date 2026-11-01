---
ddx:
  id: ashlar.evidence.g03-sdk010-init-r7-sparse-source-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.g03-sdk010-txn1-weft056-packet-20261031
      kind: informed_by
    - id: ashlar.evidence.g03-sdk010-init-r6-reconciliation-20261101
      kind: informed_by
    - id: ashlar.evidence.g03-sdk010-init-r5-import-fix-20261101
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

# R7 SDK010 fresh-install candidate

Status: **review candidate; no R7 attempt has been prepared, no permit exists, and no native action has run.** R6 remains `uncertain-after-dispatch`; its supervisor receipts are preserved and its pre-Spark evidence is reconciled in [R6 attempt reconciliation](r6-attempt-reconciliation.md). R5's parse failure and unchanged receipts remain in [R5 reconciliation](r5-attempt-reconciliation.md).

R6 refused in legacy UMF source custody because repeated clean `git status` checks on the full 60,399-path pinned producer checkout exceeded the 30-second subprocess bound. R7 uses an exact-commit sparse checkout with only `native`, `package.json`, `spec`, `src`, and `tsconfig.json`, sparse index enabled, and the existing ignored dependency tree linked from its pinned checkout. This retains the full commit, exact required import files, and enforced clean-tree checks. The sparse checkout's sanitized clean status measured 0.15 seconds. The exact checkout identity, sparse path set, index mode, dependency-link target, and empty status are checked before importing Ashlar code.

An isolated Python `-I -B` effect-free configured-source open, metadata read, and close passed in 20.793 seconds. It used SDK 0.10.0, admitted three batches, and returned the same deterministic SDK receipt as the frozen inputs. The retained evidence and hashes are in the R6 reconciliation note. This preflight had no Spark startup, installation, table, Delta, publication, or ACK effects.

The packet binds the sparse UMF source path and requires the full configured-source admission through the exact generated invocation in an isolated worker before `prepare` completes. At dispatch, it rechecks source/runtime/packet custody and takes a fresh process-slot receipt; the native worker then runs the runtime's actual configured-source admission before Spark startup. The effect-free preflight verifies Ashlar `src` admission before repository imports, parses the exact invocation with `parse_configured_local`, requires the sparse UMF source path, and confirms no Spark startup. Publication, query, ACK, and transaction 1 remain outside this packet.

R7 retains the prior exact SDK010 package/archive hashes, Delta 4.4.1 jars, Spark 4.2.0, Python 3.12.14, Java 21, 512 MiB driver, 2 GiB process-group RSS cap, one 300-second child, one attempt, and zero retries. Its attempt parent is the new `/private/tmp/ashlar-g03-sdk010-delta4-init-r7-attempts-20261101`; R3/R4/R5/R6 directories are ineligible and untouched. The packet has not run `prepare`, `slot`, or `execute`; it awaits independent review and a new permit. Transaction 1 remains blocked on a successful supervisor-closed six-role installation receipt.

Focused inert checks:

```sh
cd docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r7-sdk010-initialize-only
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python3.12 -B -m unittest -v \
  test_r7_init_packet.InitPacketTests.test_sparse_umf_checkout_has_exact_commit_clean_status_and_dependency_link \
  test_r7_init_packet.InitPacketTests.test_real_generated_invocation_parses_in_isolated_worker_child
```
