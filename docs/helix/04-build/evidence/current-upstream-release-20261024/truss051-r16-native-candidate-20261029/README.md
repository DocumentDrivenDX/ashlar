---
ddx:
  id: ashlar.evidence.truss051-r16-native-candidate-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss051-r16-effectfree-candidate-20261029
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# R16 .51 native-trial lifecycle candidate

**Disposition: the exact R16 approval was consumed by one prestart attempt, which refused before PostgreSQL startup.** The [failure evidence](r16-failure-evidence.json) binds the approval and supervisor receipts, preserved discrepancy and restored cold backup, and the absence of a start-intent marker. The original [scope](full-lifecycle-scope-proposal-r16.json) and [approval](approval-r16-exact-one-run.json) are historical and cannot authorize another attempt. The corrected process-inventory worker in this directory has different bytes and remains unrun. [run_supervisor_r16.py](run_supervisor_r16.py) admits exact scope and approval bytes, then launches the pinned Python and worker in an isolated process group. It polls the 600-second deadline, process-group RSS, attempt-tree bytes, and combined logs; it sends SIGTERM followed by SIGKILL, waits for the child, and checks for surviving group members. A durable `postgres-start-intent.json` would be written immediately before `pg_ctl start`, allowing the parent to record unknown native state and attempt cleanup only against the exact restored PGDATA if it terminates the worker. [r16_pre_migration_custody_worker.py](r16_pre_migration_custody_worker.py) owns preservation, restore, startup, and fresh public `.48` source/page custody. [r16_post_migration_admission.py](r16_post_migration_admission.py) composes its live callback with the exact `.48` → `.49` → `.51` migration/readback and planned genesis publication/protected ACK, approved seed, actual-page admission, continuation/protected ACK, then returns control for the prefix's single owned stop. This remains a local qualification candidate, not a production-support claim.

The selected candidate is specifically a macOS arm64 / CPython 3.12.14 / Homebrew PostgreSQL 18.6 local qualification trial. The `.51` wheel, source archive, and migration route are frozen in the adjacent [effect-free R16 packet](../truss051-r16-effectfree-candidate-20261029/README.md). They do not claim production support or qualify populated migration behavior on this tuple.

## API feasibility

Import-only inspection in the pinned `.51` runtime confirms the required public call shapes are available:

| Stage API | Signature |
| --- | --- |
| Migration `prepare` | `(self, *, attempt_id=None)` |
| Migration `apply` | `(self, reference)` |
| Migration `reconcile` | `(self, reference)` |
| Local publication | `publish_graph_pages(spark, output, *, connection_factory, consumer_id, feed, maximum_pages=8, wait_seconds=0, archived_acceptance_files=(), export_graph=False, destination_id=None, maximum_page_bytes=1048576, carrier_profile='ashlar-delta/0.3')` |
| Seed group | `run_create_truss_record_group_command(configuration, output, *, require_fresh=False)` |
| Continuation | `run_continue_truss_graph_command(configuration, output, *, maximum_pages=1, wait_seconds=0, maximum_page_bytes=1048576)` |
| `.51` feed reader | `read_next_transaction(assembly, transaction, source_identity, *, ok_type=None, page_type=None)` |

The public `.51` package also exposes `prepare_acceptance(documents, configuration=None, *, bun_executable='bun')` and `CatalogReferenceAssembly.accept_in_transaction(..., request_key=..., origin_bytes=..., expected_revision=..., expected_documents=...)`. No PostgreSQL connection was opened to inspect these signatures. Ashlar's local publisher uses `TrussFeedSessions` and `acknowledge_feed_with_receipt_in_transaction`; the separate managed `.51` ACK interval stub does not prevent the local route.

## Why the candidate is not executable yet

The selected `.48` → `.51` lifecycle still depends on a new distinct PGDATA restoration, preserving the anomalous 11-file selected tree, starting PostgreSQL once, and freshly comparing accepted source/view/feed/consumer/checkpoint custody before migration. The composed worker requires exact live receipts and archive bytes, checks the public `.51` source/view/page after migration, and uses the captured accepted source bytes for seed input. The historical origin file is absent; the owner approved the proposed local-trial origin meaning for the spent R16 attempt, but it is not recovered historical or production authority. The separate gate deliberately reports `review_required: true` and `execution_authorized: false`: that gate checks the map's bytes and meaning; the exact full-scope owner approval supplies execution authority. A future attempt needs a new reviewed scope and approval.

A new `.51` acceptance could call the public acceptance API, but would create a newly registered source and feed context. It cannot silently stand in for the selected source/consumer/checkpoint, the unchanged seed request, or their custody. To qualify that as a separate first-release trial would require a new semantic scope, exact real source bytes, fresh source/feed/consumer identities and a new approval. A development fixture is not an acceptable substitute.

The R15 and R16 approvals are both consumed. The R16 scope is bound to the original worker digest and its output paths contain the spent attempt. Before any new execution, an independent reviewer and owner must approve fresh paths and a new exact scope binding the corrected worker, source/runtime/state/destination inputs and local-trial origin meaning. The spent attempt did not start PostgreSQL, run the migration, publish Delta, seed Truss, or ACK.

The worker checks the frozen archive, source manifest, helper, `.48` runtime receipt and `pg_ctl`; requires macOS arm64 / CPython 3.12.14; inventories and hashes both PGDATA inputs before creating output; enforces one attempt, no retries, fresh disjoint output paths and socket, a disk reserve, and pre-copy tree/aggregate size ceilings; preserves the discrepancy tree; checks the pre-start process inventory; verifies the running server's exact `data_directory`; compares source and complete-view custody against packet-fixed identities and hashes; then binds the actual genesis page archive and checkpoint before stopping. A process-level 600-second alarm bounds the worker and each helper output is capped at 2 MiB. The 16 MiB PostgreSQL-log limit is checked after stopping; it is a post-run gate, not continuous log throttling. It creates a pre-migration registry-absence receipt, but does not create the registry.

After attempt creation, the prefix records the last effect phase and whether effects may remain, and records native effects as possible once PostgreSQL startup is attempted. It preserves stop failures, PID-file uncertainty, command diagnostics, and any callback error; scope admission failures can occur before an attempt receipt exists. A non-zero or timed-out start enters owned-target cleanup when its PID file binds to the fresh PGDATA. The parent supervisor enforces deadline, attempt/log bytes, and process-group RSS during execution, then records child status, remaining process-group members, and any exact-PGDATA cleanup result. Focused tests use temporary fake PGDATA and mocked process/server/helper boundaries; they check durable pre-effect and truthful final receipts, failure uncertainty, exact frozen pins and cursor direction, scope rejection, mocked one-start callback order, and timeout termination/reaping of a harmless child process. These are not PostgreSQL/Spark integration tests and do not verify populated migration or publication behavior.

## Checks

The pinned `.51` Python 3.12 environment imported the APIs above and the public migration route planner. The declared route was exactly `c4339ac7…b6f0 → 35fd797b…d59f → 288801f7…f3a4`. The frozen executable source archive was hash-checked (`cb57fdb7…14a0b`), and the retained extracted source root `/private/tmp/ashlar-r16-source-48fb9cb0-check/src` passed all 569 selected-file hashes against its frozen manifest. The R16 attempt preserved the 11-file selected tree and restored the 1,248-file cold backup, then refused at prestart process inventory after 305.595 seconds. The matching process command was not retained; a concurrent read-only inspection is a plausible cause, not a proven one. The corrected parser distinguishes actual PostgreSQL/`pg_ctl` ownership from unrelated path mentions and stores only a sanitized owner summary and full-inventory digest. Thirty-nine focused inert tests pass in the pinned runtime, and a fresh read-only macOS process snapshot parsed with zero target owners. No PostgreSQL, migration, Spark, publication, ACK, seed, or continuation operation ran.
