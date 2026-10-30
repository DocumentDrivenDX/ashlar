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

**Disposition: a parent-process one-shot supervisor and callable one-start full-lifecycle candidate are implemented, but neither is approved or run.** [run_supervisor_r16.py](run_supervisor_r16.py) admits the exact scope and approval bytes, then launches the exact pinned Python and worker in an isolated process group. It polls the 600-second deadline, process-group RSS, attempt-tree bytes, and combined logs; it sends SIGTERM followed by SIGKILL, waits for the child, and checks for surviving group members. A durable `postgres-start-intent.json` is written immediately before `pg_ctl start`, allowing the parent to record unknown native state and attempt cleanup only against the exact restored PGDATA if it terminates the worker. [r16_pre_migration_custody_worker.py](r16_pre_migration_custody_worker.py) owns preservation, restore, startup, and fresh public `.48` source/page custody. [r16_post_migration_admission.py](r16_post_migration_admission.py) composes its live callback with the exact `.48` → `.49` → `.51` migration/readback and the planned genesis publication/protected ACK, approved seed, actual-page admission, continuation/protected ACK, then returns control for the prefix's single owned stop. The callback durably records exact pre-migration custody bytes before migration; the worker's final stop receipt distinguishes completed, unknown, and unverified outcomes. The populated [scope proposal](full-lifecycle-scope-proposal-r16.json) binds current worker/runtime/source hashes and fresh disjoint attempt and supervisor-output paths, but is explicitly not an owner approval. Supervisor admission can fail before an attempt receipt exists. This remains a local qualification candidate, not a production-support claim.

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

The selected `.48` → `.51` lifecycle still depends on restoring the retained cold backup to distinct fresh PGDATA, preserving the anomalous 11-file selected tree, starting PostgreSQL once, and freshly comparing accepted source/view/feed/consumer/checkpoint custody before migration. The composed worker requires exact live receipts and archive bytes, checks the public `.51` source/view/page after migration, and uses the captured accepted source bytes for seed input. The historical origin file is absent; the proposed origin-map digest is new local-trial semantics, not recovered history. Its separate gate deliberately reports `review_required: true` and `execution_authorized: false`: that gate checks the proposed map's bytes and meaning; it does not grant execution authority. The exact full-scope owner approval supplies that authority and must bind the gate, map, and complete lifecycle inputs. No execution approval exists, so the code remains unrun in this workspace.

A new `.51` acceptance could call the public acceptance API, but would create a newly registered source and feed context. It cannot silently stand in for the selected source/consumer/checkpoint, the unchanged seed request, or their custody. To qualify that as a separate first-release trial would require a new semantic scope, exact real source bytes, fresh source/feed/consumer identities and a new approval. A development fixture is not an acceptable substitute.

The existing future-scope template has no full-lifecycle authorization, fresh copy/output paths, full worker hashes, or one-run binding across source custody, migration, `.4` destination, origin gate/map, seed request, and continuation. The new scope proposal is populated, but no owner approval has been created. Its `origin_gate` is intentionally a semantics check: `execution_authorized` remains false, and the owner full-scope approval must explicitly approve the proposed `local_truss_acceptance_trial` origin meaning along with every other bound input. The R15 approval is consumed and grants no authority. Before execution, an independent reviewer and owner must approve one new scope and approval binding the exact workers and every source/runtime/state/destination input. The worker has not been run against PostgreSQL or the selected PGDATA.

The worker checks the frozen archive, source manifest, helper, `.48` runtime receipt and `pg_ctl`; requires macOS arm64 / CPython 3.12.14; inventories and hashes both PGDATA inputs before creating output; enforces one attempt, no retries, fresh disjoint output paths and socket, a disk reserve, and pre-copy tree/aggregate size ceilings; preserves the discrepancy tree; checks the pre-start process inventory; verifies the running server's exact `data_directory`; compares source and complete-view custody against packet-fixed identities and hashes; then binds the actual genesis page archive and checkpoint before stopping. A process-level 600-second alarm bounds the worker and each helper output is capped at 2 MiB. The 16 MiB PostgreSQL-log limit is checked after stopping; it is a post-run gate, not continuous log throttling. It creates a pre-migration registry-absence receipt, but does not create the registry.

After attempt creation, the prefix records the last effect phase and whether effects may remain, and records native effects as possible once PostgreSQL startup is attempted. It preserves stop failures, PID-file uncertainty, command diagnostics, and any callback error; scope admission failures can occur before an attempt receipt exists. A non-zero or timed-out start enters owned-target cleanup when its PID file binds to the fresh PGDATA. The parent supervisor enforces deadline, attempt/log bytes, and process-group RSS during execution, then records child status, remaining process-group members, and any exact-PGDATA cleanup result. Focused tests use temporary fake PGDATA and mocked process/server/helper boundaries; they check durable pre-effect and truthful final receipts, failure uncertainty, exact frozen pins and cursor direction, scope rejection, mocked one-start callback order, and timeout termination/reaping of a harmless child process. These are not PostgreSQL/Spark integration tests and do not verify populated migration or publication behavior.

## Checks

The pinned `.51` Python 3.12 environment imported the APIs above and the public migration route planner. The declared route was exactly `c4339ac7…b6f0 → 35fd797b…d59f → 288801f7…f3a4`; it was metadata-only. The frozen executable source archive was hash-checked (`cb57fdb7…14a0b`), and the retained extracted source root `/private/tmp/ashlar-r16-source-48fb9cb0-check/src` passed all 569 selected-file hashes against its frozen manifest. Read-only tree inventories matched packet pins: selected incomplete PGDATA `b979795a…937c75` (11 files, 17,197,691 bytes); cold backup `67f15dff…9effc` (1,248 files, 63,997,052 bytes). Thirty-three focused inert tests pass, and all three worker/supervisor files compile. No PostgreSQL, migration, Spark, publication, ACK, seed, or continuation operation ran.
