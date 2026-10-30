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

# R16 .51 native-trial feasibility and custody prefix

**Disposition: a guarded pre-migration custody prefix is implemented, but not authorized or run.** [r16_pre_migration_custody_worker.py](r16_pre_migration_custody_worker.py) can preserve the observed anomalous PGDATA tree, copy the frozen stopped cold backup to a distinct PGDATA, start that copy, run the public `.48` accepted-source custody and pending genesis-page reads, and stop the owned server. It intentionally ends before any migration, publication, ACK, seed, or continuation effect. It is a local qualification-run candidate, not a production-support claim.

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

## Why neither path is executable yet

The selected `.48` → `.51` lifecycle still depends on restoring the retained cold backup to distinct fresh PGDATA, preserving the anomalous 11-file selected tree, starting PostgreSQL once, and freshly comparing accepted source/view/feed/consumer/checkpoint custody before migration. The prefix implements those steps through the public `.48` reader and stops on any discrepancy. The historical origin file is absent; the proposed origin-map digest is new local-trial semantics, not recovered history. No execution approval exists, so the code remains inert in this workspace.

A new `.51` acceptance could call the public acceptance API, but would create a newly registered source and feed context. It cannot silently stand in for the selected source/consumer/checkpoint, the unchanged seed request, or their custody. To qualify that as a separate first-release trial would require a new semantic scope, exact real source bytes, fresh source/feed/consumer identities and a new approval. A development fixture is not an acceptable substitute.

The existing future-scope template has no one-run authorization, fresh copy/output paths, connection-factory hash, registry backup identity, or reviewed worker/supervisor hashes. It does not satisfy this prefix worker's distinct scope profile. No new prefix scope or approval has been created. The R15 approval is consumed and grants no authority. Before any execution, an independent reviewer and owner would need to approve a new one-run scope binding the exact worker hash and all source/runtime/state inputs. The worker has not been run against PostgreSQL or the selected PGDATA.

The worker checks the frozen archive, source manifest, helper, `.48` runtime receipt and `pg_ctl`; requires macOS arm64 / CPython 3.12.14; inventories and hashes both PGDATA inputs before creating output; enforces one attempt, no retries, fresh disjoint output paths and socket, a disk reserve, and pre-copy tree/aggregate size ceilings; preserves the discrepancy tree; checks the pre-start process inventory; verifies the running server's exact `data_directory`; compares source and complete-view custody against packet-fixed identities and hashes; then binds the actual genesis page archive and checkpoint before stopping. A process-level 600-second alarm bounds the worker and each helper output is capped at 2 MiB. The 16 MiB PostgreSQL-log limit is checked after stopping; it is a post-run gate, not continuous log throttling. It creates a pre-migration registry-absence receipt, but does not create the registry.

The worker reports the last effect phase and whether effects may remain on every error; it reports native effects as possible/occurred once PostgreSQL start is attempted, and preserves stop failures, PID-file uncertainty, command diagnostics, and any prior stage error. A non-zero or timed-out start enters owned-target cleanup when its PID file binds to the fresh PGDATA. The focused tests use temporary fake PGDATA and mocked process/server/helper boundaries. They verify null-scope rejection, source/output path overlap rejection, exact source-custody pins and socket placement, cold-backup mismatch before output creation, sequential custody stage ordering, no migration invocation, accurate post-start failure classification, cleanup after non-zero start, cleanup on source mismatch, and explicit stop-failure uncertainty. They are not PostgreSQL integration tests.

## Checks

The pinned `.51` Python 3.12 environment imported the APIs above and the public migration route planner. The declared route was exactly `c4339ac7…b6f0 → 35fd797b…d59f → 288801f7…f3a4`; it was metadata-only. The frozen executable source archive was hash-checked (`cb57fdb7…14a0b`) and extracted under `/private/tmp` for import-only inspection. Read-only tree inventories matched packet pins: selected incomplete PGDATA `b979795a…937c75` (11 files, 17,197,691 bytes); cold backup `67f15dff…9effc` (1,248 files, 63,997,052 bytes). Nine focused inert tests pass. No PostgreSQL, migration, Spark, publication, ACK, seed, or continuation operation ran. No commit was made.
