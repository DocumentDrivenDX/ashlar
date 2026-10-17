"""Real local lifecycle storage; no Fabric/native/source authority claim."""
from dataclasses import replace
from pathlib import Path
import multiprocessing
import tempfile
import unittest
from uuid import UUID

from ashlar_host.fabric_release_journal import FabricReleaseJournal, JournalRefusal, ReleaseIntent, FabricMutation


def uid(number):
    return str(UUID(int=number))


def intent(number):
    return ReleaseIntent(uid(number), uid(100), str(number) * 64,
                         '{"objects":{"uuid":"original","version":1}}',
                         '{"nodes":"release-specific"}', ((f'onelake/release-{number}/nodes', 'a' * 64),))


def child_mutate(root, sender):
    try:
        FabricReleaseJournal(Path(root)).activate_local(uid(2), 1)
    except JournalRefusal as exc:
        sender.send(str(exc))
    else:
        sender.send('unexpected success')
    finally:
        sender.close()


def child_read(root, sender):
    try:
        with FabricReleaseJournal(Path(root)).reader():
            sender.send('unexpected success')
    except JournalRefusal as exc:
        sender.send(str(exc))
    finally:
        sender.close()


class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve() / 'journal'
        self.owner = FabricReleaseJournal.initialize(self.root)

    def created(self, number):
        self.owner.record_intent(intent(number))
        self.owner.begin_creation(uid(number))
        self.owner.record_created(uid(number), uid(200 + number), f'original-operation-{number}')

    def test_uncertain_creation_survives_reopen_and_never_implicitly_retries(self):
        self.owner.record_intent(intent(1))
        self.owner.begin_creation(uid(1))
        reopened = FabricReleaseJournal(self.root)
        self.assertEqual(reopened.candidate(uid(1)), (intent(1), "unknown", None, None))
        with self.assertRaisesRegex(JournalRefusal, 'already submitted'):
            reopened.begin_creation(uid(1))
        with self.assertRaisesRegex(JournalRefusal, 'created item'):
            reopened.activate_local(uid(1), 0)
        reopened.record_created(uid(1), uid(201), 'original-handle')
        reopened.activate_local(uid(1), 0)
        with reopened.reader() as active:
            self.assertEqual(active.creation_handle, 'original-handle')
            self.assertEqual(active.intent, intent(1))

    def test_interrupted_inactive_candidate_leaves_whole_r1(self):
        self.created(1)
        self.owner.activate_local(uid(1), 0)
        self.owner.record_intent(intent(2))
        self.owner.begin_creation(uid(2))
        with self.owner.reader() as active:
            self.assertEqual((active.revision, active.intent), (1, intent(1)))
        with self.assertRaises(JournalRefusal):
            self.owner.activate_local(uid(2), 1)

    def test_atomic_predecessor_and_reopen_binding(self):
        self.created(1)
        self.created(2)
        self.owner.activate_local(uid(1), 0)
        with self.assertRaisesRegex(JournalRefusal, 'stale'):
            self.owner.activate_local(uid(2), 0)
        self.owner.activate_local(uid(2), 1)
        with FabricReleaseJournal(self.root).reader() as active:
            self.assertEqual((active.revision, active.graph_model_id, active.intent), (2, uid(202), intent(2)))

    def test_real_separate_process_cannot_activate_during_reader(self):
        self.created(1)
        self.created(2)
        self.owner.activate_local(uid(1), 0)
        context = multiprocessing.get_context('spawn')
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(target=child_mutate, args=(str(self.root), sender))
        with self.owner.reader() as active:
            process.start()
            sender.close()
            try:
                self.assertTrue(receiver.poll(10), 'child lease check timed out')
                self.assertEqual(receiver.recv(), 'lease busy')
                process.join(10)
                self.assertEqual(process.exitcode, 0)
                self.assertEqual(active.intent, intent(1))
            finally:
                if process.is_alive():
                    process.kill()
                    process.join()
                receiver.close()
        self.owner.activate_local(uid(2), 1)

    def test_exception_releases_real_lease(self):
        self.created(1)
        self.owner.activate_local(uid(1), 0)
        with self.assertRaises(KeyboardInterrupt):
            with self.owner.reader():
                raise KeyboardInterrupt()
        self.created(2)
        self.owner.activate_local(uid(2), 1)

    def test_fresh_item_and_locations_cannot_be_reused(self):
        self.created(1)
        with self.assertRaisesRegex(JournalRefusal, 'reserved'):
            self.owner.record_intent(replace(intent(2), sources=intent(1).sources))
        self.owner.record_intent(intent(2))
        self.owner.begin_creation(uid(2))
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            self.owner.record_created(uid(2), uid(201), 'another-handle')
        # Failed write rolls back and does not turn unknown into created.
        with self.assertRaises(JournalRefusal):
            self.owner.activate_local(uid(2), 0)

    def test_private_paths_and_replacement_refuse(self):
        self.root.chmod(0o755)
        with self.assertRaisesRegex(JournalRefusal, 'private'):
            FabricReleaseJournal(self.root)
        self.root.chmod(0o700)
        with self.assertRaises(FileExistsError):
            FabricReleaseJournal.initialize(self.root)
        link = self.root / 'lease'
        link.unlink()
        link.symlink_to(self.root / 'journal.sqlite')
        with self.assertRaises(JournalRefusal):
            self.owner.begin_creation(uid(1))

    def test_uri_special_characters_select_only_exact_journal(self):
        root = Path(self.tmp.name).resolve() / 'private?#journal'
        selected = FabricReleaseJournal.initialize(root)
        selected.record_intent(intent(1))
        selected.begin_creation(uid(1))
        selected.record_created(uid(1), uid(201), 'original-handle')
        selected.activate_local(uid(1), 0)
        with FabricReleaseJournal(root).reader() as active:
            self.assertEqual(active.intent, intent(1))
        self.assertFalse((root.parent / 'private').exists())
        with self.assertRaises(JournalRefusal):
            with self.owner.reader():
                pass

    def test_cancellation_remains_primary_when_both_closing_checks_refuse(self):
        self.created(1)
        self.owner.activate_local(uid(1), 0)
        cancellation = KeyboardInterrupt('original cancellation')
        with self.assertRaises(KeyboardInterrupt) as captured:
            with self.owner.reader():
                (self.root / 'journal.sqlite').chmod(0o644)
                (self.root / 'lease').chmod(0o644)
                raise cancellation
        self.assertIs(captured.exception, cancellation)
        self.assertTrue(cancellation.cleanup_failed)
        (self.root / 'journal.sqlite').chmod(0o600)
        (self.root / 'lease').chmod(0o600)
        # Closing refusal did not leak the process lease.
        self.created(2)
        self.owner.activate_local(uid(2), 1)

    def test_mutation_lifetime_multiple_durable_writes_excludes_other_processes(self):
        context = multiprocessing.get_context('spawn')
        with self.owner.mutation() as permit:
            self.assertIs(type(permit), FabricMutation)
            permit.record_intent(intent(1))
            permit.begin_creation(uid(1))
            self.assertEqual(permit.candidate(uid(1)), (intent(1), 'unknown', None, None))
            # The lifetime also covers arbitrary candidate staging/external calls,
            # with no transaction open and no nested flock acquisition.
            for action in (child_read, child_mutate):
                receiver, sender = context.Pipe(duplex=False)
                process = context.Process(target=action, args=(str(self.root), sender))
                process.start()
                sender.close()
                try:
                    self.assertTrue(receiver.poll(10), 'child lease check timed out')
                    self.assertEqual(receiver.recv(), 'lease busy')
                    process.join(10)
                    self.assertEqual(process.exitcode, 0)
                finally:
                    if process.is_alive():
                        process.kill()
                        process.join()
                    receiver.close()
            permit.renew()
            permit.record_created(uid(1), uid(201), 'original-operation-1')
            self.assertEqual(permit.activate_local(uid(1), 0), 1)
        with self.owner.reader() as active:
            self.assertEqual(active.intent, intent(1))
        with self.assertRaisesRegex(JournalRefusal, 'expired'):
            permit.begin_creation(uid(1))
        with self.assertRaises(TypeError):
            FabricMutation()

    def test_cancelled_mutation_expires_permit_preserves_unknown_and_primary(self):
        cancellation = KeyboardInterrupt('creation outcome unknown')
        with self.assertRaises(KeyboardInterrupt) as captured:
            with self.owner.mutation() as permit:
                permit.record_intent(intent(1))
                permit.begin_creation(uid(1))
                (self.root / 'lease').chmod(0o644)
                raise cancellation
        self.assertIs(captured.exception, cancellation)
        self.assertTrue(cancellation.cleanup_failed)
        with self.assertRaisesRegex(JournalRefusal, 'expired'):
            permit.record_created(uid(1), uid(201), 'late-response')
        (self.root / 'lease').chmod(0o600)
        reopened = FabricReleaseJournal(self.root)
        self.assertEqual(reopened.candidate(uid(1)), (intent(1), 'unknown', None, None))
        with self.assertRaisesRegex(JournalRefusal, 'already submitted'):
            reopened.begin_creation(uid(1))
        with reopened.mutation() as recovery:
            recovery.record_created(uid(1), uid(201), 'original-recovered-handle')
            recovery.activate_local(uid(1), 0)

    def test_mutation_permit_cannot_dispatch_to_replaced_owner(self):
        other_root = Path(self.tmp.name).resolve() / 'other-journal'
        other = FabricReleaseJournal.initialize(other_root)
        with self.owner.mutation() as permit:
            permit._owner = other
            with self.assertRaisesRegex(JournalRefusal, 'owner changed'):
                permit.record_intent(intent(1))
        for selected in (self.owner, other):
            with self.assertRaisesRegex(JournalRefusal, 'missing original intent'):
                selected.candidate(uid(1))
        with other.mutation() as permit:
            self.assertIsNone(permit.renew())
            permit.record_intent(intent(1))

    def test_original_async_creation_handle_survives_cancel_before_model_id(self):
        with self.assertRaises(KeyboardInterrupt):
            with self.owner.mutation() as permit:
                permit.record_intent(intent(1))
                permit.begin_creation(uid(1))
                # Actual 202-response correspondence supplied by future native
                # owner; this test exercises retention, not a native response.
                permit.record_creation_operation(uid(1), 'original-operation')
                self.assertEqual(permit.candidate(uid(1)),
                                 (intent(1), 'creating', None, 'original-operation'))
                raise KeyboardInterrupt()
        with self.assertRaisesRegex(JournalRefusal, 'expired'):
            permit.record_creation_operation(uid(1), 'replacement')
        reopened = FabricReleaseJournal(self.root)
        self.assertEqual(reopened.candidate(uid(1)), (intent(1), 'creating', None, 'original-operation'))
        with reopened.mutation() as recovered:
            with self.assertRaisesRegex(JournalRefusal, 'already submitted'):
                recovered.begin_creation(uid(1))
            with self.assertRaises(JournalRefusal):
                recovered.record_creation_operation(uid(1), 'replacement')
            with self.assertRaisesRegex(JournalRefusal, 'changed creation handle'):
                recovered.record_created(uid(1), uid(201), 'replacement')
            self.assertEqual(recovered.candidate(uid(1))[1:], ('creating', None, 'original-operation'))
            recovered.record_created(uid(1), uid(201), 'original-operation')

    def test_original_first_load_handle_is_durable_immutable_and_item_bound(self):
        self.owner.record_intent(intent(1))
        self.owner.begin_creation(uid(1))
        self.assertIsNone(self.owner.first_load(uid(1)))
        with self.assertRaisesRegex(JournalRefusal, 'created item'):
            self.owner.record_first_load(uid(1), 'too-early')
        self.owner.record_created(uid(1), uid(201), 'original-operation')
        with self.assertRaises(KeyboardInterrupt):
            with self.owner.mutation() as permit:
                permit.record_first_load(uid(1), 'sole-original-load')
                self.assertEqual(permit.first_load(uid(1)), 'sole-original-load')
                raise KeyboardInterrupt()
        reopened = FabricReleaseJournal(self.root)
        self.assertEqual(reopened.first_load(uid(1)), 'sole-original-load')
        with self.assertRaisesRegex(JournalRefusal, 'expired'):
            permit.record_first_load(uid(1), 'replacement')
        for replacement in ('sole-original-load', 'replacement'):
            with self.assertRaisesRegex(JournalRefusal, 'already retained'):
                reopened.record_first_load(uid(1), replacement)
        self.created(2)
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            reopened.record_first_load(uid(2), 'sole-original-load')
        self.assertIsNone(reopened.first_load(uid(2)))
        self.assertEqual(reopened.candidate(uid(1))[2], uid(201))

    def test_older_journal_without_load_storage_refuses_without_migration(self):
        self.created(1)
        import sqlite3
        with sqlite3.connect(self.root / 'journal.sqlite') as db:
            db.execute('DROP TABLE first_load')
        for action in (lambda: self.owner.first_load(uid(1)),
                       lambda: self.owner.record_first_load(uid(1), 'original-load')):
            with self.assertRaisesRegex(JournalRefusal, 'implicit migration refused'):
                action()
        with sqlite3.connect(self.root / 'journal.sqlite') as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='first_load'").fetchone())
        self.assertEqual(self.owner.candidate(uid(1))[1:], ('created', uid(201), 'original-operation-1'))

    def test_synchronous_201_preserves_actual_item_bytes_without_async_handle(self):
        import json
        response = json.dumps({'id': uid(201), 'workspaceId': uid(100), 'type': 'GraphModel',
                               'extension': {'original': True}}, indent=2).encode()
        self.owner.record_intent(intent(1))
        self.owner.begin_creation(uid(1))
        self.assertEqual(self.owner.creation(uid(1)).kind, 'unknown')
        with self.assertRaises(JournalRefusal):
            self.owner.record_created_synchronous(uid(1), uid(201), b'')
        with self.assertRaisesRegex(JournalRefusal, 'workspace'):
            self.owner.record_created_synchronous(uid(1), uid(201), response.replace(uid(100).encode(), uid(101).encode()))
        with self.assertRaises(KeyboardInterrupt):
            with self.owner.mutation() as permit:
                permit.record_created_synchronous(uid(1), uid(201), response)
                observed = permit.creation(uid(1))
                self.assertEqual((observed.kind, observed.operation_handle, observed.synchronous_response),
                                 ('synchronous', None, response))
                raise KeyboardInterrupt()
        with self.assertRaisesRegex(JournalRefusal, 'expired'):
            permit.creation(uid(1))
        reopened = FabricReleaseJournal(self.root)
        self.assertEqual(reopened.creation(uid(1)).synchronous_response, response)
        self.assertEqual(reopened.candidate(uid(1)), (intent(1), 'created', uid(201), None))
        with self.assertRaises(JournalRefusal):
            reopened.begin_creation(uid(1))
        with self.assertRaises(JournalRefusal):
            reopened.record_created(uid(1), uid(201), 'invented-async-operation')
        reopened.activate_local(uid(1), 0)
        with reopened.reader() as active:
            self.assertIsNone(active.creation_handle)
            self.assertEqual(active.graph_model_id, uid(201))

    def test_async_and_sync_receipts_cannot_replace_each_other_or_migrate(self):
        import json
        import sqlite3
        response = json.dumps({'id': uid(201), 'workspaceId': uid(100), 'type': 'GraphModel'}).encode()
        self.owner.record_intent(intent(1))
        self.owner.begin_creation(uid(1))
        self.owner.record_creation_operation(uid(1), 'actual-original-operation')
        with self.assertRaisesRegex(JournalRefusal, 'without async handle'):
            self.owner.record_created_synchronous(uid(1), uid(201), response)
        self.assertEqual(self.owner.creation(uid(1)).operation_handle, 'actual-original-operation')
        self.owner.record_intent(intent(2))
        self.owner.begin_creation(uid(2))
        with sqlite3.connect(self.root / 'journal.sqlite') as db:
            db.execute('DROP TABLE synchronous_creation')
        with self.assertRaisesRegex(JournalRefusal, 'implicit migration refused'):
            self.owner.record_created_synchronous(uid(2), uid(201), response)
        self.assertEqual(self.owner.creation(uid(2)).kind, 'unknown')
        with sqlite3.connect(self.root / 'journal.sqlite') as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='synchronous_creation'").fetchone())

    def test_complete_actual_commerce_mapping_fits_separate_utf8_capacity(self):
        import gzip
        import json
        archive = Path(__file__).resolve().parents[1] / 'docs/helix/04-build/evidence/commerce-fabric-publication-gql-20261016/definition/create-request.json.gz'
        definition = json.loads(gzip.decompress(archive.read_bytes()))['definition']
        mapping = json.dumps(definition, sort_keys=True, separators=(',', ':'), allow_nan=False)
        self.assertEqual(len(mapping.encode('utf8')), 13542)
        selected = replace(intent(1), mapping_json=mapping)
        self.owner.record_intent(selected)
        self.assertEqual(self.owner.candidate(uid(1))[0].mapping_json, mapping)
        oversized = json.dumps({'mapping': 'x' * 65536}, separators=(',', ':'))
        with self.assertRaisesRegex(JournalRefusal, '65536 UTF-8 bytes'):
            replace(intent(2), mapping_json=oversized)
        with self.assertRaisesRegex(JournalRefusal, 'bounded nonempty text'):
            replace(intent(2), publication_json='x' * 8193)

    def test_atomic_admission_evidence_stale_cas_rolls_back_and_intent_is_exact(self):
        import json
        from dataclasses import asdict
        self.created(1)
        def receipt(revision):
            return json.dumps({'format': 'ashlar-private-fabric-admission/0.1', 'intent': asdict(intent(1)),
                'expected_revision': revision, 'binding': {'workspace_id': uid(100), 'graph_model_id': uid(201),
                'release_sha256': intent(1).release_sha256}, 'native': 'opaque storage control, not native admission'}).encode()
        with self.owner.mutation() as permit:
            with self.assertRaisesRegex(JournalRefusal, 'intent/item/predecessor'):
                permit.record_admission_and_activate(uid(1), 0, receipt(0).replace(uid(201).encode(), uid(202).encode()))
            with self.assertRaisesRegex(JournalRefusal, 'stale'):
                permit.record_admission_and_activate(uid(1), 1, receipt(1))
        with self.assertRaises(JournalRefusal): self.owner.admission(uid(1))
        with self.owner.mutation() as permit:
            self.assertEqual(permit.record_admission_and_activate(uid(1), 0, receipt(0)), 1)
        self.assertEqual(FabricReleaseJournal(self.root).admission(uid(1)), (0, receipt(0)))
        with self.owner.reader() as active: self.assertEqual(active.revision, 1)
        with self.assertRaisesRegex(JournalRefusal, 'expired'):
            permit.record_admission_and_activate(uid(1), 1, receipt(1))

    def test_closed_typed_correspondence_and_missing_active_refuse(self):
        with self.assertRaises(TypeError):
            ReleaseIntent(**{**intent(1).__dict__, 'authority': True})
        with self.assertRaises(JournalRefusal):
            replace(intent(1), sources=list(intent(1).sources))
        with self.assertRaises(JournalRefusal):
            self.owner.activate_local(uid(1), True)
        with self.assertRaisesRegex(JournalRefusal, 'no local active'):
            with self.owner.reader():
                pass
