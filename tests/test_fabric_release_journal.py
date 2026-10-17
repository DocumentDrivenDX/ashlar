"""Real local lifecycle storage; no Fabric/native/source authority claim."""
from dataclasses import replace
from pathlib import Path
import multiprocessing
import tempfile
import unittest
from uuid import UUID

from ashlar_host.fabric_release_journal import FabricReleaseJournal, JournalRefusal, ReleaseIntent


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
