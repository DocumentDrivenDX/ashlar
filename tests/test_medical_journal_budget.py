# SPDX-License-Identifier: MIT OR Apache-2.0
"""Selected main-file bounds use real SQLite and the actual POSIX lease."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import test_medical_publication_sources as source_fixture
from ashlar_host.medical_installation import OriginalMedicalInstallationLease, require_medical_journal_budget

class MedicalJournalBudgetTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name).resolve()
        self.selection=source_fixture.Tests.make_selection(self)
        self.db,self.reservation=source_fixture.Tests.setup_reservations(self);self.addCleanup(self.db.close)
        self.context=object()
    def test_lower_selection_refuses_before_sqlite_preflight_and_lock_creation(self):
        with patch('ashlar_host.medical_installation.LocalDeltaTransport.require_existing_journal')as preflight:
            with self.assertRaisesRegex(PermissionError,'budget exceeded'):
                with OriginalMedicalInstallationLease.open(self.selection,self.context,journal_bytes=1):pass
            preflight.assert_not_called()
        self.assertFalse((self.root/'.evolution-operator.lock').exists())
    def test_exact_size_opens_and_growth_refuses_renewal_and_closing(self):
        size=self.selection.journal.stat().st_size
        with self.assertRaisesRegex(PermissionError,'budget exceeded'):
            with OriginalMedicalInstallationLease.open(self.selection,self.context,journal_bytes=size)as lease:
                self.db.execute('INSERT INTO local_source_plan VALUES(?,?)',('a','x'*100000));self.db.commit()
                with self.assertRaisesRegex(PermissionError,'budget exceeded'):lease.renew(self.context)
        self.assertTrue(lease.closed)
    def test_selected_budget_mutation_refuses_without_raising_original(self):
        with self.assertRaisesRegex(PermissionError,'budget changed'):
            with OriginalMedicalInstallationLease.open(self.selection,self.context,journal_bytes=8388608)as lease:
                lease.journal_bytes=134217728
                with self.assertRaisesRegex(PermissionError,'budget changed'):lease.renew(self.context)
        self.assertTrue(lease.closed)
    def test_invalid_budget_refuses_before_lock_or_sqlite(self):
        for value in (True,0,-1,134217729,1.0):
            with self.subTest(value=value),patch('ashlar_host.medical_installation.LocalDeltaTransport.require_existing_journal')as preflight:
                with self.assertRaises(ValueError):
                    with OriginalMedicalInstallationLease.open(self.selection,self.context,journal_bytes=value):pass
                preflight.assert_not_called()
    def test_symlink_journal_refuses(self):
        link=self.root/'foreign.sqlite';link.symlink_to(self.selection.journal)
        with self.assertRaises(PermissionError):require_medical_journal_budget(link,134217728)
