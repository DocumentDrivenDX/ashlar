# SPDX-License-Identifier: MIT OR Apache-2.0
"""Immutable first planning custody only; no native source/effect authority."""
import json,tempfile,unittest
import sqlite3
from types import SimpleNamespace
from pathlib import Path
from ashlar_host.truss_saved_publication import retain_first_truss_planning,require_saved_planning_for_existing_plans

class Tests(unittest.TestCase):
    def test_existing_plan_cannot_create_a_replacement_planning_companion(self):
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory)/'missing-planning.json';db=sqlite3.connect(':memory:')
            try:
                db.execute('CREATE TABLE local_source_plan(request_digest TEXT)');db.execute('CREATE TABLE local_plan(operation TEXT)')
                db.execute('INSERT INTO local_source_plan VALUES(?)',('original',))
                with self.assertRaisesRegex(ValueError,'retained first planning companion'):
                    require_saved_planning_for_existing_plans(SimpleNamespace(db=db),'original',path)
                self.assertFalse(path.exists())
            finally:db.close()
    def test_original_timestamp_and_selection_retained_changed_request_refuses(self):
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory)/'original-intent.json-planning.json'
            selection={'path':'/original/intent.json','sha256':'a'*64};request={'request_digest':'b'*64,'source_batch_json':'original-whole-page'}
            first=retain_first_truss_planning(path,selection,request,'original-predecessor');raw=path.read_bytes()
            self.assertEqual(path.stat().st_mode&0o777,0o600)
            self.assertEqual(json.loads(raw),first)
            self.assertEqual(retain_first_truss_planning(path,selection,request,'original-predecessor'),first)
            self.assertEqual(path.read_bytes(),raw)
            for candidate in ({**request,'source_batch_json':'changed-whole-page'},{**request,'request_digest':'c'*64}):
                with self.assertRaisesRegex(ValueError,'first Truss planning differs'):
                    retain_first_truss_planning(path,selection,candidate,'original-predecessor')
            self.assertEqual(path.read_bytes(),raw)
