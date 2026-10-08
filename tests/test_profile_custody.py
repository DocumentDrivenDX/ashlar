import copy
import json
from pathlib import Path
import unittest
from ashlar.profile_custody import verify_profile_custody,ProfileCustodyError

ROOT=Path(__file__).resolve().parents[1]
B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/profile_custody_20261008'

class ProfileCustodyTests(unittest.TestCase):
    def setUp(self):
        self.rows=json.loads((B/'summary.json').read_text())['receipts']
        self.bundle=(B/'source-bundle.json').read_bytes()
    def verify(self,row):return verify_profile_custody(row,identity=row['identity'],version=row['version'],definition=bytes.fromhex(self.rows[0]['definition_hex']),source_bundle=self.bundle)
    def test_actual_native_original_receipts_and_reader_replay(self):
        reader=[json.loads(line) for line in (B/'reader-output.jsonl').read_text().splitlines()]
        replay=json.loads((B.parent/'profile_custody_replay_20261008/summary.json').read_text())['receipts']
        self.assertEqual(self.rows,replay)
        self.assertEqual(sorted(reader,key=lambda r:r['identity']),sorted(self.rows,key=lambda r:r['identity']))
        self.assertEqual(len(self.rows),6)
        for row in self.rows:
            receipt=verify_profile_custody(row,identity=row['identity'],version=row['version'],definition=bytes.fromhex(row['definition_hex']),source_bundle=self.bundle)
            self.assertEqual(receipt.original_database_role,'ashlar_profile_custody_writer')
            self.assertEqual(receipt.original_session_role,'postgres')
            self.assertFalse(hasattr(receipt,'admitted'))
    def test_changed_byte_custody_or_numeric_xid_refuses(self):
        for changes in [{'definition_sha256':'0'*64},{'definition_hex':'00'},{'bundle_sha256':'0'*64},{'original_xid':1},{'original_xid':'01'},{'original_xid':'9'*100000},{'unaccounted':'field'}]:
            row=copy.deepcopy(self.rows[0]);row.update(changes)
            with self.assertRaises(ProfileCustodyError):self.verify(row)

if __name__=='__main__':unittest.main()
