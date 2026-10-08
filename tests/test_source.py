import hashlib
import json
from pathlib import Path
import struct
import unittest
from ashlar.source import SourceError,jsonl_batches

ROOT=Path(__file__).resolve().parents[1]
class JsonlSourceTests(unittest.TestCase):
    def lines(self):return (ROOT/'examples/end-to-end/source.jsonl').read_bytes().splitlines(keepends=True)
    def read(self,lines,**kw):return list(jsonl_batches(lines,feed='feed',epoch='epoch',**kw))
    def test_exact_complete_batch_and_independent_digest(self):
        lines=self.lines();batch=self.read(lines)[0]
        self.assertEqual(batch.cursor_before,'0')
        self.assertEqual(batch.cursor_after,str(sum(map(len,lines))))
        self.assertEqual([e.raw for e in batch.records],lines[1:-1])
        h=hashlib.sha256()
        for raw in lines[1:-1]:h.update(struct.pack('>Q',len(raw)));h.update(raw)
        self.assertEqual(batch.records_sha256,h.hexdigest())
        self.assertIn(b'18446744073709551615',batch.records[0].raw)
        self.assertEqual(self.read(lines),[batch])
    def test_incomplete_and_bad_manifests_never_emit_batch(self):
        for lines in [self.lines()[:-1],self.lines()[1:],self.lines()[:1]+self.lines(),self.lines()[:-1]+[b'{"kind":"commit","batch_id":"wrong","record_count":2,"records_sha256":"x"}\n']]:
            with self.assertRaises(SourceError):self.read(lines)
    def test_duplicate_delivery_refuses(self):
        lines=self.lines();lines[2]=lines[1]
        with self.assertRaises(SourceError):self.read(lines)
    def test_limits_before_batch_emission(self):
        for kw in [{'max_records':1},{'max_transaction_bytes':30}]:
            with self.assertRaises(SourceError):self.read(self.lines(),**kw)
    def test_resume_offsets_do_not_narrow(self):
        start=2**64+123
        batch=self.read(self.lines(),cursor_before=str(start))[0]
        self.assertEqual(batch.cursor_before,str(start))
        self.assertEqual(int(batch.cursor_after),start+sum(map(len,self.lines())))
    def test_invalid_json_and_control_refuse(self):
        for raw in [b'{"kind":"begin","kind":"event"}\n',b'{"kind":"event","x":NaN}\n',b'\xff\n',b'{"kind":"begin","batch_id":"a","extra":1}\n',b'{}']:
            with self.assertRaises(SourceError):self.read([raw])
if __name__=='__main__':unittest.main()
