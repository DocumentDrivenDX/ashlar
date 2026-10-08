import hashlib
from pathlib import Path
import unittest
from ashlar.native import SQLResult
from ashlar.outbox import PostgresOutbox,OutboxError
ROOT=Path(__file__).resolve().parents[1]
class Executor:
    def __init__(self,rows,head='1'):self.rows=rows;self.head=head
    def query(self,sql,parameters):
        if '.head ' in sql:return SQLResult([{'position':self.head}])
        return SQLResult(self.rows)
class OutboxTests(unittest.TestCase):
    def row(self):
        raw=(ROOT/'examples/end-to-end/source.jsonl').read_bytes()
        return {'position':'1','batch_id':'fixture-tx-1','payload':raw.decode(),'digest':hashlib.sha256(raw).hexdigest()}
    def read(self,rows,head='1',after='0'):
        return PostgresOutbox(Executor(rows,head),feed='native-feed',epoch='native-epoch').read(after)
    def test_exact_native_cursor_separate_from_jsonl_offset(self):
        transaction=self.read([self.row()])[0]
        self.assertEqual((transaction.previous,transaction.position),('0','1'))
        self.assertNotEqual(transaction.position,transaction.batch.cursor_after)
        self.assertEqual(transaction.batch.feed,'native-feed')
    def test_missing_unordered_corrupt_source_refuses(self):
        for row in [dict(self.row(),position='2'),dict(self.row(),digest='0'*64),dict(self.row(),batch_id='other')]:
            with self.assertRaises(OutboxError):self.read([row],head='2')
        with self.assertRaises(OutboxError):self.read([self.row()],head='2')
        with self.assertRaises(OutboxError):self.read([])
        with self.assertRaises(OutboxError):self.read([self.row(),self.row()])
    def test_bounds_and_noncanonical_cursor_refuse(self):
        for cursor in ['01','-1',str(2**63),True]:
            with self.assertRaises(OutboxError):self.read([],after=cursor)
        with self.assertRaises(OutboxError):self.read([],head='0',after='1')
if __name__=='__main__':unittest.main()
