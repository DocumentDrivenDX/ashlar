"""Exact streaming receipt storage roundtrip and corruption refusal."""
import json,unittest,tempfile,sqlite3
from pathlib import Path
from unittest.mock import patch
from ashlar_host.local_receipt_codec import encode_local_receipt,original_receipt_chunks,local_receipt_equals,CHUNK

class Tests(unittest.TestCase):
    def test_bounded_legacy_receipt_survives_compression_overflow_only(self):
        from ashlar_host.local_receipt_codec import ReceiptStorageOverflow,STORED_LIMIT
        original='{"original":"exact legacy"}'
        with patch('ashlar_host.local_receipt_codec.encode_receipt_chunks',side_effect=ReceiptStorageOverflow('forced storage budget')):
            self.assertEqual(encode_local_receipt(original),original)
            self.assertTrue(local_receipt_equals(encode_local_receipt(original),original))
            with patch('ashlar_host.local_receipt_codec.encode_lzma_receipt_chunks',side_effect=ReceiptStorageOverflow('forced fallback overflow')):
                with self.assertRaises(ReceiptStorageOverflow):encode_local_receipt('x'*(STORED_LIMIT+1))
        with patch('ashlar_host.local_receipt_codec.encode_receipt_chunks',side_effect=ValueError('invalid original')):
            with self.assertRaisesRegex(ValueError,'invalid original'):encode_local_receipt(original)
    def test_original_unknown_content_roundtrips_and_changed_digest_refuses(self):
        original=json.dumps({'profile':'ashlar-local-delta-receipt/0.1','unknown':{'future':['exact '*1000000]},'snapshot':{'rows':[]}},ensure_ascii=False,separators=(',',':'))
        stored=encode_local_receipt(original)
        self.assertTrue(local_receipt_equals(original,original))
        chunks=list(original_receipt_chunks(stored))
        self.assertTrue(all(len(chunk)<=CHUNK for chunk in chunks));self.assertEqual(b''.join(chunks),original.encode())
        self.assertTrue(local_receipt_equals(stored,original));self.assertFalse(local_receipt_equals(stored,original+' '))
        value=json.loads(stored);value['original_sha256']='a'*64
        with self.assertRaises(ValueError):list(original_receipt_chunks(json.dumps(value)))
        oversized=json.dumps({'profile':'ashlar-local-delta-receipt-zlib/0.1','padding':'x'*4194304})
        with self.assertRaises(ValueError):list(original_receipt_chunks(oversized))
        self.assertTrue(local_receipt_equals('{"original":"legacy"}','{"original":"legacy"}'))
        self.compact_checked(original)

    def test_large_repeated_block_uses_lossless_lzma_after_actual_zlib_overflow(self):
        import base64,random
        from ashlar_host.local_receipt_codec import encode_receipt_chunks,ReceiptStorageOverflow,LZMA_PROFILE,STORED_LIMIT
        block=base64.b64encode(random.Random(17).randbytes(1048576)).decode()
        original=json.dumps({'profile':'original-native-receipt','metadata':[block]*4},separators=(',',':'))
        raw=original.encode();self.assertGreater(len(raw),STORED_LIMIT)
        with self.assertRaises(ReceiptStorageOverflow):encode_receipt_chunks((raw[offset:offset+CHUNK]for offset in range(0,len(raw),CHUNK)),len(raw))
        stored=encode_local_receipt(original);self.assertEqual(json.loads(stored)['profile'],LZMA_PROFILE)
        self.assertLessEqual(len(stored.encode()),STORED_LIMIT)
        chunks=list(original_receipt_chunks(stored));self.assertTrue(all(len(chunk)<=CHUNK for chunk in chunks))
        self.assertEqual(b''.join(chunks),raw);self.assertTrue(local_receipt_equals(stored,original))
        self.compact_checked(original)

    def test_lzma_corrupt_length_digest_and_trailing_stream_refuse(self):
        import base64
        from ashlar_host.local_receipt_codec import encode_lzma_receipt_chunks
        original=b'{"original":"exact"}'
        stored=json.loads(encode_lzma_receipt_chunks((original,),len(original)))
        for field,value in (('original_length',len(original)-1),('original_sha256','a'*64),
            ('lzma_base64',base64.b64encode(base64.b64decode(stored['lzma_base64'])+b'trailing').decode())):
            changed={**stored,field:value}
            with self.assertRaises(ValueError):list(original_receipt_chunks(json.dumps(changed)))

    def compact_checked(self,original):
        from ashlar_host.delta_custody import DeltaTarget,encoded,local_journal_definitions
        from ashlar_host.local_journal_compaction import compact_original_local_delta_journal
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);path=root/'operations.sqlite'
            for name in ('.finite-installation.lock','operations.sqlite.lock'):
                (root/name).write_bytes(b'');(root/name).chmod(0o600)
            roles=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
            for role in roles:(root/role).mkdir()
            targets=tuple(DeltaTarget('local.graph.'+role,root/role,role+'-uuid')for role in roles)
            registry={'installation':'original','targets':[{'table':target.table,'path':str(target.path),'uuid':target.uuid}for target in sorted(targets,key=lambda item:item.table)]}
            db=sqlite3.connect(path)
            for schema in local_journal_definitions().values():db.execute(schema)
            db.execute('INSERT INTO local_installation VALUES(1,?)',(encoded(registry),))
            db.execute('INSERT INTO local_operation VALUES(?,?,?,?,?,?)',('original','original-intent','original-hash',1,'committed',original));db.commit();db.close();path.chmod(0o600)
            result=compact_original_local_delta_journal(path,'original',targets)
            db=sqlite3.connect(path)
            try:
                receipt=db.execute('SELECT receipt FROM local_operation').fetchone()[0]
                self.assertTrue(local_receipt_equals(receipt,original));self.assertEqual(db.execute('SELECT intent,intent_sha,before_version,state FROM local_operation').fetchone(),('original-intent','original-hash',1,'committed'))
                self.assertLess(result['after_bytes'],result['before_bytes']);self.assertEqual(result['receipts'],1)
            finally:db.close()
