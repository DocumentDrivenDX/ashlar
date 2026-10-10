"""Pure Paths-profile boundary tests with fixture-only trust identities."""
import gzip
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ashlar import weft_paths_package as p

class PathsPackageTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name).resolve()
    def tearDown(self):self.temp.cleanup()
    def config(self,raw=b'{}'):
        index=self.root/'index';index.write_bytes(raw)
        return p.PathsInstallationConfig(index,'a'*40,hashlib.sha256(raw).hexdigest(),None,'fixture-paths',self.root/'output','aarch64-apple-darwin','27.0.1')
    def test_wrong_trust_pin_refuses_before_package_or_ready(self):
        c=self.config();object.__setattr__(c,'index_sha256','b'*64)
        original=p.read_snapshot
        def only_index(path,*args):
            self.assertEqual(path,c.index_path);return original(path,*args)
        with patch.object(p,'read_snapshot',side_effect=only_index):
            with self.assertRaises(p.PathsInstallationError):p.inspect_package(c)
        self.assertFalse(c.output.exists())
    def test_numeric_duplicate_and_utf8_refusals(self):
        for raw in [b'{"a":1,"a":2}',b'1'*10000,b'1.5',b'NaN',b'"\xff"',b'\xff\xfe{\x00}\x00']:
            with self.assertRaises(p.PathsInstallationError):p.decode_document(raw)
        self.assertEqual(p.decode_document(b'{"n":1842,"lexeme":"0002"}'),{'n':1842,'lexeme':'0002'})
    def test_decoded_profile_bound_is_local_and_single_member(self):
        raw=b'x'*17;packed=gzip.compress(raw,mtime=0)
        self.assertEqual(p._inflate(packed,17),raw)
        for value,limit in [(packed,16),(packed+packed,100),(packed+b'x',100)]:
            with self.assertRaises(p.PathsInstallationError):p._inflate(value,limit)
        from ashlar import weft_installation as old
        self.assertEqual(old.DECODED_LIMIT,20*1024*1024);self.assertEqual(p.DECODED_LIMIT,64*1024*1024)
    def test_fifo_and_symlink_refuse_bounded_descriptor(self):
        fifo=self.root/'fifo';os.mkfifo(fifo)
        with self.assertRaises(p.PathsInstallationError):p.read_snapshot(fifo)
        target=self.root/'target';target.write_bytes(b'value');link=self.root/'link';link.symlink_to(target)
        with self.assertRaises(p.PathsInstallationError):p.read_snapshot(link)
        with self.assertRaises(p.PathsInstallationError):p.read_snapshot(target,4)
    def test_exact_trie_rejects_extra_without_descending(self):
        (self.root/'known').write_bytes(b'value');(self.root/'unexpected').mkdir()
        original=os.scandir;visited=[]
        def scan(path):visited.append(path);return original(path)
        with patch.object(p.os,'scandir',side_effect=scan):
            with self.assertRaises(p.PathsInstallationError):p.verify_exact_tree(self.root,['known'])
        self.assertEqual(visited,[self.root])
    def test_aggregate_preflight_no_next_read(self):
        store=p._Package(self.root);store.total=p.TOTAL_LIMIT
        with patch.object(p,'read_snapshot',side_effect=AssertionError('unbounded next read')):
            with self.assertRaises(p.PathsInstallationError):store.artifact({'path':'next','sha256':'a'*64,'bytes':1})
    def test_read_primary_survives_readonly_marker_and_close_failure(self):
        class Cancel(KeyboardInterrupt):
            def __setattr__(self,name,value):
                if name=='cleanup_failed':raise RuntimeError('no marker')
                super().__setattr__(name,value)
        primary=Cancel();target=self.root/'input';target.write_bytes(b'original');close=os.close
        def bad_close(fd):close(fd);raise OSError('close')
        with patch.object(p.os,'read',side_effect=primary),patch.object(p.os,'close',side_effect=bad_close):
            with self.assertRaises(Cancel)as caught:p.read_snapshot(target)
        self.assertIs(caught.exception,primary)
    def test_legacy_shape_and_missing_package_are_not_fallback(self):
        with self.assertRaises(p.PathsInstallationError):p.validate_manifest({'format':'weft-distribution/0.1'})
        raw=p.encode_document({'format':'weft-distribution-index/0.1','entries':[{'realizationId':'fixture-paths','manifest':{'path':'manifest.json','sha256':'a'*64,'bytes':1},'assemblyCustody':{'path':'assembly-custody.json','sha256':'b'*64,'bytes':1},'executable':{'path':'bin/weft-paths','sha256':p.BINARY,'bytes':8273840},'target':'aarch64-apple-darwin'}]})
        with self.assertRaises(p.PathsInstallationError):p.inspect_package(self.config(raw))

if __name__=='__main__':unittest.main()
