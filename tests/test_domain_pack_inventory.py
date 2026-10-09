import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from domain_pack_inventory import (GitSource, InventoryError, ROOT, build_inventory,
                                   read_json, safe_path, sha256, verify_copy)


class DomainPackInventoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        self.base = self.repo / ROOT / 'fixture'
        self.base.mkdir(parents=True)
        self.write('data/a.csv', b'id,value\nA,"original \xc3\xa9"\n')
        self.write('ontology.json', b'{"umf":"0.8.0","id":"fixture","modules":[],"future":{"kept":true}}\n')
        pack = {'id': 'fixture', 'version': '1', 'sources': {
            'a': {'reference': 'data/a.csv', 'checksum': {'algorithm': 'sha256', 'value': sha256((self.base/'data/a.csv').read_bytes())}},
            'url': {'reference': 'https://example.invalid/data'},
            'unresolved': {'reference': 'urn:unresolved:test'},
            'missing': {'reference': 'data/not-bundled.csv'}}, 'unknown': {'preserve': 7}}
        self.write('pack.json', json.dumps(pack).encode())
        self.write('graph/fixture.json', json.dumps({'schema': {'sha256': sha256((self.base/'ontology.json').read_bytes())},
            'pack': {'sha256': sha256((self.base/'pack.json').read_bytes())},
            'objects': [{'key':'A'}], 'edges': [{'key':'e','source':'A','target':'missing'}]}).encode())
        self.write('licenses/NOTICE.txt', b'Original notice\n')
        self.write('opaque.bin', bytes(range(256)))
        self.git('init', '-q')
        self.commit = self.save()

    def write(self, rel, raw):
        p = self.base / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(raw)

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.repo), *args], stderr=subprocess.DEVNULL).decode().strip()

    def save(self):
        self.git('add', '.')
        self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture')
        return self.git('rev-parse', 'HEAD')

    def source(self):
        return GitSource(self.repo, self.commit)

    def test_inventory_uses_git_objects_not_dirty_worktree(self):
        first = build_inventory(self.source())
        self.write('pack.json', b'not JSON and not source authority')
        self.write('opaque.bin', b'changed')
        self.assertEqual(first, build_inventory(self.source()))
        files = {f['path']: f for f in first['packs'][0]['files']}
        self.assertEqual(files['opaque.bin']['sha256'], sha256(bytes(range(256))))
        self.assertEqual(first['packs'][0]['notice_files'], ['licenses/NOTICE.txt'])
        self.assertEqual(first['packs'][0]['csv_row_counts'], {'data/a.csv': 1})

    def test_full_commit_only_and_missing_pin_refuses(self):
        for pin in ['HEAD', self.commit[:7], self.commit.upper(), '0'*40]:
            with self.subTest(pin=pin), self.assertRaises(InventoryError):
                GitSource(self.repo, pin)

    def test_provenance_reports_hash_and_endpoint_conflicts(self):
        self.write('data/a.csv', b'id,value\nA,changed\n')
        self.write('pack.json', (self.base/'pack.json').read_bytes()+b'\n')
        self.commit = self.save()
        pack = build_inventory(self.source())['packs'][0]
        self.assertFalse(pack['sources'][0]['declared_checksum_matches'])
        self.assertFalse(pack['graph']['pack_checksum_matches'])
        self.assertTrue(pack['graph']['schema_checksum_matches'])
        self.assertEqual(pack['graph']['unresolved_endpoint_count'], 1)
        availability = {s['id']:s['availability'] for s in pack['sources']}
        self.assertEqual(availability, {'a':'bundled','missing':'unbundled','unresolved':'unresolved-uri','url':'external-not-fetched'})

    def test_path_escape_and_git_expression_refuse(self):
        for path in ['../x','/absolute','a/../x','a//x','a/./x','C:\\file','x\x00y','HEAD:path']:
            with self.subTest(path=path), self.assertRaises(InventoryError):
                safe_path(path)
        self.write('pack.json', b'{"sources":{"escape":{"reference":"../../outside"}}}')
        self.commit = self.save()
        with self.assertRaises(InventoryError):
            build_inventory(self.source())

    def test_symlink_git_object_refuses(self):
        (self.base/'link').symlink_to('/does/not/exist')
        self.commit = self.save()
        with self.assertRaises(InventoryError):
            self.source()

    def test_duplicate_members_invalid_utf8_and_nonfinite_refuse(self):
        for raw in [b'{"id":1,"id":2}', b'{"x":NaN}', b'{"x":Infinity}', b'\xff']:
            with self.subTest(raw=raw), self.assertRaises(InventoryError):
                read_json(raw)

    def test_missing_or_nested_copy_pack_refuses(self):
        for pack in ['missing', 'fixture/data']:
            with self.subTest(pack=pack), self.assertRaises(InventoryError):
                verify_copy(self.source(), pack, self.repo/'absent')

    def test_copy_is_exact_including_unknown_files(self):
        source = self.source()
        dest = self.repo/'copy'
        prefix = ROOT+'/fixture/'
        for p in source.files:
            path = dest/p[len(prefix):]
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(source.read(p))
        verify_copy(source,'fixture',dest)
        (dest/'opaque.bin').write_bytes(b'changed')
        with self.assertRaises(InventoryError):
            verify_copy(source,'fixture',dest)
        (dest/'opaque.bin').write_bytes(bytes(range(256)))
        (dest/'extra').write_bytes(b'extra')
        with self.assertRaises(InventoryError):
            verify_copy(source,'fixture',dest)
        (dest/'extra').unlink()
        (dest/'opaque.bin').unlink()
        (dest/'opaque.bin').symlink_to(self.base/'opaque.bin')
        with self.assertRaises(InventoryError):
            verify_copy(source,'fixture',dest)


if __name__ == '__main__':
    unittest.main()
