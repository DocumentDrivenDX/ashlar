"""Byte-custody inventory of pinned UMF packs; no UMF semantic validation.

Reads Git objects rather than checkout files. Unknown files and JSON content remain
covered by exact-byte hashes. Never executes generators or downloads source URLs.
"""
import argparse
import csv
from collections import Counter
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
from urllib.parse import urlsplit

PIN = '1f7b5f5d2a355c4b476e3a96b289b9048f03f567'
UPSTREAM = 'https://github.com/DocumentDrivenDX/umf'
ROOT = 'spec/domain-packs'


class InventoryError(ValueError):
    pass


def safe_path(value):
    if not isinstance(value, str) or not value or '\\' in value or '\x00' in value:
        raise InventoryError('Invalid repository-relative path')
    p = PurePosixPath(value)
    if p.is_absolute() or any(v in ('', '.', '..') for v in value.split('/')) or ':' in value:
        raise InventoryError('Path must be normalized and repository-relative')
    return value


def sha256(value):
    return hashlib.sha256(value).hexdigest()


def read_json(raw):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise InventoryError('Duplicate JSON member')
            out[key] = value
        return out
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(InventoryError('Nonfinite JSON')))
    except (UnicodeError, json.JSONDecodeError) as e:
        raise InventoryError('Unreadable metadata JSON') from e


class GitSource:
    def __init__(self, repo, commit=PIN):
        if not re.fullmatch(r'[0-9a-f]{40}', commit):
            raise InventoryError('Full lowercase commit hash required')
        self.repo = str(repo)
        self.commit = commit
        resolved = self._git('rev-parse', '--verify', commit + '^{commit}').decode().strip()
        if resolved != commit:
            raise InventoryError('Commit identity changed')
        self.files = {}
        self._bytes = {}
        for record in self._git('ls-tree', '-rz', commit, '--', ROOT).split(b'\0'):
            if not record:
                continue
            metadata, path = record.split(b'\t', 1)
            mode, kind, oid = metadata.decode().split()
            path = safe_path(path.decode('utf-8'))
            if kind != 'blob' or mode not in ('100644', '100755'):
                raise InventoryError('Symlink or nonregular pack source refused')
            self.files[path] = oid

    def _git(self, *args):
        p = subprocess.run(['git', '-C', self.repo, *args], capture_output=True)
        if p.returncode:
            raise InventoryError('Pinned Git object unavailable')
        return p.stdout

    def read(self, path):
        path = safe_path(path)
        if path not in self.files:
            raise InventoryError('Path absent from pinned pack tree')
        if path not in self._bytes:
            self._bytes[path] = self._git('cat-file', 'blob', self.files[path])
        return self._bytes[path]


def source_reference(source, base, source_id, item):
    """Describe source custody without interpreting its schema or URL content."""
    row = {'id': source_id, 'declaration': item}
    reference = item.get('reference')
    if not reference:
        row['availability'] = 'no-reference'
    elif isinstance(reference, str) and urlsplit(reference).scheme:
        row['availability'] = 'external-not-fetched' if urlsplit(reference).scheme in ('https', 'http') else 'unresolved-uri'
    else:
        path = base + '/' + safe_path(reference)
        row['path'] = path
        if path not in source.files:
            row['availability'] = 'unbundled'
        else:
            raw = source.read(path)
            row.update(availability='bundled', sha256=sha256(raw), bytes=len(raw))
            declared = item.get('checksum', {})
            if declared.get('algorithm') == 'sha256':
                row['declared_checksum_matches'] = declared.get('value') == row['sha256']
    return row


def build_inventory(source):
    packs = []
    manifests = sorted(p for p in source.files if p.endswith('/pack.json') and p.count('/') == 3)
    for manifest in manifests:
        base = manifest.rsplit('/', 1)[0]
        name = base.rsplit('/', 1)[1]
        pack = read_json(source.read(manifest))
        files = []
        tabular = []
        csv_counts = {}
        notices = []
        for path in sorted(p for p in source.files if p.startswith(base + '/')):
            raw = source.read(path)
            relative = path[len(base)+1:]
            files.append({'path': relative, 'git_blob': source.files[path],
                          'sha256': sha256(raw), 'bytes': len(raw)})
            if relative.startswith('umf/') and relative.endswith('.json'):
                schema = read_json(raw)
                tabular.append({'path': relative, 'format_version': schema.get('version'),
                                'table_name': schema.get('table_name'),
                                'columns': len(schema.get('columns', [])),
                                'declared_relationships': schema.get('relationships', {})})
            if relative.startswith('data/') and relative.endswith('.csv'):
                csv_counts[relative] = sum(1 for _ in csv.DictReader(io.StringIO(raw.decode('utf-8'), newline='')))
            if any(s in relative.lower() for s in ('license', 'notice', 'attribution')):
                notices.append(relative)
        entry = {'directory': name, 'id': pack.get('id'), 'version': pack.get('version'),
                 'pack_sha256': sha256(source.read(manifest)), 'files': files,
                 'tabular_schemas': tabular, 'csv_row_counts': csv_counts,
                 'notice_files': notices, 'qualification': pack.get('qualification'),
                 'sources': [source_reference(source, base, k, v)
                             for k, v in sorted(pack.get('sources', {}).items())],
                 'schema_declarations': pack.get('schemas', []),
                 'bindings': pack.get('bindings', pack.get('source_bindings', [])),
                 'scenario_checks': pack.get('scenario_checks', [])}
        ontology_path = base + '/ontology.json'
        if ontology_path in source.files:
            model = read_json(source.read(ontology_path))
            entry['ontology'] = {'path': 'ontology.json', 'umf_version': model.get('umf'),
                                 'document_id': model.get('id'),
                                 'modules': [{'id': m.get('id'), 'elements': len(m.get('elements', [])),
                                              'relationships': m.get('relationships', [])}
                                             for m in model.get('modules', [])]}
        graph_path = base + '/graph/fixture.json'
        if graph_path in source.files:
            graph = read_json(source.read(graph_path))
            objects, edges = graph.get('objects', []), graph.get('edges', [])
            keys = {o['key'] for o in objects}
            entry['graph'] = {'path': 'graph/fixture.json', 'format': graph.get('format'),
                              'version': graph.get('version'), 'schema': graph.get('schema'),
                              'pack': graph.get('pack'), 'qualification': graph.get('qualification'),
                              'objects': len(objects), 'edges': len(edges),
                              'object_types': [{'type': read_json(k.encode()), 'count': v} for k, v in sorted(Counter(json.dumps(o.get('type'), sort_keys=True) for o in objects).items())],
                              'relationship_types': [{'relationship': read_json(k.encode()), 'count': v} for k, v in sorted(Counter(json.dumps(e.get('relationship'), sort_keys=True) for e in edges).items())],
                              'unique_object_keys': len(keys),
                              'unique_edge_keys': len({e['key'] for e in edges}),
                              'unresolved_endpoint_count': sum(e['source'] not in keys or e['target'] not in keys for e in edges),
                              'source_metadata': graph.get('source_metadata', {}),
                              'source_bindings': graph.get('source_bindings', []),
                              'dataset': graph.get('dataset'), 'run': graph.get('run')}
            for label, path in [('schema', ontology_path), ('pack', manifest)]:
                digest = graph.get(label, {}).get('sha256')
                entry['graph'][label + '_checksum_matches'] = (digest == sha256(source.read(path))) if digest and path in source.files else None
        packs.append(entry)
    return {'format': 'ashlar.domain-pack-inventory', 'version': '0.1',
            'upstream': UPSTREAM, 'commit': source.commit, 'source_root': ROOT,
            'qualification': 'Exact Git-byte custody and structural inventory only. No UMF validation, canonical Value conversion, graph admission or engine support claim.',
            'packs': packs}


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + '\n').encode('utf-8')


def verify_copy(source, pack, destination):
    root = Path(destination)
    if root.is_symlink():
        raise InventoryError('Symlink copy root refused')
    pack = safe_path(pack)
    if '/' in pack:
        raise InventoryError('Single pack directory required')
    prefix = ROOT + '/' + pack + '/'
    paths = sorted(p for p in source.files if p.startswith(prefix))
    if not paths:
        raise InventoryError('Pinned pack absent')
    expected = {p[len(prefix):] for p in paths}
    actual = set()
    for p in root.rglob('*'):
        if p.is_symlink():
            raise InventoryError('Symlink copied source refused')
        if p.is_file():
            actual.add(p.relative_to(root).as_posix())
    if actual != expected:
        raise InventoryError('Copied pack path inventory differs')
    for path in paths:
        if (root / path[len(prefix):]).read_bytes() != source.read(path):
            raise InventoryError('Copied pack source bytes differ')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--umf-repo', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--commerce-copy')
    args = parser.parse_args()
    source = GitSource(args.umf_repo)
    raw = encoded(build_inventory(source))
    target = Path(args.output)
    if args.check:
        if target.read_bytes() != raw:
            raise InventoryError('Inventory differs from pinned source objects')
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
    if args.commerce_copy:
        verify_copy(source, 'commerce', args.commerce_copy)
    print(json.dumps({'commit': source.commit, 'packs': len(read_json(raw)['packs']),
                      'inventory_sha256': sha256(raw), 'check': args.check}))


if __name__ == '__main__':
    main()
