"""Prepare exact lexical commerce CSV cells for public UMF literal validation.

This explicit CSV target profile constructs requests; it does not admit UMF values,
validate Records/keys/relationships, allocate storage IDs, or authorize ingestion.
"""
import argparse
import base64
import csv
import io
import json
from pathlib import Path

from domain_pack_inventory import (GitSource, InventoryError, PIN, build_inventory,
                                   encoded, read_json, safe_path, sha256)

PROFILE = 'ashlar.commerce-csv-literals/0.1'


def literal_request(field, lexical):
    """Declared families choose carriers. Public UMF must validate each request."""
    scalar = field.get('scalarType')
    if field.get('kind') != 'field' or field.get('cardinality') != 'one':
        raise InventoryError('Only explicit scalar Field/cardinality-one bindings supported')
    if scalar not in ('string', 'integer', 'decimal'):
        raise InventoryError('Selected scalar family has no converter profile')
    return { {'string': 'string', 'integer': 'integerToken', 'decimal': 'decimalToken'}[scalar]: lexical }


def prepare_values(directory, inventory, inspection):
    root = Path(directory)
    if root.is_symlink():
        raise InventoryError('Symlink source root refused')
    if inventory.get('commit') != PIN or inspection.get('validatorRevision') != PIN:
        raise InventoryError('Original pinned source and validator revision required')
    packs = [p for p in inventory['packs'] if p['directory'] == 'commerce']
    if len(packs) != 1:
        raise InventoryError('Exactly one commerce source inventory required')
    pack_inventory = packs[0]
    expected = {f['path']: f for f in pack_inventory['files']}
    actual = set()
    for path in root.rglob('*'):
        if path.is_symlink():
            raise InventoryError('Symlink source content refused')
        if path.is_file():
            actual.add(path.relative_to(root).as_posix())
    if actual != set(expected):
        raise InventoryError('Copied commerce path inventory differs')
    originals = {}
    for relative, reference in expected.items():
        safe_path(relative)
        raw = (root / relative).read_bytes()
        if sha256(raw) != reference['sha256'] or len(raw) != reference['bytes']:
            raise InventoryError('Original commerce byte custody differs')
        originals[relative] = raw
    model_bytes = originals['ontology.json']
    model = read_json(model_bytes)
    try:
        retained = base64.b64decode(inspection['sourceBase64'], validate=True)
    except (ValueError, KeyError) as error:
        raise InventoryError('Missing original inspection bytes') from error
    if retained != model_bytes or inspection.get('sourceSha256') != sha256(model_bytes) or inspection.get('sourceBytes') != len(model_bytes):
        raise InventoryError('Inspection original-source custody differs')
    if model.get('umf') != '0.8.0' or inspection.get('umfCoreVersion') != '0.8.0' or inspection.get('documentId') != model['id']:
        raise InventoryError('Explicit original UMF 0.8 context required')
    if inspection.get('validation') != {'valid': True, 'complete': True, 'diagnostics': []}:
        raise InventoryError('Original complete public document validation required')
    if inspection.get('resultSchema') != {'id':'urn:umf:core:schema-properties-selection:1.0.0','valid':True}:
        raise InventoryError('Original current public selection-schema receipt required')
    selection = inspection['selection']
    if selection['source'] != model or selection['query'] != {'references':'transitive'}:
        raise InventoryError('Complete original public model selection required')
    elements = {(m['id'], e['id']):e for m in model['modules'] for e in m['elements']}
    selected = {(e['module'],e['element']['id']):e['element'] for e in selection['selection']}
    if selected != elements or len(selection['selection']) != len(elements):
        raise InventoryError('Original complete element selection differs')
    pack = read_json(originals['pack.json'])
    rows = []
    sources = []
    for schema_ref in pack['schemas']:
        if schema_ref['format'] != 'tablespec':
            continue
        table = read_json(originals[safe_path(schema_ref['reference'])])
        table_name = table['table_name']
        record_key = ('domain', schema_ref['id'])
        record = selected.get(record_key)
        if not record or record.get('kind') != 'record' or record.get('name') != table_name:
            raise InventoryError('Explicit table-to-Record binding required')
        members = [selected[(r['module'],r['element'])] for r in record['members']]
        by_name = {m['name']:m for m in members}
        if len(by_name) != len(members) or set(by_name) != {c['name'] for c in table['columns']}:
            raise InventoryError('Exact declared member/column binding required')
        relative = 'data/' + safe_path(table_name) + '.csv'
        raw = originals[relative]
        text = raw.decode('utf-8')
        lines = io.StringIO(text, newline='').readlines()
        reader = csv.reader(io.StringIO(text, newline=''), strict=True)
        header = next(reader)
        if len(set(header)) != len(header) or set(header) != set(by_name):
            raise InventoryError('Exact unique CSV member header required')
        prior = reader.line_num
        count = 0
        for index, cells in enumerate(reader, 1):
            if len(cells) != len(header):
                raise InventoryError('Missing or extra CSV cell refused; no implicit absent/null/default')
            row_bytes = ''.join(lines[prior:reader.line_num]).encode('utf-8')
            prior = reader.line_num
            values = []
            for name, lexical in zip(header, cells):
                field = by_name[name]
                values.append({'field': {'module':'domain','element':field['id']},
                               'column':name, 'lexical':lexical, 'presence':'present',
                               'value':literal_request(field,lexical),
                               'public_umf_validation':'unexecuted'})
            rows.append({'sourceRowKey':json.dumps([PROFILE,model['id'],relative,index],ensure_ascii=False,separators=(',',':')),
                         'source': {'path':relative,'sha256':sha256(raw),'rowOrdinal':index,
                                    'rawRowBase64':base64.b64encode(row_bytes).decode(), 'rawRowSha256':sha256(row_bytes)},
                         'record': {'document':model['id'],'module':'domain','element':record['id']},
                         'declaredKeys':record.get('keys',[]), 'fields':values})
            count += 1
        sources.append({'path':relative,'sha256':sha256(raw),'bytes':len(raw),
                        'sourceBase64':base64.b64encode(raw).decode(),'rows':count,
                        'declaredForeignKeys':table.get('relationships',{}).get('foreign_keys',[])})
    if len({r['sourceRowKey'] for r in rows}) != len(rows):
        raise InventoryError('Source row address collision')
    return {'format':'ashlar.commerce-field-literal-requests','version':'0.1',
            'profile':PROFILE, 'upstreamCommit':PIN, 'documentId':model['id'],
            'ontologySha256':sha256(model_bytes), 'inventorySha256':sha256(encoded(inventory)),
            'inspectionSha256':sha256(encoded(inspection)),
            'rules': {'strings':'Exact decoded CSV text; empty text and literal null remain strings.',
                      'numbers':'Exact decoded CSV text in integerToken/decimalToken; no parsing, normalization or rounding. Public UMF validation still required.',
                      'presence':'Every existing CSV cell is present. Missing/extra columns refuse. No null sentinel or default substitution is declared.',
                      'identity':'Source-row address is injective path/ordinal provenance, not an encoded logical key, graph storage ID or Truss accepted ID.',
                      'time':'ordered_at is declared string and remains string; no timestamp inference.'},
            'sources':sources,'rows':rows,
            'qualification':'Requests only. Original public inspection is a trusted local development input, not signed provenance. Field literal, Record, key, relationship, storage/engine admission and source authority remain separate. Graph fixtures use seeded replay IDs unlike these original CSV template IDs; no implicit graph binding.'}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--umf-repo',required=True)
    p.add_argument('--inventory',required=True)
    p.add_argument('--inspection',required=True)
    p.add_argument('--source',required=True)
    p.add_argument('--output',required=True)
    args=p.parse_args()
    source=GitSource(args.umf_repo)
    authoritative=build_inventory(source)
    original_inventory=Path(args.inventory).read_bytes()
    if original_inventory != encoded(authoritative):
        raise InventoryError('Inventory differs from actual pinned Git objects')
    inspection_raw=Path(args.inspection).read_bytes()
    result=prepare_values(args.source,authoritative,read_json(inspection_raw))
    result['inspectionSha256']=sha256(inspection_raw)
    Path(args.output).write_bytes(encoded(result))
    print(json.dumps({'rows':len(result['rows']),'field_requests':sum(len(r['fields']) for r in result['rows']),
                      'qualification':'Unexecuted public UMF field-literal requests'}))


if __name__=='__main__':
    main()
