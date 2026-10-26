# SPDX-License-Identifier: MIT OR Apache-2.0
"""Authored six-role UMF 0.10 migration for the current managed Delta target.

The old v0.1 carrier source remains immutable. These separately retained
v0.10 documents adapt only the UMF core header; current UMF public DDL output
and current native UC UUID/schema/protocol observations are separate gates.
"""
from dataclasses import dataclass
import hashlib
import json
import re
from importlib.resources import files

ROLES=('object_current','edge_current','tombstone','whole_source_history',
       'publication_manifest','publication_attempt_phase')
GENERATED_SHA256='438e63964bd79670481a05670d881b7551737a90f6492713807946748d14cd07'
NATIVE_TYPES={'string':'STRING','long':'BIGINT','boolean':'BOOLEAN','timestamp':'TIMESTAMP'}
ROOT='src/ashlar_host/resources/managed-current-core-010/'

def require(value):
    if not value:raise ValueError('Current UMF 0.10 managed carrier admission refused')

def json_object(raw):
    value=json.loads(raw)
    require(type(value)is dict)
    return value

@dataclass(frozen=True)
class CurrentManagedCarriers:
    generated_bytes:bytes
    inputs:tuple
    physical_profile='ashlar-delta/0.3'
    lookup_hash_profile='sha256-json-v1'
    def __post_init__(self):
        require(type(self.generated_bytes)is bytes and hashlib.sha256(self.generated_bytes).hexdigest()==GENERATED_SHA256)
        generated=json_object(self.generated_bytes)
        require(generated.get('profile')=='ashlar-managed-current-carriers/0.1'
            and generated.get('generator')=={'project':'UMF','revision':'2ede4c879bea5db13794a6fa30e67c821306c17f','extension':'umf.delta.definition/0.1.0'}
            and set(generated.get('carriers',{}))==set(ROLES))
        originals=dict(self.inputs)
        require(len(originals)==7 and set(originals)=={'index.json',*(r+'.umf.json' for r in ROLES)})
        require(set(generated.get('inputs',{}))=={ROOT+k for k in originals})
        for name,raw in self.inputs:require(hashlib.sha256(raw).hexdigest()==generated['inputs'][ROOT+name])
        index=json_object(originals['index.json'])
        require(index.get('profile')=='ashlar-managed-current-carrier-migration/0.1'
            and index.get('umfCore')=='0.10.0' and index.get('roles')==list(ROLES))
        for role in ROLES:
            model=json_object(originals[role+'.umf.json'])
            require(model.get('umf')=='0.10.0'
                and model.get('vocabularies',{}).get('umf.delta',{}).get('version')=='0.1.0'
                and model.get('vocabularies',{}).get('umf.delta.definition',{}).get('version')=='0.1.0')
            carrier=generated['carriers'][role]
            require(carrier['sql'].startswith('CREATE TABLE `'+role+'` (') and carrier['sql'].endswith(';\n'))
            columns=carrier['columns'];require(type(columns)is list and bool(columns)
                and len({c['name'] for c in columns})==len(columns)
                and all(c['deltaType'] in NATIVE_TYPES and type(c['nullable'])is bool for c in columns))
    def ordered_fields(self,role):
        if role not in ROLES:raise ValueError('Exact current managed role required')
        generated=json_object(self.generated_bytes)
        return tuple((c['name'],NATIVE_TYPES[c['deltaType']],c['nullable']) for c in generated['carriers'][role]['columns'])
    def create_sql(self,role,qualified_name):
        if role not in ROLES or type(qualified_name)is not tuple or len(qualified_name)!=3:
            raise ValueError('Exact current managed role/name required')
        if any(type(part)is not str or re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,127}',part)is None for part in qualified_name):
            raise ValueError('Portable qualified managed identifier required')
        generated=json_object(self.generated_bytes);sql=generated['carriers'][role]['sql']
        # UMF emits the authored unqualified name. Only quote-qualified target
        # identity is substituted into the DDL proposal; no values are spliced.
        quoted='.'.join('`'+part.replace('`','``')+'`' for part in qualified_name)
        return sql.replace('CREATE TABLE `'+role+'`','CREATE TABLE '+quoted,1)

def load_current_managed_carriers(profile='ashlar-delta/0.3'):
    if profile == 'ashlar-delta/0.4-xxhash64':
        from .managed_current_xxhash64_carriers import load_current_managed_xxhash64_carriers
        return load_current_managed_xxhash64_carriers()
    if profile != 'ashlar-delta/0.3':
        raise ValueError('Exact managed-current carrier profile required')
    root=files('ashlar_host').joinpath('resources','managed-current-core-010')
    def read(name):
        with root.joinpath(name).open('rb') as stream:
            raw=stream.read(16777217)
        require(0<len(raw)<=16777216)
        return raw
    names=('index.json',*(r+'.umf.json' for r in ROLES))
    return CurrentManagedCarriers(read('runtime-carriers.generated.json'),tuple((n,read(n)) for n in names))
