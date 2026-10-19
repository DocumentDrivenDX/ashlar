# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact packaged canonical managed eight-table assets, not a UMF interpreter.

Original generator/input provenance and ordered declared columns are retained.
A matching installation is retained correspondence only: current native actor,
grants, UUIDs, schema, protocol, retention and source parity remain separate.
"""
from dataclasses import dataclass
import hashlib,json,re
from importlib.resources import files
from uuid import UUID
from ashlar.schema import decode_retained_json
from .lifecycle import finish

GENERATED_SHA256='89bcb139316bfbb110c9f3beaa1b582de6d521239217abcc307e55fc43a81e45'
TABLES=('object_current','edge_current','source_record','property_journal','tombstone','publication_manifest','publication_attempt_phase','whole_source_history')
PREFIX='docs/helix/02-design/models/ashlar-delta-runtime/'
NATIVE_TYPES={'string':'STRING','long':'BIGINT','boolean':'BOOLEAN','timestamp':'TIMESTAMP'}

def _require(value):
    if not value:raise ValueError('Original canonical managed carrier correspondence refused')
def _json(raw):return decode_retained_json(raw)

@dataclass(frozen=True)
class ManagedCarriers:
    generated_bytes:bytes
    inputs:tuple
    def __post_init__(self):
        _require(type(self.generated_bytes)is bytes and hashlib.sha256(self.generated_bytes).hexdigest()==GENERATED_SHA256)
        _require(type(self.inputs)is tuple and len(self.inputs)==9 and all(type(p)is tuple and len(p)==2 and type(p[0])is str and type(p[1])is bytes and 0<len(p[1])<=65536 for p in self.inputs))
        originals=dict(self.inputs);_require(len(originals)==9 and set(originals)=={'index.json',*(t+'.umf.json'for t in TABLES)})
        generated=_json(self.generated_bytes);_require(generated['profile']=='ashlar-generated-delta-carriers/0.1'and set(generated['carriers'])==set(TABLES))
        _require(set(generated['inputs'])=={PREFIX+n for n in originals})
        for name,raw in self.inputs:_require(hashlib.sha256(raw).hexdigest()==generated['inputs'][PREFIX+name])
        index=_json(originals['index.json']);_require(set(index['tables'])==set(TABLES)and len(index['tables'])==len(TABLES)and re.fullmatch('[0-9a-f]{40}',index['umfRevision'])is not None)
        _require(generated['generator']=={'project':'UMF','revision':index['umfRevision'],'extension':'umf.delta.definition/0.1.0'})
        for role in TABLES:
            carrier=generated['carriers'][role]
            _require(carrier['sql'].startswith('CREATE TABLE `'+role+'` (')and carrier['sql'].endswith(';\n'))
            _require(type(carrier['columns'])is list and bool(carrier['columns'])and len({c['name']for c in carrier['columns']})==len(carrier['columns']))
            _require(all(c['deltaType']in NATIVE_TYPES and type(c['nullable'])is bool for c in carrier['columns']))
    @property
    def generated_sha256(self):return GENERATED_SHA256
    @property
    def generator(self):return tuple(sorted(_json(self.generated_bytes)['generator'].items()))
    def ordered_columns(self,role):
        if role not in TABLES:raise ValueError('Exact managed carrier role required')
        return tuple((c['name'],NATIVE_TYPES[c['deltaType']])for c in _json(self.generated_bytes)['carriers'][role]['columns'])
    def ordered_fields(self,role):
        """Full original column names, native family and nullability, in declared order."""
        if role not in TABLES:raise ValueError('Exact managed carrier role required')
        return tuple((c['name'],NATIVE_TYPES[c['deltaType']],c['nullable'])for c in _json(self.generated_bytes)['carriers'][role]['columns'])


def load_managed_carriers():
    root=files('ashlar_host').joinpath('resources','managed-delta-v03')
    def read(name):
        stream=root.joinpath(name).open('rb');primary=None;raw=None
        try:
            raw=stream.read(65537);_require(0<len(raw)<=65536)
        except BaseException as error:primary=error
        finish(primary,[stream.close]);return raw
    generated=read('runtime-carriers.generated.json')
    inputs=tuple((name,read(name))for name in ('index.json',*(t+'.umf.json'for t in TABLES)))
    return ManagedCarriers(generated,inputs)


def validate_managed_installation(installation,carriers):
    """Original tool installation checks, without checkout or native authority."""
    _require(type(carriers)is ManagedCarriers);carriers.__post_init__()
    _require(type(installation)is dict);namespace=installation.get('namespace')
    _require(type(namespace)is str and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',namespace)is not None)
    generated=_json(carriers.generated_bytes)
    _require(installation.get('state')=='installed'and installation.get('generated_carriers_sha256')==carriers.generated_sha256
        and installation.get('umf_generator')==generated['generator']and installation.get('model_inputs')==generated['inputs'])
    _require(type(installation.get('authenticated_owner'))is str and bool(installation['authenticated_owner']))
    tables=installation.get('tables');_require(type(tables)is dict and set(tables)=={namespace+'.'+t for t in TABLES})
    seen=set()
    for role in TABLES:
        entry=tables[namespace+'.'+role];_require(type(entry)is dict);uuid=entry.get('uuid')
        try:canonical=str(UUID(uuid))
        except (ValueError,TypeError,AttributeError):raise ValueError('Canonical managed table UUID required')from None
        _require(uuid==canonical and uuid not in seen);seen.add(uuid)
        _require(entry.get('columns')==[list(c)for c in carriers.ordered_columns(role)])
    return namespace
