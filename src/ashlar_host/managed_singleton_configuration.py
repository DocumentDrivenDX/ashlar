# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed installed managed-read selection; configuration grants no authority."""
from dataclasses import dataclass,fields
import json
from pathlib import Path
import re
from ashlar.schema import decode_retained_json
from .config import ProducerConfig
from .managed_publication_selection import ManagedSelectionFile,ManagedPublicationSelectionSpec
from .managed_pin_reader import ManagedPinReaderConfig,parse_managed_pin_reader
from .managed_sql_read import ManagedSQLReadConfig
from .managed_singleton import ManagedSingletonSelector

PROFILE='ashlar-managed-singleton/0.1'

def _require(value):
    if not value:raise ValueError('managed-singleton-configuration')

def _path(value):
    _require(type(value)is str and 0<len(value)<=4096 and '\0'not in value)
    path=Path(value)
    _require(path.is_absolute()and path.resolve()==path)
    return path

def _closed(value,names):
    _require(type(value)is dict and set(value)==set(names))
    return value

@dataclass(frozen=True)
class ManagedSingletonConfig:
    selection:ManagedPublicationSelectionSpec
    sql:ManagedSQLReadConfig
    pins:ManagedPinReaderConfig
    producer:ProducerConfig
    selector:ManagedSingletonSelector
    output:Path
    whole_seconds:int
    stream_bytes:int
    receipt_bytes:int

    @property
    def cleanup_seconds(self):return self.sql.statement_seconds+2*self.sql.http_seconds+10

    def __post_init__(self):
        for value,kind in ((self.selection,ManagedPublicationSelectionSpec),(self.sql,ManagedSQLReadConfig),
                           (self.pins,ManagedPinReaderConfig),(self.producer,ProducerConfig),(self.selector,ManagedSingletonSelector)):
            _require(type(value)is kind);value.__post_init__()
        _require(type(self.output)is type(Path())and self.output.is_absolute()and self.output.resolve()==self.output)
        _require(type(self.whole_seconds)is int and 30<=self.whole_seconds<=600 and self.whole_seconds>self.cleanup_seconds+self.producer.timeout_seconds)
        _require(type(self.stream_bytes)is int and 1<=self.stream_bytes<=1048576)
        _require(type(self.receipt_bytes)is int and 1<=self.receipt_bytes<=self.stream_bytes)
        _require(self.sql.maximum_statements==128)
        protected=[self.selection.journal,self.producer.source,self.pins.passfile,self.sql.config_file,
                   self.selection.installation.path,self.selection.intake_proof.path,self.selection.source_configuration.path]
        _require(all(self.output!=p and self.output not in p.parents and p not in self.output.parents for p in protected))

def parse_managed_singleton(raw):
    """Parse explicit operator settings without acquiring credentials or services."""
    try:
        _require(type(raw)is bytes and 0<len(raw)<=262144)
        value=_closed(decode_retained_json(raw),('profile','selection','sql','pins','producer','selector','output','bounds'))
        _require(value['profile']==PROFILE)
        selection=_closed(value['selection'],('installation','intake_proof','source_configuration','journal','publication_id','completed_ordinal','maximum_journal_bytes','maximum_rows'))
        def pin(value):
            p=_closed(value,('path','sha256'))
            _require(type(p['sha256'])is str and re.fullmatch('[0-9a-f]{64}',p['sha256'])is not None)
            return ManagedSelectionFile(_path(p['path']),p['sha256'])
        selected=ManagedPublicationSelectionSpec(*(pin(selection[k])for k in ('installation','intake_proof','source_configuration')),
            _path(selection['journal']),selection['publication_id'],selection['completed_ordinal'],selection['maximum_journal_bytes'],selection['maximum_rows'])
        sql=dict(_closed(value['sql'],[f.name for f in fields(ManagedSQLReadConfig)]))
        for name in ('config_file','cli'):sql[name]=_path(sql[name])
        producer=dict(_closed(value['producer'],[f.name for f in fields(ProducerConfig)]))
        for name in ('source','bun','git'):producer[name]=_path(producer[name])
        selector=_closed(value['selector'],('source_system','type_id','entity_id'))
        bounds=_closed(value['bounds'],('whole_seconds','stream_bytes','receipt_bytes'))
        result=ManagedSingletonConfig(selected,ManagedSQLReadConfig(**sql),
            parse_managed_pin_reader(json.dumps(value['pins']).encode()),ProducerConfig(**producer),
            ManagedSingletonSelector(selector['source_system'],selector['type_id'],selector['entity_id']),
            _path(value['output']),**bounds)
        result.__post_init__();return result
    except Exception:raise ValueError('managed-singleton-configuration')from None
