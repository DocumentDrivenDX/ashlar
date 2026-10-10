"""Held finite-file publication reader; no commerce or PostgreSQL factory."""
from contextlib import contextmanager
import hashlib,json
from pathlib import Path
from ashlar.publication import Snapshot,resolve_publication
from ashlar.native import quote_table_identifier
from ashlar.manifest import FIELDS
from .delta_custody import encoded
from .lifecycle import owned_context
from .supply_chain_request import SOURCE_SYSTEM,SOURCE_SHA,source_snapshot
from .supply_chain_publication import FiniteSupplyChainDriver,GRAPH_ROLES

class FiniteSupplyChainProvider:
    def __init__(self,driver,aliases,expected_binding):
        if type(driver)is not FiniteSupplyChainDriver or driver.manifest is None:raise ValueError('Actual original committed finite driver required')
        aliases=source_snapshot(aliases);binding=source_snapshot(expected_binding)
        if type(aliases)is not dict or set(aliases)!={driver.tables[r]for r in GRAPH_ROLES}or len(set(aliases.values()))!=4:
            raise ValueError('Complete injective original alias vector required')
        for alias in aliases.values():quote_table_identifier(alias)
        self.driver,self.context,self.aliases=driver,driver.context,aliases
        self.expected_binding=encoded(binding);self.active=False;self.closed=None
    def runtime(self,context):
        if context is not self.context or not self.active:raise PermissionError('Held original finite reader required')
        self.driver.require(context);self.driver.admit_current(self.driver.manifest)
        spark=self.driver.transport.spark
        if spark.version!='4.0.1' or spark.sql("SELECT ('a' COLLATE UTF8_BINARY) = ('A' COLLATE UTF8_BINARY) AS equal").first()['equal']is not False:
            raise ValueError('Selected native Spark4.0.1 binary runtime required')
    @contextmanager
    def interval(self,context):
        if context is not self.context or self.active:raise PermissionError('Nonnested original finite reader required')
        self.closed=None
        with owned_context(self.driver.writer(self.driver.request['stream'],context)):
            self.active=True
            try:
                opening=self.resolve(context);yield;closing=self.resolve(context)
                if opening!=closing:raise ValueError('Original finite publication changed during held read')
            finally:self.active=False
        self.closed={'profile':'ashlar-immutable-file-publication-interval/0.1','publication_id':opening.descriptor.publication_id,
            'manifest_sha256':hashlib.sha256(encoded(dict(opening.descriptor.raw)).encode()).hexdigest(),
            'source':self.driver.source.metadata(),'qualification':'Actual held complete native publication and original finite-file receipt renewed through release; no protected ACK/outbox authority'}
    def closed_interval_custody(self,context):
        if context is not self.context or self.active or self.closed is None:raise PermissionError('Complete finite interval not released')
        return source_snapshot(self.closed)
    def authorize(self,context,publication_id,tables):
        self.runtime(context)
        if publication_id!=self.driver.publication_id or set(tables)!=set(self.aliases):raise ValueError('Original complete finite read vector required')
    def descriptors(self,publication_id):
        fields=','.join('CAST(unix_micros(recorded_at) AS STRING) AS recorded_at'if k=='recorded_at'else k for k in FIELDS)
        return self.driver.transport.query('SELECT '+fields+' FROM '+quote_table_identifier(self.driver.tables['manifest'])+' WHERE publication_id=:id LIMIT 2',{'id':publication_id}).rows
    def validate_descriptor(self,descriptor,context):
        self.driver.validate_manifest(dict(descriptor.raw),context)
    def inspect_snapshot(self,table,version):
        target=self.driver.transport.targets[table]
        detail=self.driver.transport.query('DESCRIBE DETAIL '+quote_table_identifier(table),{}).rows
        if len(detail)!=1 or detail[0]['id']!=target.uuid:raise ValueError('Original finite native identity differs')
        if type(version)is not int or not 0<=version<2**63:raise ValueError('Exact original native version required')
        self.driver.transport.spark.read.format('delta').option('versionAsOf',version).load(str(target.path)).limit(1).collect()
        return Snapshot(table,target.uuid,version)
    def resolve(self,context):
        return resolve_publication(self,self.driver.publication_id,{t:self.driver.transport.targets[t].uuid for t in self.aliases},context=context,
            supported_profiles=['ashlar-delta/0.3'],supported_revisions={SOURCE_SYSTEM:[SOURCE_SHA]})
    def admit_binding(self,binding,resolved,context):
        self.runtime(context)
        if encoded(binding)!=self.expected_binding:raise ValueError('Original finite model/carrier binding differs')
        expected={'id':resolved.descriptor.publication_id,'manifestUuid':self.driver.transport.targets[self.driver.tables['manifest']].uuid,
            'tables':[{'name':self.aliases[t].split('.'),'uuid':s.uuid,'version':s.version}for t,s in sorted(resolved.snapshots.items())]}
        if binding['publication']!=expected:raise ValueError('Original finite full compiler vector differs')
        for native,alias in self.aliases.items():
            detail=self.driver.transport.spark.sql('DESCRIBE DETAIL '+quote_table_identifier(alias)).first().asDict();target=self.driver.transport.targets[native]
            if detail['id']!=target.uuid or detail['location'].removeprefix('file:').rstrip('/')!=str(target.path):raise ValueError('Original finite alias identity differs')
    def native_table_schema(self,table,resolved,context):
        self.runtime(context);table=source_snapshot(table)
        matching=[t for t,alias in self.aliases.items()if alias.split('.')==table.get('name')]
        if len(matching)!=1:raise ValueError('Schema outside original finite alias vector')
        native=matching[0];snapshot=resolved.snapshots[native]
        if table!={'name':self.aliases[native].split('.'),'uuid':snapshot.uuid,'version':snapshot.version}:raise ValueError('Original finite schema pin differs')
        schema=self.driver.transport.spark.read.format('delta').option('versionAsOf',snapshot.version).load(str(self.driver.transport.targets[native].path)).schema
        return {'table':table,'schema':json.loads(schema.json()),'nativeTypes':[[f.name,f.dataType.simpleString().upper()]for f in schema.fields]}
