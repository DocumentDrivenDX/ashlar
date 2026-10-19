# SPDX-License-Identifier: MIT OR Apache-2.0
"""Session-owned external aliases inside an actual configured publication hold.

Only metadata for successfully created aliases is removed. Original tables, data,
manifest/vector identities and source authority are never rewritten or released.
"""
from collections.abc import Mapping
from contextlib import contextmanager
import json,uuid
from pathlib import Path
from types import MappingProxyType
from urllib.parse import urlsplit,unquote
from ashlar.native import quote_table_identifier
from ashlar.publication import ResolvedPublication,Descriptor,Snapshot
from .configured_local_read import ConfiguredLocalRead
from .delta_custody import LocalDeltaTransport
from .lifecycle import finish

_MINT=object()
def _require(value):
    if not value:raise PermissionError('Original configured query alias custody refused')

class ConfiguredQueryAliases(Mapping):
    def __setattr__(self,name,value):
        if getattr(self,'_sealed',False):raise AttributeError('Configured query aliases are immutable')
        object.__setattr__(self,name,value)
    def __init__(self,scope,transport,aliases,*,_mint=None):
        _require(_mint is _MINT and type(scope)is ConfiguredLocalRead and type(transport)is LocalDeltaTransport and scope.transport is transport)
        self.scope=scope;self.transport=transport;self.spark=transport.spark;self.spark_context=self.spark.sparkContext
        self.application_id=self.spark_context.applicationId;self.master=self.spark_context.master
        self.catalog=self.spark.catalog.currentCatalog();self.database=self.spark.catalog.currentDatabase()
        _require(self.master=='local[1]'and type(self.application_id)is str and bool(self.application_id)and self.catalog=='spark_catalog')
        self._aliases=MappingProxyType(dict(aliases));self.descriptor=scope.descriptor;self.vector=scope.vector
        self._targets=tuple((table,transport.targets[table].uuid,transport.targets[table].path,self.descriptor.versions[table])for table in aliases)
        _require(set(aliases)==set(self.descriptor.versions)==set(self.vector.targets)and len(aliases)==4)
        for table,identity,path,version in self._targets:
            _require(self.vector.targets[table]==(identity,version)and type(path)is type(Path('/'))and path.is_absolute()and path.resolve()==path and not any(c in str(path)for c in ("'",'\\','\0')))
        self._active=True;self._sealed=True
    def __iter__(self):return iter(self._aliases)
    def __len__(self):return len(self._aliases)
    def __getitem__(self,key):return self._aliases[key]
    def _session(self):
        _require(self.transport.spark is self.spark and self.spark.sparkContext is self.spark_context
            and self.spark_context.master==self.master=='local[1]'and self.spark_context.applicationId==self.application_id
            and self.spark.catalog.currentCatalog()==self.catalog=='spark_catalog'and self.spark.catalog.currentDatabase()==self.database)
    def _identity(self,table):
        self._session();alias=self._aliases[table];target=next(t for t in self._targets if t[0]==table)
        _require(self.spark.catalog.getTable(alias).tableType=='EXTERNAL')
        detail=self.spark.sql('DESCRIBE DETAIL '+quote_table_identifier(alias)).first().asDict()
        location=detail.get('location');_require(type(location)is str)
        uri=urlsplit(location);_require(uri.scheme in ('','file')and not uri.netloc and not uri.query and not uri.fragment)
        _require(detail.get('format')=='delta'and detail.get('id')==target[1]and Path(unquote(uri.path)).resolve()==target[2])
    def renew(self):
        _require(self._active and type(self.scope)is ConfiguredLocalRead and self.scope.transport is self.transport)
        self._session();self.scope.renew()
        _require(self.scope.descriptor is self.descriptor and self.scope.vector is self.vector
            and tuple((table,self.transport.targets[table].uuid,self.transport.targets[table].path,self.descriptor.versions[table])for table in self)==self._targets)
        for table in self:self._identity(table)
        self.scope.renew()
    def native_table_schema(self,table,resolved,context):
        self.renew();_require(context is self.scope.context and type(resolved)is ResolvedPublication and type(resolved.descriptor)is Descriptor and resolved.descriptor==self.descriptor)
        _require(type(table)is dict)
        matches=[original for original,alias in self.items()if alias.split('.')==table.get('name')];_require(len(matches)==1)
        original=matches[0];target=next(t for t in self._targets if t[0]==original)
        _require(table=={'name':self[original].split('.'),'uuid':target[1],'version':target[3]})
        _require(set(resolved.snapshots)==set(self.vector.targets)and all(type(resolved.snapshots[t])is Snapshot and resolved.snapshots[t].table==t and (resolved.snapshots[t].uuid,resolved.snapshots[t].version)==self.vector.targets[t]for t in self.vector.targets))
        schema=self.spark.read.format('delta').option('versionAsOf',target[3]).load(str(target[2])).schema
        result={'table':dict(table),'schema':json.loads(schema.json()),'nativeTypes':[[f.name,f.dataType.simpleString().upper()]for f in schema.fields]}
        self.scope.validate_snapshot(original,target[1],target[3],tuple(tuple(c)for c in result['nativeTypes']))
        self.renew();return result


@contextmanager
def open_configured_query_aliases(scope,transport):
    _require(type(scope)is ConfiguredLocalRead and type(transport)is LocalDeltaTransport and scope.transport is transport)
    scope.renew();namespace='ashlar_configured_query_'+uuid.uuid4().hex
    aliases={table:'spark_catalog.'+namespace+'.'+table.rsplit('.',1)[1]for table in scope.descriptor.versions}
    owner=ConfiguredQueryAliases(scope,transport,aliases,_mint=_MINT);created=[];database=False;primary=None
    name=quote_table_identifier('spark_catalog.'+namespace+'.placeholder').rsplit('.',1)[0]
    try:
        owner._session();owner.spark.sql('CREATE DATABASE '+name).collect();database=True
        for table,alias in owner.items():
            owner._session();scope.renew();target=next(t for t in owner._targets if t[0]==table)
            owner.spark.sql('CREATE TABLE '+quote_table_identifier(alias)+" USING DELTA LOCATION '"+str(target[2])+"'").collect()
            created.append(table);owner._identity(table)
        owner.renew();yield owner
    except BaseException as error:primary=error
    def drop(table):
        owner._identity(table);owner.spark.sql('DROP TABLE '+quote_table_identifier(owner[table])).collect()
    def drop_database():
        owner._session();owner.spark.sql('DROP DATABASE '+name).collect()
    def deactivate():object.__setattr__(owner,'_active',False)
    finish(primary,([owner.renew]if len(created)==4 else[])+[lambda t=t:drop(t)for t in reversed(created)]+([drop_database]if database else[])+[deactivate])
