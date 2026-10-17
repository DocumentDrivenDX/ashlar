"""Typed reads through Ashlar's original held publication execution boundary.

Hosts provide native publication, pin, policy and deadline owners. This module
never opens a connection or treats an SDK digest as publication authority.
"""
from __future__ import annotations

import copy
import hashlib
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager
from datetime import timedelta
from decimal import Decimal
from time import monotonic
from types import MappingProxyType
from typing import Any, TypeVar

from pydantic import BaseModel
from umf_access.adoption import TargetIdentity
from umf_access.codec_support import revalidate_model
from umf_access.engine_catalog import EngineRead
from umf_access.native_program import canonical_json, program_json
from umf_access.pages import (
    ReadContinuation,
    ReadObligationUnmet,
    ReadPage,
    StaleContinuation,
    StreamLimitReached,
)
from umf_access.reviewed import CatalogRead

from .execution_scope import preserve_body_failure
from .publication import ResolutionError
from .weft_decode import ExactScalar, decode_exact_scalar
from .weft_query import PageContextRefused, PageReadRows, read_weft
from .weft_related_keys_decode import DecodedRelatedKeys

P=TypeVar('P',bound=BaseModel);R=TypeVar('R',bound=BaseModel)

def _parameter(value):
    if type(value)is str:return {'family':'string','value':value}
    if type(value)is bool:return {'family':'boolean','value':'true' if value else 'false'}
    if type(value)is int:return {'family':'integer','value':str(value)}
    if type(value)is Decimal and value.is_finite():return {'family':'decimal','value':format(value,'f')}
    raise ResolutionError('Closed exact non-temporal scalar parameter required')

def _scalar(value):
    if type(value) is ExactScalar:
        if type(value.value) not in (str,bool,int,Decimal) or type(value.value) is Decimal and not value.value.is_finite():raise ResolutionError('Original non-temporal scalar value required')
        return value.value
    if type(value) in (dict,MappingProxyType):
        if dict(value)=={'state':'absent'}:raise ResolutionError('ADOPTION_MISMATCH: selected column absent')
        if dict(value)=={'state':'null'}:return None
        if set(value)=={'state','value'} and value['state']=='value':return value['value']
        raise ResolutionError('Exact source-decoded scalar column required')
    if value is None or type(value) in (str,bool,int,Decimal):return value
    raise ResolutionError('Selected scalar result carrier unsupported')

def _key_values(values,types):
    carriers={'string':str,'boolean':bool,'integer':int,'decimal':Decimal}
    if len(values)!=len(types)or any(type(value)is not carriers.get(kind['family'])or type(value)is Decimal and not value.is_finite()for value,kind in zip(values,types,strict=True)):
        raise ResolutionError('Complete exact original native page Key carriers required')
    return tuple(values)

class _Policy:
    def __init__(self,original,compiler,program,model,deadline,*,page_after_json=None,page_metadata=None,total_outputs=None):
        self.original,self.compiler,self.program,self.model,self.deadline=original,compiler,program,model,deadline
        self.validated=None
        self.page_after_json,self.page_metadata,self.total_outputs=page_after_json,page_metadata,total_outputs
    def _time(self):
        if monotonic()>=self.deadline:raise TimeoutError('Ashlar typed read deadline expired')
    def bind_descriptor(self,*args):self._time();return self.original.bind_descriptor(*args)
    def admit_artifact(self,request,artifact,*args):
        self._time()
        checked=program_json(self.compiler.check_engine_read_json(self.program,canonical_json(request)) if self.page_after_json is None else self.compiler.check_engine_page_json(self.program,canonical_json(request),self.page_after_json))
        if checked.get('status')!='compiled' or checked.get('artifact')!=artifact or self.page_metadata is not None and checked.get('page')!=self.page_metadata:
            raise ResolutionError('Original Weft engine compilation correspondence refused')
        return self.original.admit_artifact(request,artifact,*args)
    def verify_native_profile(self,*args):self._time();return self.original.verify_native_profile(*args)
    def authorize_query(self,*args):self._time();return self.original.authorize_query(*args)
    def admit_record_integrity(self,*args):self._time();return self.original.admit_record_integrity(*args)
    def admit_relationship_integrity(self,*args):
        self._time()
        callback=getattr(self.original,'admit_relationship_integrity',None)
        if not callable(callback):raise ResolutionError('Original relationship authority owner required')
        return callback(*args)
    def admit_page_integrity(self,*args):self._time();return self.original.admit_page_integrity(*args)
    def page_context(self,*args):self._time();return self.original.page_context(*args)
    def decode_result(self,*args):
        self._time()
        value=self.original.decode_result(*args)
        if type(value) is ExactScalar:
            representation=args[0].get('representation')
            logical=representation.get('logicalType',{}) if type(representation) is dict else {}
            counted=logical.get('family')=='integer' and logical.get('facets')=={}
            decoded=decode_exact_scalar(representation,value.original,count=counted)
            if type(decoded.value) is not type(value.value) or decoded!=value or type(decoded.value) is Decimal and decoded.value.as_tuple()!=value.value.as_tuple():
                raise ResolutionError('Original exact scalar token correspondence differs')
            return value
        if type(value) is DecodedRelatedKeys:
            if args[0].get('representation',{}).get('kind')!='relatedKeys':
                raise ResolutionError('Original related-Key output representation required')
            return value
        return _scalar(value)
    def authorize_result(self,rows,descriptor,context):
        self._time()
        names=tuple(self.model.model_fields)
        visible=len(names)if self.page_metadata is None else self.page_metadata['visibleOutputs']
        if visible!=len(names):raise ResolutionError('Complete visible generated page signature required')
        from umf_access.related_keys import transport_spec

        from .sdk_related_keys import typed_related_keys
        related=transport_spec(self.model)
        if related is not None:
            output,key_model,decoder,_=related
            outputs=program_json(self.program).get('outputs',[])
            matching=[entry for entry in outputs if entry.get('name')==output]
            if len(matching)!=1 or matching[0].get('carrier')!='related-keys' or matching[0].get('decoder')!=decoder:
                raise ResolutionError('Original related-Key compiler output differs')
        validated=[]
        for row in rows:
            if len(row)!=(len(names)if self.total_outputs is None else self.total_outputs):raise ResolutionError('Complete generated result signature required')
            fields={name:_scalar(value) if type(value) is ExactScalar else value for name,value in zip(names,row[:visible],strict=True)}
            if related is not None:
                fields[output]=typed_related_keys(fields[output],decoder,key_model)
            validated.append(self.model.model_validate(fields))
        if self.original.authorize_result(rows,descriptor,context)is not None:
            raise ResolutionError('Original Ashlar policy refused typed release')
        self._time();self.validated=validated
        return None

class AshlarTarget:
    def __init__(self,*,compiler,executor,backend,pins,vector,policy,context,supported_profiles,supported_revisions,
                 compiler_method:str='registered',
                 binding_sha256:str,platform:tuple[tuple[str,str],...],default_timeout:timedelta,
                 deadline:Callable[[timedelta],AbstractContextManager[None]],catalog_entries:tuple[EngineRead[Any,Any],...]=()):
        if len(binding_sha256)!=64 or any(c not in '0123456789abcdef' for c in binding_sha256):raise ValueError('Original binding digest required')
        if not callable(getattr(compiler,'check_engine_read_json',None)) or not callable(deadline):raise ValueError('Original engine compiler and native deadline owner required')
        if compiler_method not in ('registered','ashlar-spark4','ashlar-spark4-fields','ashlar-dbsql-20261016','ashlar-spark4-pages','ashlar-dbsql-20261016-pages'):raise ValueError('Explicit original Ashlar compiler required')
        self.compiler_method=compiler_method
        self.compiler,self.executor,self.backend,self.pins=compiler,executor,backend,pins
        self.vector,self.policy,self.context=vector,policy,context
        self.supported_profiles,self.supported_revisions=tuple(supported_profiles),{name:tuple(values) for name,values in supported_revisions.items()}
        self.binding_sha256,self.platform=binding_sha256,tuple(platform)
        self.default_timeout,self.deadline,self.catalog_entries=default_timeout,deadline,catalog_entries
        self.native_target_json=None
        if compiler_method.endswith('-pages'):
            if not catalog_entries or not callable(getattr(compiler,'check_engine_page_json',None)):
                raise ValueError('Original page compiler and catalog targets required')
            targets=[]
            for entry in catalog_entries:
                if not isinstance(entry,EngineRead) or entry.artifact()['compiler']!=compiler_method:
                    raise ValueError('Original page compiler targets required')
                target=entry.artifact()['request']['target']
                if target.get('backendId')!='ashlar.databricks' or target.get('bindingSha256')!=binding_sha256 or hashlib.sha256(target.get('bindingJson','').encode()).hexdigest()!=binding_sha256:
                    raise ValueError('Complete original Ashlar page binding required')
                targets.append(target)
            if any(target!=targets[0]for target in targets[1:]):raise ValueError('Mixed original page targets refuse')
            self.native_target_json=canonical_json(targets[0])
        self._budget(None)
    def _budget(self,timeout):
        value=self.default_timeout if timeout is None else timeout
        if type(value)is not timedelta or not 0<value.total_seconds()<=2147483:raise TimeoutError('Positive bounded Ashlar deadline required')
        return value
    def identity(self)->TargetIdentity:
        v=self.vector
        context=canonical_json({'authority':v.authority,'scopeKind':v.scope_kind,'scopeId':v.scope_id,'custodyDigest':v.custody_digest,'targets':{name:list(pair)for name,pair in v.targets.items()}})
        version,profile=('0.1.0-qualified','dbsql2026.39-qualified') if self.compiler_method=='registered' else ('0.1.0-local-spark4-candidate','spark4.0.1-delta4.0.0-candidate')
        if self.compiler_method=='ashlar-dbsql-20261016':version,profile='0.1.0-managed-20261016-candidate','dbsql2026.39-build-20261016-candidate'
        if self.native_target_json is not None:
            original=program_json(self.native_target_json);version,profile=original['backendVersion'],original['targetProfile']
        return TargetIdentity('ashlar','ashlar.databricks',version,profile,self.binding_sha256,self.platform,context)
    def read(self,entry:CatalogRead[P,R],parameters:P,*,timeout:timedelta|None=None)->list[R]:
        if not isinstance(entry,EngineRead):raise ResolutionError('Original engine catalog required')
        program=entry.artifact();request=copy.deepcopy(program['request'])
        if program['version']!='weft.engine-read/0.1':raise ResolutionError('Original ordinary read program required')
        if program['compiler']!=self.compiler_method or request['target']['bindingSha256']!=self.binding_sha256:
            raise ResolutionError('Original Ashlar binding or compiler differs')
        values=revalidate_model(entry.parameters,parameters)
        request['parameters']={n:_parameter(v)for n,v in values.model_dump(mode='python').items()}
        budget=self._budget(timeout);until=monotonic()+budget.total_seconds()
        original=entry.originals()
        with preserve_body_failure(self.deadline(budget)):
            checked=program_json(self.compiler.check_engine_read_json(original,canonical_json(request)))
            if checked.get('status')!='compiled':raise ResolutionError('Original engine catalog current binding refused')
            artifact=checked['artifact']
            if tuple(c['outputName']for c in artifact['columns'])!=tuple(entry.result.model_fields):raise ResolutionError('Complete generated output signature differs')
            policy=_Policy(self.policy,self.compiler,original,entry.result,until)
            read_weft(self.executor,self.backend,self.pins,self.vector,policy,request=request,artifact=artifact,
                context=self.context,supported_profiles=self.supported_profiles,supported_revisions=self.supported_revisions)
            policy._time()
            if policy.validated is None:raise ResolutionError('Typed validation was not completed inside held publication')
            result=policy.validated
        if monotonic()>=until:raise TimeoutError('Ashlar deadline expired during final release')
        return result
    def verify(self)->None:
        if not self.catalog_entries:raise ResolutionError('Explicit engine catalog entries required')
        for entry in self.catalog_entries:
            values={}
            for name,p in entry.artifact()['request']['parameters'].items():
                family,value=p['family'],p['value']
                values[name]={'string':lambda:value,'integer':lambda:int(value),'decimal':lambda:Decimal(value),'boolean':lambda:value=='true'}[family]()
            if entry.artifact()["version"]=="weft.engine-page/0.1":self.page(entry,entry.parameters.model_validate(values))
            else:self.read(entry,entry.parameters.model_validate(values))
    def page(self,entry:CatalogRead[P,R],parameters:P,*,after:ReadContinuation|None=None,timeout:timedelta|None=None)->ReadPage[R]:
        if not isinstance(entry,EngineRead) or self.native_target_json is None:raise ResolutionError('Original Ashlar page target required')
        program=entry.artifact();request=copy.deepcopy(program['request'])
        if program['version']!='weft.engine-page/0.1' or program['compiler']!=self.compiler_method or request['target']!=program_json(self.native_target_json):
            raise ResolutionError('Complete original Ashlar page target differs')
        values=revalidate_model(entry.parameters,parameters)
        request['parameters']={name:_parameter(value)for name,value in values.model_dump(mode='python').items()}
        identity=hashlib.sha256(canonical_json([self.identity().digest(),entry.artifact_json,entry.context_json,request['parameters']]).encode()).hexdigest()
        after_values=None
        if after is not None:
            if type(after)is not ReadContinuation or after.identity_digest!=identity or after.owner_cursor is None:
                raise StaleContinuation('Original publication/catalog/arguments and page context required')
            types=program['artifact']['logicalPlan']['pageKey']['types']
            try:_key_values(after.values,types)
            except ResolutionError as error:raise StaleContinuation('Original complete typed page Key required')from error
            after_values=[_parameter(value)['value']for value in after.values]
        after_json=canonical_json(after_values)
        budget=self._budget(timeout);until=monotonic()+budget.total_seconds();original=entry.originals()
        with preserve_body_failure(self.deadline(budget)):
            checked=program_json(self.compiler.check_engine_page_json(original,canonical_json(request),after_json))
            if checked.get('status')!='compiled':raise ResolutionError('Original owner page composition refused')
            artifact,metadata=checked['artifact'],checked['page']
            visible=metadata['visibleOutputs'];keys=metadata['keyOutputs'];size=program['pageSize']
            if type(visible)is not int or visible!=len(entry.result.model_fields) or keys!=list(range(visible,len(artifact['columns']))) or not keys or metadata['pageSize']!=size or tuple(c['outputName']for c in artifact['columns'][:visible])!=tuple(entry.result.model_fields):
                raise ResolutionError('Complete original page output/Key metadata required')
            policy=_Policy(self.policy,self.compiler,original,entry.result,until,page_after_json=after_json,page_metadata=metadata,total_outputs=len(artifact['columns']))
            try:
                native=read_weft(self.executor,self.backend,self.pins,self.vector,policy,request=request,artifact=artifact,
                    context=self.context,supported_profiles=self.supported_profiles,supported_revisions=self.supported_revisions,
                    continuation_context=None if after is None else after.owner_cursor)
            except PageContextRefused as error:
                if after is not None:raise StaleContinuation('Original publication or caller/policy context no longer admitted')from error
                raise
            policy._time()
            if type(native)is not PageReadRows or policy.validated is None or len(native.rows)!=len(policy.validated) or len(native.rows)>size+1:
                raise ResolutionError('Complete bounded held native page required')
            types=artifact['logicalPlan']['pageKey']['types']
            decoded_keys=[_key_values(tuple(row[index]for index in keys),types)for row in native.rows]
            next_after=None
            if len(native.rows)>size:
                next_after=ReadContinuation(decoded_keys[size-1],identity,native.owner_context)
            result=ReadPage(tuple(policy.validated[:size]),next_after)
        if monotonic()>=until:raise TimeoutError('Ashlar deadline expired during final page release')
        return result
    def stream(self,entry:CatalogRead[P,R],parameters:P,*,max_rows:int,page_size:int|None=None,timeout:timedelta|None=None)->Generator[R,None,None]:
        if type(max_rows)is not int or max_rows<1:raise ValueError('Positive explicit stream row bound required')
        if page_size is not None:
            if type(page_size)is not int or not 1<=page_size<=1000:raise ValueError('Positive bounded stream page_size required')
            if not isinstance(entry,EngineRead):raise ResolutionError('Original engine page catalog required')
            # The original engine program has a fixed lookahead fetch bound.
            # Replacing its authored page size would change compiler custody.
            if page_size!=entry.artifact().get('pageSize'):
                raise ReadObligationUnmet('STREAM_PAGE_SIZE_UNSUPPORTED: original fixed Ashlar page bound differs')
        checked=revalidate_model(entry.parameters,parameters)
        after=None;emitted=0
        while emitted<max_rows:
            page=self.page(entry,checked,after=after,timeout=timeout)
            if not page.items and page.next_after is not None:raise ReadObligationUnmet('Original page made no continuation progress')
            for row in page.items:
                if emitted==max_rows:raise StreamLimitReached(max_rows,truncated=True)
                emitted+=1
                yield row
            if page.next_after is None:return
            if emitted==max_rows:raise StreamLimitReached(max_rows,truncated=None)
            after=page.next_after
