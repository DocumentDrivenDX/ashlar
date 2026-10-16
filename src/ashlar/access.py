"""Typed reads through Ashlar's original held publication execution boundary.

Hosts provide native publication, pin, policy and deadline owners. This module
never opens a connection or treats an SDK digest as publication authority.
"""
from __future__ import annotations
from contextlib import AbstractContextManager
from collections.abc import Callable,Generator
from datetime import timedelta
from decimal import Decimal
from time import monotonic
from types import MappingProxyType
from typing import Any,TypeVar
import copy,json
from pydantic import BaseModel
from umf_access.adoption import TargetIdentity
from umf_access.engine_catalog import EngineRead
from umf_access.reviewed import CatalogRead
from umf_access.pages import ReadPage,ReadContinuation
from umf_access.codec_support import revalidate_model
from umf_access.native_program import canonical_json,program_json
from .weft_query import read_weft,_preserve_body_failure
from .publication import ResolutionError
P=TypeVar('P',bound=BaseModel);R=TypeVar('R',bound=BaseModel)

def _parameter(value):
    if type(value)is str:return {'family':'string','value':value}
    if type(value)is bool:return {'family':'boolean','value':'true' if value else 'false'}
    if type(value)is int:return {'family':'integer','value':str(value)}
    if type(value)is Decimal and value.is_finite():return {'family':'decimal','value':format(value,'f')}
    raise ResolutionError('Closed exact non-temporal scalar parameter required')

def _scalar(value):
    if type(value) in (dict,MappingProxyType):
        if dict(value)=={'state':'absent'}:raise ResolutionError('ADOPTION_MISMATCH: selected column absent')
        if dict(value)=={'state':'null'}:return None
        if set(value)=={'state','value'} and value['state']=='value':return value['value']
        raise ResolutionError('Exact source-decoded scalar column required')
    if value is None or type(value) in (str,bool,int,Decimal):return value
    raise ResolutionError('Selected scalar result carrier unsupported')

class _Policy:
    def __init__(self,original,compiler,program,model,deadline):
        self.original,self.compiler,self.program,self.model,self.deadline=original,compiler,program,model,deadline
        self.validated=None
    def _time(self):
        if monotonic()>=self.deadline:raise TimeoutError('Ashlar typed read deadline expired')
    def bind_descriptor(self,*args):self._time();return self.original.bind_descriptor(*args)
    def admit_artifact(self,request,artifact,*args):
        self._time()
        checked=program_json(self.compiler.check_engine_read_json(self.program,canonical_json(request)))
        if checked.get('status')!='compiled' or checked.get('artifact')!=artifact:
            raise ResolutionError('Original Weft engine compilation correspondence refused')
        return self.original.admit_artifact(request,artifact,*args)
    def verify_native_profile(self,*args):self._time();return self.original.verify_native_profile(*args)
    def authorize_query(self,*args):self._time();return self.original.authorize_query(*args)
    def admit_record_integrity(self,*args):self._time();return self.original.admit_record_integrity(*args)
    def decode_result(self,*args):self._time();return _scalar(self.original.decode_result(*args))
    def authorize_result(self,rows,descriptor,context):
        self._time()
        names=tuple(self.model.model_fields)
        validated=[]
        for row in rows:
            if len(row)!=len(names):raise ResolutionError('Complete generated result signature required')
            validated.append(self.model.model_validate(dict(zip(names,row,strict=True))))
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
        if compiler_method not in ('registered','ashlar-spark4'):raise ValueError('Explicit original Ashlar compiler required')
        self.compiler_method=compiler_method
        self.compiler,self.executor,self.backend,self.pins=compiler,executor,backend,pins
        self.vector,self.policy,self.context=vector,policy,context
        self.supported_profiles,self.supported_revisions=tuple(supported_profiles),{name:tuple(values) for name,values in supported_revisions.items()}
        self.binding_sha256,self.platform=binding_sha256,tuple(platform)
        self.default_timeout,self.deadline,self.catalog_entries=default_timeout,deadline,catalog_entries
        self._budget(None)
    def _budget(self,timeout):
        value=self.default_timeout if timeout is None else timeout
        if type(value)is not timedelta or not 0<value.total_seconds()<=2147483:raise TimeoutError('Positive bounded Ashlar deadline required')
        return value
    def identity(self)->TargetIdentity:
        v=self.vector
        context=canonical_json({'authority':v.authority,'scopeKind':v.scope_kind,'scopeId':v.scope_id,'custodyDigest':v.custody_digest,'targets':{name:list(pair)for name,pair in v.targets.items()}})
        version,profile=('0.1.0-qualified','dbsql2026.39-qualified') if self.compiler_method=='registered' else ('0.1.0-local-spark4-candidate','spark4.0.1-delta4.0.0-candidate')
        return TargetIdentity('ashlar','ashlar.databricks',version,profile,self.binding_sha256,self.platform,context)
    def read(self,entry:CatalogRead[P,R],parameters:P,*,timeout:timedelta|None=None)->list[R]:
        if not isinstance(entry,EngineRead):raise ResolutionError('Original engine catalog required')
        program=entry.artifact();request=copy.deepcopy(program['request'])
        if program['compiler']!=self.compiler_method or request['target']['bindingSha256']!=self.binding_sha256:
            raise ResolutionError('Original Ashlar binding or compiler differs')
        values=revalidate_model(entry.parameters,parameters)
        request['parameters']={n:_parameter(v)for n,v in values.model_dump(mode='python').items()}
        budget=self._budget(timeout);until=monotonic()+budget.total_seconds()
        original=entry.originals()
        with _preserve_body_failure(self.deadline(budget)):
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
            self.read(entry,entry.parameters.model_validate(values))
    def page(self,entry:CatalogRead[P,R],parameters:P,*,after:ReadContinuation|None=None,timeout:timedelta|None=None)->ReadPage[R]:
        raise ResolutionError('Original engine page program required')
    def stream(self,entry:CatalogRead[P,R],parameters:P,*,max_rows:int,timeout:timedelta|None=None)->Generator[R,None,None]:
        raise ResolutionError('Original bounded engine stream program required')
        yield
