"""Explicit original finite-pack publication/query invocation and owned output.

The native owner establishes current local source/publication custody. This
configuration and its reports grant no source, writer, retention or ACK authority.
"""
from __future__ import annotations
from dataclasses import dataclass
import hashlib,json,re
from pathlib import Path
from .lifecycle import finish,owned_context
from .evolution_cli import InvocationFile
from .finite_pack import FinitePackDefinition,UMF_REVISION
from .finite_dataset import FiniteDatasetConfig
from .finite_native import FinitePackNativeConfig
from .delta_custody import operation_capacity

PROFILE='ashlar-original-finite-pack-invocation/0.1'
SOURCE_PROFILE='ashlar-original-finite-pack-invocation/0.2'
CONFIG_LIMIT=65536
OUTPUT_LIMIT=64*1024*1024

def _path(value):
    if type(value)is not str or not 0<len(value)<=4096 or not Path(value).is_absolute():raise ValueError('Explicit finite invocation path required')
    p=Path(value)
    if any(q.is_symlink()for q in (p,*p.parents)):raise ValueError('Original cooperating path required')
    return p

def _closed(value,keys):
    if type(value)is not dict or set(value)!=set(keys):raise ValueError('Closed finite invocation required')

def _bound(value,maximum):
    if type(value)is not int or not 0<value<=maximum:raise ValueError('Explicit positive bounded invocation required')
    return value

@dataclass(frozen=True)
class FinitePackInvocation:
    mode:str
    native:FinitePackNativeConfig
    output:Path
    query:dict|None
    profile:str=PROFILE
    def __post_init__(self):
        if type(self.mode)is not str or self.mode not in ('publish','query')or type(self.native)is not FinitePackNativeConfig:raise ValueError('Selected finite invocation required')
        if type(self.profile)is not str or self.profile not in (PROFILE,SOURCE_PROFILE)or (self.profile==SOURCE_PROFILE and self.mode!='query'):raise ValueError('Exact selected finite invocation profile required')
        self.native.__post_init__()
        if type(self.output)is not type(Path('/'))or not self.output.is_absolute():raise ValueError('Explicit output required')
        for p in (self.native.output,self.native.jars,self.native.pack,self.native.model,self.native.graph,self.native.producer.source,self.native.producer.bun,self.native.producer.git):
            if self.output==p or self.output in p.parents or p in self.output.parents:raise ValueError('Separate finite observation output required')
        if self.mode=='publish':
            if self.query is not None:raise ValueError('Publish has no query configuration')
        else:
            _closed(self.query,('index','installation','maximum_artifact_bytes','maximum_rows','maximum_cell_bytes','maximum_total_cell_bytes','maximum_observation_bytes')+(()if self.profile==PROFILE else ('maximum_public_source_bytes',)))
            for key in ('index','installation'):
                if type(self.query[key])is not type(Path('/'))or not self.query[key].is_absolute():raise ValueError('Explicit installed compiler paths required')
                p=self.query[key]
                if self.output==p or self.output in p.parents or p in self.output.parents:raise ValueError('Separate compiler/output paths required')
            for key,maximum in (('maximum_artifact_bytes',16777216),('maximum_rows',1000000),('maximum_cell_bytes',16777216),('maximum_total_cell_bytes',OUTPUT_LIMIT),('maximum_observation_bytes',OUTPUT_LIMIT)):_bound(self.query[key],maximum)
            if self.profile==SOURCE_PROFILE:_bound(self.query['maximum_public_source_bytes'],16777216)
            if self.query['maximum_cell_bytes']>self.query['maximum_total_cell_bytes']:raise ValueError('Cell bound exceeds total')

def parse_finite_pack_invocation(raw,mode):
    if type(raw)is not bytes or not 0<len(raw)<=CONFIG_LIMIT or type(mode)is not str or mode not in ('publish','query'):raise ValueError('Bounded selected configuration required')
    def pairs(items):
        value={}
        for key,item in items:
            if key in value:raise ValueError('Duplicate configuration member')
            value[key]=item
        return value
    def integer(token):
        if len(token)>8 or not re.fullmatch('0|[1-9][0-9]*',token):raise ValueError('Canonical bounded configuration integer required')
        return int(token)
    def refuse(token):raise ValueError('Exact configuration scalar required')
    value=json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_float=refuse,parse_constant=refuse)
    _closed(value,('profile','mode','pack','publication','jars','model','graph','pack_file','producer','operation_capacity','output')+(()if mode=='publish'else ('query',)))
    if value['profile']not in (PROFILE,SOURCE_PROFILE) or value['mode']!=mode or (value['profile']==SOURCE_PROFILE and mode!='query'):raise ValueError('Exact selected finite invocation profile required')
    definition=FinitePackDefinition(value['pack'])
    if definition.name not in ('archaeology','ecology'):raise ValueError('Selected original17 query profile required')
    producer=value['producer'];_closed(producer,('source','bun','git','timeout_seconds','maximum_output_bytes','maximum_receipt_bytes'))
    selected=FiniteDatasetConfig(definition,*(_path(producer[k])for k in ('source','bun','git')),*(producer[k]for k in ('timeout_seconds','maximum_output_bytes','maximum_receipt_bytes')))
    native=FinitePackNativeConfig(*(_path(value[k])for k in ('publication','jars','pack_file','model','graph')),selected,operation_capacity(value['operation_capacity']))
    query=None
    if mode=='query':
        query=value['query'];_closed(query,('index','installation','maximum_artifact_bytes','maximum_rows','maximum_cell_bytes','maximum_total_cell_bytes','maximum_observation_bytes')+(()if value['profile']==PROFILE else ('maximum_public_source_bytes',)))
        query={k:_path(v)if k in ('index','installation')else v for k,v in query.items()}
    return FinitePackInvocation(mode,native,_path(value['output']),query,value['profile'])

def _publish(config):
    from .finite_native import publish_finite_pack_native
    return publish_finite_pack_native(config)

def _open(config,receipt):
    from .finite_native import open_finite_pack_publication
    return open_finite_pack_publication(config,receipt)

def _aliases(driver):
    from .finite_native import finite_pack_query_aliases
    return finite_pack_query_aliases(driver)

def _report_value(value,maximum):
    """Exact selected decoder carriers; no generic dataclass/string coercion."""
    from decimal import Decimal
    from ashlar.weft_decode import ExactScalar
    from ashlar.weft_path_decode import DecodedPaths,PathItem
    from ashlar.weft_related_keys_decode import DecodedRelatedKeys
    remaining=maximum
    def owned(item,depth=0,decoded=False):
        nonlocal remaining
        remaining-=1
        if remaining<0 or depth>64:raise ValueError('Complete report nesting/capacity bound')
        if item is None or type(item)is bool:return item
        if type(item)is str:
            remaining-=len(item.encode('utf-8'))
            if remaining<0:raise ValueError('Complete report payload bound')
            return item
        if type(item)is int:
            if item.bit_length()>512:raise ValueError('Exact report integer bound')
            return {'$type':'integer','text':owned(str(item),depth+1)}if decoded else item
        if type(item)is bytes:
            if len(item)>remaining//2:raise ValueError('Exact original byte report bound')
            return {'$type':'bytes','hex':owned(item.hex(),depth+1)}
        if type(item)is Decimal:
            parts=item.as_tuple()
            if not item.is_finite()or len(parts.digits)>128 or abs(parts.exponent)>128:raise ValueError('Exact report decimal bound')
            return {'$type':'Decimal','text':owned(str(item),depth+1)}
        if type(item)is ExactScalar:
            decoded_value=owned(item.value,depth+1,True)
            return {'$type':'ExactScalar','value':decoded_value,'original':owned(item.original,depth+1)}
        if type(item)is PathItem:return {'$type':'PathItem','intermediate':owned(item.intermediate,depth+1,True),'terminal':owned(item.terminal,depth+1,True),'edges':owned(item.edges,depth+1,True)}
        if type(item)is DecodedPaths:return {'$type':'DecodedPaths','original':owned(item.original,depth+1),'state':owned(item.state,depth+1),'items':owned(item.items,depth+1,True),'truncated':owned(item.truncated,depth+1)}
        if type(item)is DecodedRelatedKeys:return {'$type':'DecodedRelatedKeys','original':owned(item.original,depth+1),'items':owned(item.items,depth+1,True),'truncated':owned(item.truncated,depth+1)}
        if type(item)in (list,tuple):return [owned(v,depth+1,decoded)for v in item]
        if type(item)is dict:
            result={}
            for key,v in item.items():
                if type(key)is not str:raise ValueError('Exact report named member required')
                result[owned(key,depth+1)]=owned(v,depth+1,decoded)
            return result
        raise ValueError('Unknown report carrier refused')
    return owned(value)

def _body(value,maximum):
    body=bytearray()
    value=_report_value(value,maximum)
    for chunk in json.JSONEncoder(ensure_ascii=True,separators=(',',':'),allow_nan=False).iterencode(value):
        raw=chunk.encode()
        if len(body)+len(raw)+1>maximum:raise ValueError('Complete finite output bound')
        body.extend(raw)
    body.extend(b'\n')
    return bytes(body)

def _write(path,value,maximum=OUTPUT_LIMIT):
    body=_body(value,maximum)
    with owned_context(path.open('xb'))as stream:
        if stream.write(body)!=len(body):raise ValueError('Finite output write refused')

def _query(invocation):
    from ashlar.weft_count_star_distribution import CountStarDistributionPaths,installed_count_star_schema_bundle
    from ashlar.weft_path_decode import PathDecodeConfig
    from .pack_count_star_compile import PackCountStarCompileConfig
    from .pack_count_star_query import finite_pack_public_source,query_pack_count_star_cases
    from .count_star_execution import CountStarExecutionConfig
    from .count_star_admission import CountStarAdmissionConfig,SCHEMA_SHA256
    from .count_star_schema import make_offline_count_star_schema_validation
    from .path_capture import PathCaptureConfig
    from .evolution_producer import snapshot
    invocation.__post_init__();q=invocation.query;paths=CountStarDistributionPaths(q['index'],q['installation'])
    bundle=dict(installed_count_star_schema_bundle(paths))
    schema=make_offline_count_star_schema_validation({name:bundle[name]for name in SCHEMA_SHA256})
    compile_config=PackCountStarCompileConfig(paths,q['maximum_artifact_bytes']);pack=snapshot(invocation.native.pack,1048576)
    budget=0
    with owned_context((invocation.output/'observations.jsonl').open('xb'))as log:
        def record(value):
            nonlocal budget
            raw=_body(value,q['maximum_observation_bytes']-budget)
            budget+=len(raw)
            if budget>q['maximum_observation_bytes']:raise ValueError('Complete raw observation bound')
            if log.write(raw)!=len(raw):raise ValueError('Raw observation write refused')
            log.flush()
        def compiler(name,iteration,request,response):
            if any(type(b)is not bytes or len(b)>q['maximum_artifact_bytes']for b in (request,response)):raise ValueError('Original compiler artifact bound')
            record({'kind':'compiler','id':name,'iteration':iteration,'requestHex':request.hex(),'responseHex':response.hex()})
        def native(name,captured):record({'kind':'native','id':name,'capture':captured})
        with owned_context(_open(invocation.native,invocation.output/'public-dataset.json'))as driver:
            execution=CountStarExecutionConfig(CountStarAdmissionConfig(q['maximum_artifact_bytes'],schema),PathCaptureConfig(q['maximum_rows'],q['maximum_cell_bytes'],q['maximum_total_cell_bytes']),PathDecodeConfig(q['maximum_cell_bytes']),finite_pack_public_source(driver.source,q.get('maximum_public_source_bytes',q['maximum_artifact_bytes'])),UMF_REVISION,maximum_public_source_bytes=q.get('maximum_public_source_bytes'))
            result=query_pack_count_star_cases(driver,pack,_aliases(driver),compile_config,execution,observe=compiler,observe_native=native)
    return result

def run_finite_pack_command(configuration,mode):
    """Success follows native cleanup and unchanged original invocation custody."""
    selected=InvocationFile.read(configuration,CONFIG_LIMIT);invocation=parse_finite_pack_invocation(selected.raw,mode)
    invocation.__post_init__();invocation.output.mkdir(mode=0o700);primary=None;result=None
    try:
        if mode=='publish':
            published=_publish(invocation.native)
            result={'publication':str(invocation.native.output),'source':published['source'],'manifest':published['manifest'],'qualification':published['qualification']}
        else:result=_query(invocation)
    except BaseException as error:primary=error
    finish(primary,[lambda:selected.renew(CONFIG_LIMIT)])
    result={**result,'outerCleanupComplete':True,'invocationCustodyComplete':True}
    _write(invocation.output/'report.json',result)
    return {'pack':invocation.native.producer.definition.name,'mode':mode,'report':str(invocation.output/'report.json'),'qualification':result['qualification']}
