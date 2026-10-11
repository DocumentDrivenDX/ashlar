"""Closed installed original supply-chain native invocation; no report authority."""
from dataclasses import dataclass
import json,re
from pathlib import Path
from ashlar.weft_count_star_distribution import CountStarDistributionPaths
from .evolution_cli import InvocationFile
from .lifecycle import finish
from .supply_chain_producer import SupplyChainProducerConfig
from .supply_chain_native import SupplyChainNativeConfig,publish_query_supply_chain_native

PROFILE='ashlar-original-supply-chain-native-invocation/0.1'
CONFIG_LIMIT=65536

def _closed(value,keys):
    if type(value)is not dict or set(value)!=set(keys):raise ValueError('Closed original supply-chain invocation required')

def _path(value):
    if type(value)is not str or not 0<len(value)<=4096 or not Path(value).is_absolute():raise ValueError('Explicit original supply-chain path required')
    selected=Path(value)
    if any(p.is_symlink()for p in (selected,*selected.parents)):raise ValueError('Original cooperating path required')
    return selected

@dataclass(frozen=True)
class SupplyChainNativeInvocation:
    native:SupplyChainNativeConfig
    def __post_init__(self):
        if type(self.native)is not SupplyChainNativeConfig:raise ValueError('Typed original supply-chain configuration required')
        self.native.__post_init__()
        selected=self.native
        for path in (selected.jars,selected.pack,selected.model,selected.graph,selected.producer.source,selected.producer.bun,selected.producer.git,selected.compiler.index,selected.compiler.output):
            if selected.output==path or selected.output in path.parents or path in selected.output.parents:raise ValueError('Separate fresh native output required')
        if any(type(p)is not type(Path('/'))or not p.is_absolute()or any(q.is_symlink()for q in (p,*p.parents))for p in (selected.compiler.index,selected.compiler.output)):
            raise ValueError('Explicit installed compiler paths required')

def parse_supply_chain_native_invocation(raw):
    if type(raw)is not bytes or not 0<len(raw)<=CONFIG_LIMIT:raise ValueError('Bounded original supply-chain configuration required')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate invocation member')
            result[key]=value
        return result
    def integer(token):
        if len(token)>8 or not re.fullmatch('0|[1-9][0-9]*',token):raise ValueError('Canonical bounded invocation integer required')
        return int(token)
    def refuse(token):raise ValueError('Exact invocation scalar required')
    value=json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_float=refuse,parse_constant=refuse)
    _closed(value,('profile','output','jars','pack','model','graph','producer','compiler','operation_capacity'))
    if type(value['profile'])is not str or value['profile']!=PROFILE:raise ValueError('Exact original supply-chain profile required')
    producer=value['producer'];_closed(producer,('source','bun','git','timeout_seconds','maximum_output_bytes','maximum_receipt_bytes'))
    selected=SupplyChainProducerConfig(*(_path(producer[k])for k in ('source','bun','git')),*(producer[k]for k in ('timeout_seconds','maximum_output_bytes','maximum_receipt_bytes')))
    compiler=value['compiler'];_closed(compiler,('index','installation'))
    paths=CountStarDistributionPaths(_path(compiler['index']),_path(compiler['installation']))
    return SupplyChainNativeInvocation(SupplyChainNativeConfig(*(_path(value[k])for k in ('output','jars','pack','model','graph')),selected,paths,value['operation_capacity']))

def run_supply_chain_native_command(configuration):
    selected=InvocationFile.read(configuration,CONFIG_LIMIT)
    invocation=parse_supply_chain_native_invocation(selected.raw);invocation.__post_init__()
    path=selected.path;output=invocation.native.output
    if output==path or output in path.parents or path in output.parents:raise ValueError('Separate invocation/output required')
    primary=None;result=None
    try:
        result=publish_query_supply_chain_native(invocation.native,config_custody=selected)
    except BaseException as error:primary=error
    if primary is not None:finish(primary,[lambda:selected.renew(CONFIG_LIMIT)])
    return result
