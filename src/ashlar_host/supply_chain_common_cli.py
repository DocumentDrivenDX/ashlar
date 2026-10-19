# SPDX-License-Identifier: MIT OR Apache-2.0
"""Selected original common queries over an existing publication; no reingest."""
from dataclasses import dataclass
import json,os,sys
from pathlib import Path
from .evolution_cli import InvocationFile
from .delta_custody import operation_capacity
from .lifecycle import finish,owned_context
from .exact_report import report_body
from .supply_chain_producer import SupplyChainProducerConfig,HeldSupplyChainDataset
from .supply_chain_common_read import SupplyChainCommonReadConfig,SupplyChainCommonRuntime,open_supply_chain_common_read
from .supply_chain_common_query import COMMON_CASE_IDS,DEFAULT_COMMON_CASE_IDS,REPORT_BYTES,SupplyChainCommonQueryConfig,query_supply_chain_common
from .evolution_verification_types import VerificationBounds
from .path_capture import PathCaptureConfig
from ashlar.weft_path_decode import PathDecodeConfig
from ashlar.weft_count_star_distribution import CountStarDistributionPaths

@dataclass(frozen=True)
class CommonInvocation:
    root: Path
    python: Path
    java_home: Path
    jars: Path
    pack: Path
    model: Path
    graph: Path
    producer: SupplyChainProducerConfig
    query: SupplyChainCommonQueryConfig
    capacity: object


def parse_common_configuration(raw):
    def pairs(items):
        result={}
        for k,v in items:
            if k in result:raise ValueError('Duplicate common query configuration key')
            result[k]=v
        return result
    value=json.loads(raw,object_pairs_hook=pairs)
    keys={'profile','publication_directory','python','java_home','jars','pack','model','graph','producer','compiler','operation_capacity'}
    if type(value)is not dict or set(value)!=keys or value['profile']!='ashlar-supply-chain-common-query/0.1':raise ValueError('Closed original common query configuration required')
    def path(value,directory=False):
        if type(value)is not str or not 0<len(value)<=4096:raise ValueError('Explicit canonical runtime/input path required')
        p=Path(value)
        if not p.is_absolute()or p.resolve()!=p or not(p.is_dir()if directory else p.is_file()):raise ValueError('Existing canonical runtime/input path required')
        return p
    producer=value['producer'];pk={'source','bun','git','timeout_seconds','maximum_output_bytes','maximum_receipt_bytes'}
    compiler=value['compiler']
    if type(producer)is not dict or set(producer)!=pk or type(compiler)is not dict or set(compiler)!={'index','installation'}:raise ValueError('Closed actual producer/compiler selections required')
    selected=SupplyChainProducerConfig(path(producer['source'],True),path(producer['bun']),path(producer['git']),*(producer[k]for k in ('timeout_seconds','maximum_output_bytes','maximum_receipt_bytes')))
    query=SupplyChainCommonQueryConfig(CountStarDistributionPaths(path(compiler['index']),path(compiler['installation'],True)),1048576,PathCaptureConfig(128,1048576,16777216),PathDecodeConfig(1048576))
    invocation=CommonInvocation(path(value['publication_directory'],True),path(value['python']),path(value['java_home'],True),path(value['jars'],True),path(value['pack']),path(value['model']),path(value['graph']),selected,query,value['operation_capacity'])
    capacity=operation_capacity(invocation.capacity)
    if capacity is None:raise ValueError('Explicit original operation capacity required')
    if invocation.python!=Path(sys.executable).resolve()or not(invocation.java_home/'bin/java').is_file():raise ValueError('Selected current interpreter and existing JDK executable required')
    return invocation


def run_supply_chain_common_command(configuration,output,*,selected_case_ids=None):
    if selected_case_ids is not None and (type(selected_case_ids)is not tuple or not selected_case_ids or len(set(selected_case_ids))!=len(selected_case_ids)or any(type(c)is not str or c not in COMMON_CASE_IDS for c in selected_case_ids)):
        raise ValueError('Unique original common case selection required')
    original=InvocationFile.read(Path(configuration),65536);config=parse_common_configuration(original.raw);root=Path(output)
    inputs=(original.path,config.root,config.python,config.java_home,config.jars,config.pack,config.model,config.graph,config.producer.source,config.producer.bun,config.producer.git,config.query.distribution.index,config.query.distribution.output)
    if not root.is_absolute()or root.parent.resolve()!=root.parent or not root.parent.is_dir()or root.exists()or root.is_symlink()or any(root==p or root in p.parents or p in root.parents for p in inputs):raise ValueError('Fresh separate common query output directory required')
    root.mkdir(mode=0o700);runtime=root/'runtime';runtime.mkdir(mode=0o700)
    previous={k:os.environ.get(k)for k in ('JAVA_HOME','PYSPARK_PYTHON','PYSPARK_DRIVER_PYTHON')}
    def restore():
        for k,v in previous.items():
            if v is None:os.environ.pop(k,None)
            else:os.environ[k]=v
    def write(path,raw):
        with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb')as stream:
            if stream.write(raw)!=len(raw):raise OSError('Incomplete original query output')
            stream.flush();os.fsync(stream.fileno())
    answer=None;primary=None
    try:
        os.environ.update(JAVA_HOME=str(config.java_home),PYSPARK_PYTHON=sys.executable,PYSPARK_DRIVER_PYTHON=sys.executable)
        producer=HeldSupplyChainDataset(config.producer,config.model,config.graph,root/'source-receipt.json')
        reader=SupplyChainCommonReadConfig(producer.original_invocation(),config.root,config.root/'registry.json',config.root/'operations.sqlite',SupplyChainCommonRuntime(config.python,config.java_home,runtime),config.jars,config.pack,config.capacity,VerificationBounds(262144,128,1048576,16777216,1048576,1048576))
        def observe(ordinal,iteration,request,response):
            write(root/f'query-{ordinal}-{iteration}-request.json',request);write(root/f'query-{ordinal}-{iteration}-response.json',response)
        with owned_context(open_supply_chain_common_read(reader))as opened:
            answer=query_supply_chain_common(opened,config.query,observe=observe,selected_case_ids=selected_case_ids)
    except BaseException as error:primary=error
    finish(primary,[restore,lambda:original.renew(65536)])
    actual=DEFAULT_COMMON_CASE_IDS if selected_case_ids is None else tuple(case for case in COMMON_CASE_IDS if case in selected_case_ids)
    answer={**answer,'original_case_ids':list(DEFAULT_COMMON_CASE_IDS),'available_case_ids':list(COMMON_CASE_IDS),'default_case_ids':list(DEFAULT_COMMON_CASE_IDS),'selected_case_ids':list(actual)}
    write(root/'result.json',report_body(answer,REPORT_BYTES))
    return {'output':str(root/'result.json'),'selected_case_ids':list(actual)}
