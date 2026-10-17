"""Installed seven-operation GraphFrames query over an existing supply publication.

This command never publishes or installs dependencies. It independently opens the
actual finite source/native owner and an explicitly selected isolated runtime.
"""
from pathlib import Path
import hashlib,json,os,sys
from .evolution_cli import InvocationFile
from .lifecycle import finish,owned_context
from .supply_chain_native_cli import parse_supply_chain_native_invocation
from .supply_chain_producer import HeldSupplyChainDataset
from .supply_chain_common_read import (SupplyChainCommonReadConfig,SupplyChainCommonRuntime,
    open_supply_chain_common_read)
from .evolution_verification_types import VerificationBounds
from .graphframes_local import (GraphFramesLocalRuntimeSelection,read_local_runtime_plan,
    local_digest,local_encoded,selected_path)
from .graphframes_release import GraphFramesRuntimeConfig,execute_supply_chain_graphframes_release
from .graphframes_isolated_runtime import hold_isolated_graphframes_runtime
from .exact_report import report_body


def _write(path,raw):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);primary=None
    try:
        offset=0
        while offset<len(raw):
            count=os.write(fd,raw[offset:])
            if count<=0:raise OSError('Complete private output required')
            offset+=count
        os.fsync(fd)
    except BaseException as error:primary=error
    finish(primary,[lambda:os.close(fd)])


def run_inspect_graphframes_runtime_command(output,worker_revision,worker_roots,sdk_root,transitive_roots):
    from .graphframes_local_setup import inspect_local_graphframes_runtime
    output=Path(output);selected_path(output)
    if not output.parent.is_dir()or output.exists():raise ValueError('Fresh selected runtime plan required')
    roots=tuple(Path(p)for p in worker_roots)+(Path(sdk_root),)+tuple(Path(p)for p in transitive_roots)
    if any(output==p or p in output.parents or output in p.parents for p in roots):
        raise ValueError('Runtime plan must be outside inspected roots')
    raw=inspect_local_graphframes_runtime(worker_revision,tuple(Path(p)for p in worker_roots),
        Path(sdk_root),tuple(Path(p)for p in transitive_roots))
    _write(output,raw)
    return {'output':str(output),'sha256':local_digest(raw),
        'qualification':'Candidate actual runtime inventory only; separately select its hash before query'}


def run_supply_chain_graphframes_command(configuration,java_home,runtime_plan,runtime_plan_sha256,
        graphframes_java_home,graphframes_jars,output):
    original=InvocationFile.read(Path(configuration),65536)
    native=parse_supply_chain_native_invocation(original.raw).native
    native.__post_init__()
    output=Path(output);java_home=Path(java_home);gf_java=Path(graphframes_java_home);jars=Path(graphframes_jars)
    plan=Path(runtime_plan)
    raw,identity,value,inventory=read_local_runtime_plan(plan,runtime_plan_sha256)
    selection=GraphFramesLocalRuntimeSelection(native.producer.source,plan,runtime_plan_sha256)
    for path in (output,java_home,gf_java,jars):selected_path(path)
    inputs=(original.path,native.output,native.model,native.graph,native.pack,native.producer.source,
        native.producer.bun,native.producer.git,native.jars,java_home,gf_java,jars,plan,
        *(Path(p)for p in value['roots']))
    if (output.exists()or not output.parent.is_dir()or any(output==p or output in p.parents or p in output.parents for p in inputs)):
        raise ValueError('Fresh separate GraphFrames query output required')
    if not native.output.is_dir()or not java_home.is_dir()or not gf_java.is_dir()or not jars.is_dir():
        raise ValueError('Actual existing publication and selected JDK/JAR directories required')
    output.mkdir(mode=0o700);ephemeral=output/'canonical-runtime';ephemeral.mkdir(mode=0o700)
    def descriptors(files):return [{'path':str(f.path),'sha256':f.sha256,'size':f.size}for f in files]
    python=Path(value['python'])
    def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
    runtime=GraphFramesRuntimeConfig(python,sha(python),gf_java,sha(gf_java/'bin/java'),inventory.worker_revision,
        local_digest(local_encoded(descriptors(inventory.worker))),
        local_digest(local_encoded({n:descriptors(files)for n,files in inventory.sdks})),
        jars,output/'engine',300,1048576,16777216)
    previous={key:os.environ.get(key)for key in ('JAVA_HOME','PYSPARK_PYTHON','PYSPARK_DRIVER_PYTHON','SPARK_LOCAL_IP')}
    def restore():
        for key,v in previous.items():
            if v is None:os.environ.pop(key,None)
            else:os.environ[key]=v
    def close_plan():
        current,closing_identity,_,_=read_local_runtime_plan(plan,runtime_plan_sha256)
        if current!=raw or closing_identity!=identity:raise PermissionError('Original selected runtime plan changed')
    answer=None;observations=[];runtime_observations=[];primary=None;observed_bytes=0
    def observe(record):
        nonlocal observed_bytes
        body=report_body(record,16777216);observed_bytes+=len(body)
        if observed_bytes>16777216:raise ValueError('Complete observation budget exceeded')
        observations.append(record)
    try:
        os.environ.update(JAVA_HOME=str(java_home),PYSPARK_PYTHON=sys.executable,
            PYSPARK_DRIVER_PYTHON=sys.executable,SPARK_LOCAL_IP='127.0.0.1')
        producer=HeldSupplyChainDataset(native.producer,native.model,native.graph,output/'public-source-receipt.json')
        config=SupplyChainCommonReadConfig(producer.original_invocation(),native.output,
            native.output/'registry.json',native.output/'operations.sqlite',
            SupplyChainCommonRuntime(Path(sys.executable).resolve(),java_home,ephemeral),
            native.jars,native.pack,native.operation_capacity,
            VerificationBounds(262144,128,1048576,16777216,1048576,1048576))
        with hold_isolated_graphframes_runtime(selection,runtime,output)as runtime_owner:
            with owned_context(open_supply_chain_common_read(config))as opened:
                answer=execute_supply_chain_graphframes_release(opened,runtime,runtime_owner,runtime_owner,observe)
            runtime_observations=runtime_owner.observations
    except BaseException as error:primary=error
    finish(primary,[restore,lambda:original.renew(65536),close_plan])
    body=report_body({'profile':'ashlar-installed-supply-chain-graphframes/0.1','result':answer,
        'observations':observations,'runtime_observations':runtime_observations,
        'qualification':'Seven common operations over actual held finite-file original supply-chain publication; no publication, Weft execution, protectedACK, cloud or authoredGraphFrames scenarios'},16777216)
    _write(output/'report.json',body)
    return {'output':str(output/'report.json'),'common_cases':7}
