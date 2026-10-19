# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fixed installed original17 worker; parent owns whole deadline and control pipe."""
from contextlib import ExitStack
from dataclasses import asdict
import argparse,base64,hashlib,json,os,re,signal,stat
from pathlib import Path
from .evolution_cli import InvocationFile
from .lifecycle import finish,owned_context
from .local_graph_export import open_local_graph_export
from .puppy_finite_local import PuppyFiniteLoopbackConfig,PuppyGremlinLoopbackConfig,PuppyScenarioContainerReceipt
from .puppy_scenario_configuration import parse_puppy_scenario_configuration,with_puppy_scenario_credentials
from .puppy_scenario_sources import open_puppy_scenario_source
from .puppy_scenario_preparation import PuppyScenarioPreparationConfig,prepare_puppy_scenarios
from .puppy_scenario_native import execute_owned_puppy_scenarios

REPORT_LIMIT=16777216

def scenario_report_bytes(value,maximum=REPORT_LIMIT):
    """Explicit SDK/raw-byte carriers; never float or arbitrary integer subclasses."""
    if type(maximum)is not int or not 1<=maximum<=REPORT_LIMIT:raise ValueError('Finite report bound required')
    def convert(item,depth=0):
        if depth>32:raise ValueError('Bounded complete scenario report depth required')
        if item is None or type(item)in (str,bool,int):return item
        if type(item)is bytes:return {'carrier':'original-bytes-base64','bytes_base64':base64.b64encode(item).decode(),'sha256':hashlib.sha256(item).hexdigest()}
        if type(item)is dict:
            if any(type(k)is not str for k in item):raise ValueError('Exact scenario report object keys required')
            return {k:convert(v,depth+1)for k,v in item.items()}
        if type(item)in (list,tuple):return [convert(v,depth+1)for v in item]
        try:
            from gremlin_python.statics import long as graphson_long
        except ImportError:graphson_long=None
        if graphson_long is not None and type(item)is graphson_long and -(2**63)<=item<2**63:
            return {'carrier':'decoded-graphson-g:Int64','integer_token':str(int(item))}
        raise ValueError('Unknown scenario report carrier refused')
    parts=[];size=1
    for text in json.JSONEncoder(ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).iterencode(convert(value)):
        part=text.encode();size+=len(part)
        if size>maximum:raise ValueError('Complete scenario report exceeds bound')
        parts.append(part)
    return b''.join(parts)+b'\n'

def _write(path,raw,maximum):
    if type(raw)is not bytes or not 0<len(raw)<=maximum:raise ValueError('Complete bounded private output required')
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

def run_puppy_scenario_worker(configuration,*,control_fd,invocation_nonce,configuration_sha256):
    if type(control_fd)is not int or control_fd<3 or os.get_blocking(control_fd)or not stat.S_ISFIFO(os.fstat(control_fd).st_mode):raise ValueError('Explicit nonblocking owned creation pipe required')
    if type(invocation_nonce)is not str or re.fullmatch('[0-9a-f]{32}',invocation_nonce)is None or type(configuration_sha256)is not str or re.fullmatch('[0-9a-f]{64}',configuration_sha256)is None:raise ValueError('Exact parent configuration/nonce pins required')
    invocation=InvocationFile.read(Path(configuration),262144)
    selected_file=invocation.path.lstat()
    configuration_parent=invocation.path.parent.lstat()
    if selected_file.st_uid!=os.geteuid()or stat.S_IMODE(selected_file.st_mode)!=0o600 or configuration_parent.st_uid!=os.geteuid()or stat.S_IMODE(configuration_parent.st_mode)!=0o700:raise PermissionError('Current private configuration file required')
    if hashlib.sha256(invocation.raw).hexdigest()!=configuration_sha256:raise PermissionError('Parent selected configuration differs')
    config=parse_puppy_scenario_configuration(invocation.raw);output=config.output
    parent=output.parent;info=parent.lstat()
    if parent.resolve()!=parent or info.st_uid!=os.geteuid()or not stat.S_ISDIR(info.st_mode)or stat.S_IMODE(info.st_mode)!=0o700 or output.exists()or output.is_symlink():raise PermissionError('Fresh output under current private owner required')
    output.mkdir(mode=0o700);identity=output.stat();created=False;answer=None;primary=None
    def renew():
        invocation.renew(262144)
        selected_now=invocation.path.lstat();configuration_parent_now=invocation.path.parent.lstat()
        if (selected_now.st_uid,selected_now.st_mode)!=(selected_file.st_uid,selected_file.st_mode)or (configuration_parent_now.st_dev,configuration_parent_now.st_ino,configuration_parent_now.st_uid,configuration_parent_now.st_mode)!=(configuration_parent.st_dev,configuration_parent.st_ino,configuration_parent.st_uid,configuration_parent.st_mode):raise PermissionError('Original private configuration changed')
        current=parent.lstat();named=output.lstat()
        if (current.st_dev,current.st_ino,current.st_uid,current.st_mode)!=(info.st_dev,info.st_ino,info.st_uid,info.st_mode)or (named.st_dev,named.st_ino,named.st_uid,named.st_mode)!=(identity.st_dev,identity.st_ino,identity.st_uid,identity.st_mode)or output.resolve()!=output:raise PermissionError('Original private worker output changed')
    def creation(receipt):
        nonlocal created
        if created or type(receipt)is not PuppyScenarioContainerReceipt or receipt.invocation_nonce!=invocation_nonce:raise PermissionError('One actual returned creation receipt required')
        r=config.runtime
        if (type(receipt.container_id)is not str or re.fullmatch('[0-9a-f]{64}',receipt.container_id)is None
                or receipt.image!=r.image or receipt.name!=r.name
                or receipt.readonly_mount_source!=output/'prepared/scenarios.duckdb'
                or receipt.readonly_mount_target!='/tmp/scenarios.duckdb'
                or receipt.http_port!=r.http_port or receipt.bolt_port!=r.bolt_port
                or receipt.gremlin_port!=r.gremlin_port or receipt.cpus!=2 or receipt.memory_bytes!=3*1024**3):raise PermissionError('Original creation selections differ')
        payload={**asdict(receipt),'readonly_mount_source':str(receipt.readonly_mount_source)}
        raw=scenario_report_bytes({'type':'created','receipt':payload},4096)
        atomic=os.fpathconf(control_fd,'PC_PIPE_BUF')
        for offset in range(0,len(raw),atomic):
            part=raw[offset:offset+atomic]
            if os.write(control_fd,part)!=len(part):raise OSError('Complete parent creation handoff required')
        created=True
    def failure(value):
        renew();_write(output/'failure.json',scenario_report_bytes(value),REPORT_LIMIT)
    try:
        renew()
        with ExitStack()as owners:
            uses=[];exports=[]
            for files in config.packs:
                export=owners.enter_context(owned_context(open_local_graph_export(files.export)));exports.append(export)
                use=owners.enter_context(owned_context(open_puppy_scenario_source(files,export=export,producer=config.producer,receipt_output=output/(files.name+'-public-dataset.json'))));uses.append(use)
                for role,path in (('model',files.model),('graph',files.graph),('pack',files.pack)):
                    original=InvocationFile.read(path,262144);_write(output/(files.name+'-original-'+role+'.json'),original.raw,262144);original.renew(262144)
            sources=tuple(use.source for use in uses);pack_bytes=tuple(use.pack_bytes for use in uses);exports=tuple(exports)
            preparation=prepare_puppy_scenarios(PuppyScenarioPreparationConfig(pack_bytes,output/'prepared'),sources=sources,exports=exports)
            def execute(username,password):
                r=config.runtime;common=dict(docker=r.docker,docker_host=r.docker_host,image=r.image,name=r.name,prepared=output/'prepared',username=username,password=password,http_port=r.http_port,bolt_port=r.bolt_port,deadline_seconds=config.bounds.native_seconds)
                runtime=PuppyGremlinLoopbackConfig(**common,gremlin_port=r.gremlin_port)if r.protocol=='Gremlin'else PuppyFiniteLoopbackConfig(**common)
                return execute_owned_puppy_scenarios(runtime,pack_bytes=pack_bytes,sources=sources,exports=exports,preparation=preparation,native_map_profile=r.native_map_profile,maximum_rows=config.bounds.maximum_rows,maximum_bytes=config.bounds.maximum_bytes,failure_observer=failure,creation_observer=creation,invocation_nonce=invocation_nonce)
            answer=with_puppy_scenario_credentials(config.credentials,execute)
        if not created:raise PermissionError('Actual created container handoff missing')
        renew()
    except BaseException as error:primary=error
    finish(primary,[renew])
    raw=scenario_report_bytes({'profile':'ashlar-original17-worker-result/0.1','configuration_sha256':configuration_sha256,'invocation_nonce':invocation_nonce,'all_outer_closing_completed':True,'result':answer})
    renew();_write(output/'result.json',raw,REPORT_LIMIT);renew()
    return {'output':str(output/'result.json'),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}

def main():
    parser=argparse.ArgumentParser()
    for key in ('configuration','invocation-nonce','configuration-sha256'):parser.add_argument('--'+key,required=True)
    parser.add_argument('--control-fd',required=True,type=int);args=parser.parse_args()
    def cancel(_signal,_frame):raise KeyboardInterrupt('Owned worker termination')
    signal.signal(signal.SIGTERM,cancel)
    try:
        run_puppy_scenario_worker(args.configuration,control_fd=args.control_fd,invocation_nonce=args.invocation_nonce,configuration_sha256=args.configuration_sha256)
    except BaseException as error:
        print('ashlar original17 worker refused: '+type(error).__name__)
        raise SystemExit(2)from None

if __name__=='__main__':main()
