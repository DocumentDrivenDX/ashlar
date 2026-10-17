"""Closed installed local lifecycle invocation with one bounded owned worker."""
import argparse
from contextlib import contextmanager,redirect_stdout
from dataclasses import dataclass
import hashlib,json,os,signal,stat,sys
from pathlib import Path
from ashlar.schema import decode_retained_json
from .config import ProducerConfig
from .configured_source_admission import ConfiguredSourceAdmissionConfig
from .configured_local_runtime import ConfiguredLocalRuntimeConfig,initialize_configured_local,publish_configured_local,read_configured_local
from .evolution_cli import InvocationFile
from .evolution_producer import snapshot
from .lifecycle import finish
from .process_capture import capture_process


def _require(value):
    if not value:raise ValueError('Configured local invocation refused')
def _closed(value,keys):
    _require(type(value)is dict and set(value)==set(keys));return value
def _path(value):
    _require(type(value)is str and 0<len(value)<=4096 and '\0'not in value)
    path=Path(value);_require(path.is_absolute()and path.parent.resolve()==path.parent);return path
def _encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()

@dataclass(frozen=True)
class ConfiguredLocalInvocation:
    operation:str
    ordinal:int
    python:Path
    runtime:ConfiguredLocalRuntimeConfig
    whole_seconds:int
    stream_bytes:int
    receipt_bytes:int
    selector:object=None
    def __post_init__(self):
        _require(self.operation in ('initialize','publish','read')and type(self.ordinal)is int and
                 (self.ordinal==0 if self.operation=='initialize'else 1<=self.ordinal<=4))
        _require(self.selector is None if self.operation!='read'else type(self.selector)is tuple and len(self.selector)==2 and all(type(v)is int and -(2**63)<=v<2**63 for v in self.selector))
        _require(type(self.python)is type(Path())and self.python.is_absolute())
        _require(type(self.runtime)is ConfiguredLocalRuntimeConfig);self.runtime.__post_init__()
        _require(self.runtime.installation_sha256 is None if self.operation=='initialize'else self.runtime.installation_sha256 is not None)
        _require(type(self.whole_seconds)is int and 90<=self.whole_seconds<=600 and self.whole_seconds>60+self.runtime.admission.producer.timeout_seconds)
        _require(type(self.stream_bytes)is int and 65536<=self.stream_bytes<=1048576)
        _require(type(self.receipt_bytes)is int and 1024<=self.receipt_bytes<=min(self.stream_bytes,1048576))

def parse_configured_local(raw):
    _require(type(raw)is bytes and len(raw)<=262144)
    value=decode_retained_json(raw);_require(type(value)is dict)
    _closed(value,({'selector'}if value.get('operation')=='read'else set())|{'profile','operation','ordinal','python','installation','installation_id','installation_sha256','output','jars','java_home','source_configuration','producer','stream','materialization_clock_us','operation_capacity','bounds'})
    _require(value['profile']=='ashlar-configured-local/0.1')
    p=_closed(value['producer'],{'source','bun','git','timeout_seconds','maximum_output_bytes','maximum_receipt_bytes'})
    producer=ProducerConfig(_path(p['source']),_path(p['bun']),_path(p['git']),p['timeout_seconds'],p['maximum_output_bytes'],p['maximum_receipt_bytes'])
    bounds=_closed(value['bounds'],{'whole_seconds','stream_bytes','receipt_bytes','maximum_admission_receipt_bytes'})
    output=_path(value['output'])
    admission=ConfiguredSourceAdmissionConfig(_path(value['source_configuration']),producer,output/'admission',bounds['maximum_admission_receipt_bytes'])
    runtime=ConfiguredLocalRuntimeConfig(installation=_path(value['installation']),output=output,jars=_path(value['jars']),java_home=_path(value['java_home']),admission=admission,stream=value['stream'],materialization_clock_us=value['materialization_clock_us'],operation_capacity=value['operation_capacity'],installation_id=value['installation_id'],installation_sha256=value['installation_sha256'])
    selector=None
    if value['operation']=='read':
        selected=_closed(value['selector'],{'type_id','entity_id'});selector=(selected['type_id'],selected['entity_id'])
    result=ConfiguredLocalInvocation(value['operation'],value['ordinal'],_path(value['python']),runtime,bounds['whole_seconds'],bounds['stream_bytes'],bounds['receipt_bytes'],selector);result.__post_init__();return result

def _python_identity(path):
    resolved=path.resolve(strict=True);s=path.stat()
    _require(stat.S_ISREG(s.st_mode)and os.access(path,os.X_OK))
    return str(resolved),s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns

def _result(config,value):
    _require(type(value)is dict and value.get('installation_id')==config.runtime.installation_id and value.get('protected_ack')is False)
    if config.operation=='initialize':
        _closed(value,{'profile','installation_id','operation','installation_sha256','installation','published','protected_ack'})
        _require(value['profile']=='ashlar-configured-local-initialized/0.1'and value['operation']=='initialize'and value['published']is False)
        _require(type(value['installation_sha256'])is str and len(value['installation_sha256'])==64 and all(c in '0123456789abcdef'for c in value['installation_sha256']))
    elif config.operation=='read':
        _closed(value,{'profile','installation_id','operation','manifest','ordinal','local_checkpoint','completed_batches','selector','row','protected_ack'})
        _require(value['profile']=='ashlar-configured-local-read-result/0.1'and value['operation']=='read'and type(value['ordinal'])is int and value['ordinal']==config.ordinal)
        _require(type(value['completed_batches'])is int and config.ordinal<=value['completed_batches']<=4)
        _require(type(value['selector'])is dict and value['selector']=={'type_id':config.selector[0],'entity_id':config.selector[1]}and all(type(v)is int for v in value['selector'].values())and(value['row']is None or type(value['row'])is dict))
    else:
        _closed(value,{'profile','installation_id','operation','manifest','ordinal','local_checkpoint','completed_batches','replayed','native_counts','protected_ack'})
        _require(value['profile']=='ashlar-configured-local-publication-result/0.1'and type(value['ordinal'])is int and value['ordinal']==config.ordinal and type(value['completed_batches'])is int and value['completed_batches']==config.ordinal)
        _require(type(value['replayed'])is bool and value['operation']==('replay'if value['replayed']else'publish'))
    return value

def _write(path,raw):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);primary=None
    try:
        _require(os.write(fd,raw)==len(raw));os.fsync(fd)
    except BaseException as exc:primary=exc
    finish(primary,[lambda:os.close(fd)])

@contextmanager
def _diagnostics():
    sys.stdout.flush();sys.stderr.flush();saved=os.dup(1);primary=None
    try:
        os.dup2(2,1)
        with redirect_stdout(sys.stderr):yield
    except BaseException as exc:primary=exc
    finish(primary,[sys.stderr.flush,lambda:os.dup2(saved,1),lambda:os.close(saved)])

def _worker(path,digest):
    original=InvocationFile.read(path,262144);_require(hashlib.sha256(original.raw).hexdigest()==digest)
    config=parse_configured_local(original.raw);_require(Path(sys.executable)==config.python)
    previous=signal.getsignal(signal.SIGALRM)
    def expired(*_):
        signal.setitimer(signal.ITIMER_REAL,0);raise TimeoutError('configured-local-work-deadline')
    signal.signal(signal.SIGALRM,expired);signal.setitimer(signal.ITIMER_REAL,config.whole_seconds-60)
    def restore():signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,previous)
    primary=None;result=None
    try:
        with _diagnostics():
            if config.operation=='initialize':result=initialize_configured_local(config.runtime)
            elif config.operation=='publish':result=publish_configured_local(config.runtime,ordinal=config.ordinal)
            else:result=read_configured_local(config.runtime,ordinal=config.ordinal,type_id=config.selector[0],entity_id=config.selector[1])
        original.renew(262144)
    except BaseException as exc:primary=exc
    finish(primary,[restore,lambda:original.renew(262144)])
    raw=_encoded(result);_require(len(raw)<=config.receipt_bytes)
    return _encoded({'profile':'ashlar-configured-local-worker/0.1','configuration_sha256':digest,'status':'complete','result':result})

def run_configured_local_command(path):
    original=InvocationFile.read(path,262144);config=parse_configured_local(original.raw)
    _require(not config.runtime.output.exists()and not config.runtime.output.is_symlink())
    parent=config.runtime.output.parent.lstat()
    _require(stat.S_ISDIR(parent.st_mode)and parent.st_uid==os.getuid()and not parent.st_mode&0o022)
    python=_python_identity(config.python)
    environment={'PATH':'/usr/bin:/bin','HOME':os.environ.get('HOME','/nonexistent'),'LANG':'C.UTF-8','LC_ALL':'C.UTF-8',
        'JAVA_HOME':str(config.runtime.java_home),'PYSPARK_PYTHON':str(config.python),'PYSPARK_DRIVER_PYTHON':str(config.python),
        'PYTHONDONTWRITEBYTECODE':'1','SPARK_LOCAL_IP':'127.0.0.1'}
    primary=None;out=None;err=None
    try:
        out,err=capture_process([str(config.python),'-I','-B','-m','ashlar_host.configured_local_cli','--configuration',str(original.path),'--sha256',hashlib.sha256(original.raw).hexdigest()],cwd=original.path.parent,environment=environment,timeout_seconds=config.whole_seconds,maximum_output_bytes=config.stream_bytes)
    except BaseException as exc:primary=exc
    def closing():
        original.renew(262144);_require(_python_identity(config.python)==python)
    finish(primary,[closing])
    _require(type(out)is bytes and len(out)<=config.stream_bytes)
    envelope=_closed(decode_retained_json(out),{'profile','configuration_sha256','status','result'})
    _require(envelope['profile']=='ashlar-configured-local-worker/0.1'and envelope['configuration_sha256']==hashlib.sha256(original.raw).hexdigest())
    if not config.runtime.output.exists():config.runtime.output.mkdir(mode=0o700)
    output_stat=config.runtime.output.lstat();_require(stat.S_ISDIR(output_stat.st_mode)and output_stat.st_uid==os.getuid()and stat.S_IMODE(output_stat.st_mode)==0o700)
    def output_current():
        current=config.runtime.output.lstat()
        _require((current.st_dev,current.st_ino)==(output_stat.st_dev,output_stat.st_ino)and stat.S_ISDIR(current.st_mode)and current.st_uid==os.getuid()and stat.S_IMODE(current.st_mode)==0o700)
    output_current()
    _write(config.runtime.output/'diagnostics.log',err)
    if envelope['status']!='complete':
        _require(envelope['status']=='refused'and envelope['result']is None)
        _write(config.runtime.output/'failure.json',_encoded({'status':'refused','configuration_sha256':envelope['configuration_sha256']}))
        raise ValueError('Configured local worker refused')
    result=_result(config,envelope['result'])
    # The concrete runtime writes correspondence only after its owned closing.
    retained=decode_retained_json(snapshot(config.runtime.output/'runtime-report.json',config.receipt_bytes))
    _require(retained==result);raw=_encoded(result);_require(len(raw)<=config.receipt_bytes)
    closing();output_current();_write(config.runtime.output/'report.json',raw);return result

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--configuration',required=True);parser.add_argument('--sha256',required=True);args=parser.parse_args()
    try:raw=_worker(Path(args.configuration),args.sha256)
    except Exception:
        print('ashlar configured local worker refused',file=sys.stderr)
        raw=_encoded({'profile':'ashlar-configured-local-worker/0.1','configuration_sha256':args.sha256,'status':'refused','result':None})
    sys.stdout.buffer.write(raw);sys.stdout.buffer.flush()

if __name__=='__main__':main()
