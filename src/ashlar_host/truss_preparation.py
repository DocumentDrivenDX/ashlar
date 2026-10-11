"""Bounded effect-free installed preparation owner; portable parent never imports Truss."""
from dataclasses import asdict, fields
import hashlib
import base64
import csv
import io
import email.parser
import importlib.metadata
import json
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import sys
import time
import zipfile
from .lifecycle import finish, owned_context
from .truss_preparation_configuration import (MAX_CONFIG, PinnedFile, TrussPreparationConfig,
    absolute, closed, decode, parse_truss_preparation_configuration)

RESULT_PROFILE='ashlar-truss-preparation-result/0.1'
_SUPERVISOR="""import os,signal,subprocess,sys
fd=int(sys.argv[1])
child=subprocess.Popen(sys.argv[2:],stdin=subprocess.DEVNULL,close_fds=True)
status=child.wait()
os.close(1);os.close(2)
os.write(fd,(str(status)+'\\n').encode('ascii'));os.close(fd)
while True:signal.pause()
"""


def capture_preparation(argv, *, cwd, environment, deadline_seconds, capture_bytes):
    """One selected process group, combined stream bound, primary-safe all-path reap."""
    if (type(argv)is not list or not 1<=len(argv)<=16 or any(type(a)is not str or not a or '\x00'in a or len(a.encode())>4096 for a in argv)
            or not Path(argv[0]).is_absolute() or type(deadline_seconds)is not int or not 0<deadline_seconds<=60
            or type(capture_bytes)is not int or not 0<capture_bytes<=16777216
            or type(environment)is not dict or any(type(k)is not str or type(v)is not str or '\x00'in k or '\x00'in v for k,v in environment.items())or sum(len(k.encode())+len(v.encode())for k,v in environment.items())>65536 or type(cwd)is not type(Path('/'))or not cwd.is_absolute()):
        raise ValueError('Closed bounded preparation capture required')
    process=None;selector=None;streams=[];read_fd=write_fd=None;primary=None
    output=[bytearray(),bytearray(),bytearray()];status=None;deadline=time.monotonic()+deadline_seconds
    try:
        read_fd,write_fd=os.pipe()
        process=subprocess.Popen([argv[0],'-I','-B','-S','-c',_SUPERVISOR,str(write_fd)]+argv,
            cwd=cwd,env=environment,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
            start_new_session=True,pass_fds=(write_fd,))
        streams=[process.stdout,process.stderr]
        os.close(write_fd);write_fd=None
        stream=os.fdopen(read_fd,'rb',buffering=0);read_fd=None;streams.append(stream)
        selector=selectors.DefaultSelector()
        for index,stream in enumerate(streams):
            os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,index)
        while selector.get_map():
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('Preparation deadline exceeded')
            for key,_ in selector.select(min(.05,remaining)):
                remaining_bytes=16-len(output[2])if key.data==2 else capture_bytes-len(output[0])-len(output[1])
                chunk=os.read(key.fileobj.fileno(),min(65536,remaining_bytes+1))
                if not chunk:selector.unregister(key.fileobj);continue
                if len(chunk)>remaining_bytes:raise ValueError('Preparation capture exceeded')
                output[key.data].extend(chunk)
        raw=bytes(output[2]);status=int(raw)
        if raw!=(str(status)+'\n').encode()or not -255<=status<=255:raise ValueError('Complete child terminal required')
    except BaseException as error:primary=error
    callbacks=[]
    if process is not None:
        def kill():
            try:os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
        callbacks.extend((kill,lambda:process.wait(timeout=2)))
    for fd in (read_fd,write_fd):
        if fd is not None:callbacks.append(lambda fd=fd:os.close(fd))
    if selector is not None:callbacks.append(selector.close)
    callbacks.extend(stream.close for stream in streams)
    finish(primary,callbacks)
    if status!=0:raise ValueError('Preparation child refused')
    return bytes(output[0]),bytes(output[1])


def json_bytes(value, maximum):
    """Incrementally enforce encoded capacity before building a complete JSON buffer."""
    total=0;nodes=0
    def charge(size):
        nonlocal total
        total+=size
        if total>maximum:raise ValueError('Preparation representation exceeded')
    def string(value):
        charge(2)
        for char in value:
            n=ord(char)
            if 55296<=n<=57343:raise ValueError('Unicode scalar required')
            charge(2 if char in ('"','\\')or n in (8,9,10,12,13)else 6 if n<32 or 127<n<=65535 else 12 if n>65535 else 1)
    def visit(item,depth=0):
        nonlocal nodes
        nodes+=1
        if depth>12 or nodes>100000:raise ValueError('Bounded preparation metadata required')
        if type(item)is str:string(item)
        elif item is None:charge(4)
        elif type(item)is bool:charge(4 if item else 5)
        elif type(item)is int:
            if item.bit_length()>64:raise ValueError('Bounded metadata integer required')
            charge(len(str(item)))
        elif type(item)is dict:
            charge(2+max(0,len(item)-1))
            for key,value in item.items():
                if type(key)is not str:raise ValueError('Exact metadata key required')
                string(key);charge(1);visit(value,depth+1)
        elif type(item)is list:
            charge(2+max(0,len(item)-1))
            for value in item:visit(value,depth+1)
        else:raise ValueError('Closed preparation metadata required')
    visit(value)
    encoder=json.JSONEncoder(ensure_ascii=True,sort_keys=True,separators=(',',':'),allow_nan=False)
    chunks=[];size=0
    for piece in encoder.iterencode(value):
        if len(piece)>maximum-size:raise ValueError('Preparation representation exceeded')
        data=piece.encode('ascii');size+=len(data)
        if size>maximum:raise ValueError('Preparation representation exceeded')
        chunks.append(data)
    return b''.join(chunks)


def verify_wheel_correspondence(config):
    """Bind selected original wheel payload to current installed Truss bytes, not authority."""
    config.wheel.renew()
    with owned_context(zipfile.ZipFile(config.wheel.path)) as wheel:
        infos=wheel.infolist()
        if len(infos)>8192 or len({i.filename for i in infos})!=len(infos):raise ValueError('Bounded wheel namespace required')
        records=[i for i in infos if i.filename.endswith('.dist-info/RECORD')]
        if len(records)!=1 or records[0].file_size>1048576:raise ValueError('Original bounded wheel RECORD required')
        record_name=records[0].filename
        with owned_context(wheel.open(records[0])) as stream:
            record_raw=stream.read(1048577)
        if len(record_raw)>1048576:raise ValueError('Wheel RECORD capacity exceeded')
        rows=list(csv.reader(io.StringIO(record_raw.decode('utf8'))))
        if len(rows)>8192 or any(len(row)!=3 for row in rows)or len({row[0]for row in rows})!=len(rows):raise ValueError('Closed original wheel RECORD required')
        inventory={i.filename:i for i in infos if not i.is_dir()}
        if set(inventory)!={row[0]for row in rows}or sum(i.file_size for i in inventory.values())>134217728:raise ValueError('Complete bounded wheel RECORD namespace required')
        for name,dig,size in rows:
            if Path(name).is_absolute()or '..'in Path(name).parts:raise ValueError('Wheel path refused')
            info=inventory[name]
            if name==record_name:
                if dig or size:raise ValueError('Original self RECORD required')
                continue
            if not dig.startswith('sha256=')or size!=str(info.file_size):raise ValueError('Original wheel member pin required')
            with owned_context(wheel.open(info)) as stream:
                h=hashlib.sha256();count=0
                while True:
                    chunk=stream.read(min(65536,info.file_size-count+1))
                    if not chunk:break
                    count+=len(chunk)
                    if count>info.file_size:raise ValueError('Wheel member grew')
                    h.update(chunk)
            if count!=info.file_size or base64.urlsafe_b64encode(h.digest()).rstrip(b'=').decode()!=dig[7:]:raise ValueError('Wheel RECORD bytes differ')
        metadata_name=record_name[:-6]+'METADATA'
        if metadata_name not in inventory or inventory[metadata_name].file_size>1048576:raise ValueError('Original wheel distribution metadata required')
        with owned_context(wheel.open(metadata_name)) as stream:metadata_raw=stream.read(1048577)
        metadata=email.parser.BytesParser().parsebytes(metadata_raw)
        if metadata['Name']!=config.distribution or metadata['Version']!=config.version:raise ValueError('Original wheel distribution differs')
        expected={str(f.path.relative_to(config.module_origin.parent.parent)):f for root in config.import_roots for f in root.files if f.path.is_relative_to(config.module_origin.parent)}
        payload={i.filename:i for i in infos if i.filename.startswith('truss/') and not i.is_dir()}
        if set(payload)!=set(expected):raise ValueError('Complete wheel/package payload differs')
        for name,info in payload.items():
            f=expected[name]
            if info.file_size!=f.bytes or info.file_size>67108864 or stat.S_ISLNK(info.external_attr>>16):raise ValueError('Wheel member correspondence refused')
            with owned_context(wheel.open(info)) as stream:
                h=hashlib.sha256();size=0
                while True:
                    chunk=stream.read(min(65536,f.bytes-size+1))
                    if not chunk:break
                    size+=len(chunk)
                    if size>f.bytes:raise ValueError('Wheel member grew')
                    h.update(chunk)
                if size!=f.bytes or h.hexdigest()!=f.sha256:raise ValueError('Installed payload not original wheel bytes')


def prepare_installed_views(config):
    """Private child composition, only public Truss types/functions; no database ports."""
    if type(config)is not TrussPreparationConfig:raise ValueError('Exact preparation config required')
    config.__post_init__();config.renew();verify_wheel_correspondence(config)
    if '.'.join(map(str,sys.version_info[:3]))!=config.python_version or Path(sys.executable).resolve()!=config.python.path:
        raise ValueError('Selected preparation interpreter differs')
    roots=[str(r.path)for r in config.import_roots]
    # -S excludes .pth/site startup; only closed selected roots precede stdlib.
    sys.path[:0]=roots
    from truss import CatalogDocument, DocumentInterpretation, PreparedAcceptance, PreparationProvenance, PreparationRejected, prepare_acceptance
    import truss
    if Path(truss.__file__).resolve()!=config.module_origin:raise ValueError('Selected public Truss origin differs')
    distribution=importlib.metadata.distribution(config.distribution)
    if distribution.version!=config.version or not any(Path(distribution.locate_file('')).resolve()==r.path for r in config.import_roots):
        raise ValueError('Selected Truss distribution differs')
    originals=[d.source.read(config.limits.input_bytes)for d in config.documents]
    configuration=config.configuration.read(config.limits.input_bytes)
    # The upstream request contains configuration hex and document base64. Admit
    # its conservative exact representation expansion before constructing carriers.
    request_bound=2*len(configuration)+sum(4*((len(raw)+2)//3)+12*(len(d.document_id)+len(d.document_revision)+len(d.artifact_identity or d.document_id))+256 for d,raw in zip(config.documents,originals))+1024
    if request_bound>config.limits.input_bytes:raise ValueError('Original request representation exceeded')
    documents=tuple(CatalogDocument(d.document_id,d.document_revision,raw,d.artifact_identity)for d,raw in zip(config.documents,originals))
    primary=None;prepared=None;rejection=None
    try:
        prepared=prepare_acceptance(documents,configuration,bun_executable=str(config.bun.path))
    except PreparationRejected as error:rejection=error
    except BaseException as error:primary=error
    finish(primary,[config.renew])
    if rejection is not None:
        reason=rejection.reason
        if reason not in ('resource','invalid_input','invalid_document','unsupported_profile','producer_unavailable','runtime_unavailable'):raise ValueError('Closed owner refusal required')
        diagnostics=getattr(rejection,'diagnostics',())
        if type(diagnostics)is not tuple or any(type(raw)is not bytes for raw in diagnostics):raise ValueError('Original refusal diagnostics required')
        files=[('original-configuration.json',configuration)]+[('refusal-%04d.json'%i,raw)for i,raw in enumerate(diagnostics)]
        return {'status':'refused','reason':reason,'provenance':{}},files
    if type(prepared)is not PreparedAcceptance or {f.name for f in fields(prepared)}!={'input_bytes','documents','declarations_bytes','archive_documents_bytes','provenance','report_evidence_bytes'} or type(prepared.provenance)is not PreparationProvenance or type(prepared.documents)is not tuple or len(prepared.documents)!=len(documents):raise ValueError('Exact complete public preparation result required')
    files=[('original-configuration.json',configuration)]
    for name,attribute in [('input.json','input_bytes'),('declarations.json','declarations_bytes'),('archive-documents.json','archive_documents_bytes'),('report-evidence.json','report_evidence_bytes')]:
        raw=getattr(prepared,attribute)
        if type(raw)is not bytes:raise ValueError('Complete raw preparation view required')
        files.append((name,raw))
    for i,(original,observed)in enumerate(zip(documents,prepared.documents)):
        if type(observed)is not DocumentInterpretation or observed.document!=original or type(observed.observation_bytes)is not bytes:raise ValueError('Original document observation correspondence required')
        files.extend([('document-%04d.umf.json'%i,original.content),('observation-%04d.json'%i,observed.observation_bytes)])
    if sum(len(raw)for _,raw in files)>config.limits.output_bytes:raise ValueError('Aggregate returned raw views exceeded')
    provenance=asdict(prepared.provenance)
    if provenance.get('scope')!='original_umf_preparation_only' or provenance.get('installation_profiles_verified')is not False or provenance.get('runtime_version')!=config.bun_version:raise ValueError('Preparation-only provenance required')
    return {'status':'prepared','provenance':provenance},files


def emit_preparation_transport(config, metadata, files, stream):
    """Length-framed original bytes, total admitted before serialization/copy."""
    if len(files)>2048 or len({name for name,_ in files})!=len(files) or any(type(raw)is not bytes for _,raw in files):raise ValueError('Closed result files required')
    total=sum(len(raw)for _,raw in files)
    if total>config.limits.output_bytes:raise ValueError('Aggregate raw views exceeded')
    header={**metadata,'profile':RESULT_PROFILE,'files':[{'name':name,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}for name,raw in files]}
    encoded=json_bytes(header,config.limits.receipt_bytes)
    if 8+len(encoded)+total>min(config.limits.capture_bytes,config.limits.output_bytes):raise ValueError('Aggregate transport representation exceeded')
    def write(raw):
        view=memoryview(raw)
        while view:
            n=stream.write(view)
            if type(n)is not int or not 0<n<=len(view):raise ValueError('Complete preparation transport write required')
            view=view[n:]
    write(len(encoded).to_bytes(8,'big'));write(encoded)
    for _,raw in files:write(raw)
    stream.flush()


def parse_preparation_transport(config, raw):
    if type(raw)is not bytes or not 8<len(raw)<=min(config.limits.capture_bytes,config.limits.output_bytes):raise ValueError('Bounded complete preparation transport required')
    size=int.from_bytes(raw[:8],'big')
    if not 0<size<=config.limits.receipt_bytes or size>len(raw)-8:raise ValueError('Bounded header required')
    header=decode(raw[8:8+size],config.limits.receipt_bytes)
    if type(header)is not dict:raise ValueError('Closed result header required')
    required=('status','provenance','profile','files')if header.get('status')=='prepared'else('status','reason','provenance','profile','files')
    closed(header,required)
    if header['profile']!=RESULT_PROFILE or header['status']not in ('prepared','refused')or type(header['files'])is not list or len(header['files'])>2048:raise ValueError('Closed preparation result required')
    files=[];offset=8+size;names=set();total=0
    for item in header['files']:
        closed(item,('name','bytes','sha256'));name=item['name'];length=item['bytes']
        if type(name)is not str or not name or '/'in name or '\\'in name or name in ('.','..','receipt.json')or name in names or type(length)is not int or length<0:raise ValueError('Distinct result files required')
        names.add(name);total+=length
        if total>config.limits.output_bytes or length>len(raw)-offset:raise ValueError('Complete result bound required')
        view=memoryview(raw)[offset:offset+length]
        if hashlib.sha256(view).hexdigest()!=item['sha256']:raise ValueError('Original result bytes differ')
        files.append((name,view));offset+=length
    if offset!=len(raw):raise ValueError('Trailing/missing result bytes refused')
    if header['status']=='prepared':
        expected={'original-configuration.json','input.json','declarations.json','archive-documents.json','report-evidence.json'}|{'%s-%04d.%s'%(kind,i,'umf.json'if kind=='document'else'json')for i in range(len(config.documents))for kind in ('document','observation')}
        if names!=expected or type(header['provenance'])is not dict or header['provenance'].get('scope')!='original_umf_preparation_only' or header['provenance'].get('installation_profiles_verified')is not False:raise ValueError('Complete preparation-only result required')
        by_name=dict(files)
        for i,d in enumerate(config.documents):
            if hashlib.sha256(by_name['document-%04d.umf.json'%i]).hexdigest()!=d.source.sha256 or len(by_name['document-%04d.umf.json'%i])!=d.source.bytes:raise ValueError('Original document bytes differ')
    else:
        if header['reason']not in ('resource','invalid_input','invalid_document','unsupported_profile','producer_unavailable','runtime_unavailable')or header['provenance']!={}or names!=({'original-configuration.json'}|{'refusal-%04d.json'%i for i in range(len(files)-1)}):raise ValueError('Closed original refusal required')
    original_config=dict(files).get('original-configuration.json')
    if original_config is None or len(original_config)!=config.configuration.bytes or hashlib.sha256(original_config).hexdigest()!=config.configuration.sha256:raise ValueError('Original Truss configuration bytes differ')
    return header,files


def run_truss_preparation_command(configuration):
    """Installed parent owns fresh output, finite child and final independent custody."""
    path=absolute(str(configuration));fd=None;primary=None;config=None;parent_fd=output_fd=None;answer=None
    try:
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK);info=os.fstat(fd)
        if not stat.S_ISREG(info.st_mode)or not 0<info.st_size<=MAX_CONFIG:raise ValueError('Bounded configuration file required')
        raw=os.read(fd,info.st_size+1)
        if len(raw)!=info.st_size:raise ValueError('Complete configuration required')
        after=os.fstat(fd);named=path.lstat()
        original=(info.st_dev,info.st_ino,info.st_uid,info.st_mode,info.st_size,info.st_mtime_ns)
        if info.st_nlink!=1 or after.st_nlink!=1 or named.st_nlink!=1 or (after.st_dev,after.st_ino,after.st_uid,after.st_mode,after.st_size,after.st_mtime_ns)!=original or (named.st_dev,named.st_ino,named.st_uid,named.st_mode,named.st_size,named.st_mtime_ns)!=original:raise ValueError('Original command configuration replaced')
        pin=PinnedFile(path,len(raw),hashlib.sha256(raw).hexdigest())
        selected=path.lstat()
        if (selected.st_dev,selected.st_ino,selected.st_uid,selected.st_mode,selected.st_size,selected.st_mtime_ns)!=original:raise ValueError('Original command configuration replaced')
        config=parse_truss_preparation_configuration(raw)
        parent=config.output.parent;parent_fd=os.open(parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW);identity=os.fstat(parent_fd)
        if identity.st_uid!=os.getuid()or stat.S_IMODE(identity.st_mode)!=0o700:raise ValueError('Private owned output parent required')
        def renew_parent():
            current=parent.lstat();held=os.fstat(parent_fd)
            if (current.st_dev,current.st_ino,current.st_uid,current.st_mode)!=(identity.st_dev,identity.st_ino,identity.st_uid,identity.st_mode)or(held.st_dev,held.st_ino)!=(identity.st_dev,identity.st_ino):raise ValueError('Output parent replaced')
        renew_parent();config.renew();verify_wheel_correspondence(config);pin.renew()
        os.mkdir(config.output.name,0o700,dir_fd=parent_fd);created=os.stat(config.output.name,dir_fd=parent_fd,follow_symlinks=False)
        output_fd=os.open(config.output.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent_fd)
        def renew_output():
            renew_parent();held=os.fstat(output_fd);named=os.stat(config.output.name,dir_fd=parent_fd,follow_symlinks=False)
            if (held.st_dev,held.st_ino,held.st_uid,held.st_mode)!=(created.st_dev,created.st_ino,created.st_uid,created.st_mode)or(named.st_dev,named.st_ino)!=(created.st_dev,created.st_ino):raise ValueError('Fresh output replaced')
        renew_output()
        worker=Path(__file__).with_name('_truss_preparation_worker.py').resolve()
        sources=[Path(__file__).resolve(),Path(__file__).with_name('truss_preparation_configuration.py').resolve(),worker,Path(__file__).with_name('lifecycle.py').resolve()]
        worker_pins=[PinnedFile(p,p.stat().st_size,hashlib.sha256(p.read_bytes()).hexdigest())for p in sources]
        environment={'PATH':str(config.bun.path.parent),'LANG':'C.UTF-8','PYTHONDONTWRITEBYTECODE':'1','TMPDIR':str(config.output)}
        argv=[str(config.python.path),'-I','-B','-S',str(worker),str(path),pin.sha256]
        transport,stderr=capture_preparation(argv,cwd=config.output,environment=environment,deadline_seconds=config.limits.deadline_seconds,capture_bytes=config.limits.capture_bytes)
        for p in worker_pins:p.renew()
        header,files=parse_preparation_transport(config,transport)
        # Closing checks are independent and precede any successful evidence release.
        finish(None,[pin.renew,config.renew,renew_output])
        total=len(stderr)+sum(len(data)for _,data in files)
        if total>config.limits.output_bytes:raise ValueError('Aggregate release exceeded')
        inventory=[];retained={}
        def retain(name,data):
            nonlocal total
            handle=None;error=None
            try:
                renew_output();handle=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=output_fd)
                view=memoryview(data)
                while view:
                    n=os.write(handle,view)
                    if n<=0:raise ValueError('Complete output write required')
                    view=view[n:]
                os.fsync(handle)
            except BaseException as exc:error=exc
            finish(error,[lambda:os.close(handle)]if handle is not None else [])
            retained[name]=os.stat(name,dir_fd=output_fd,follow_symlinks=False)
            inventory.append({'name':name,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()})
        for name,data in files:retain(name,data)
        retain('child-stderr.bin',stderr)
        def renew_retained():
            renew_output()
            if set(os.listdir(output_fd))!=set(retained):raise ValueError('Released namespace changed')
            for name,original in retained.items():
                current=os.stat(name,dir_fd=output_fd,follow_symlinks=False)
                if (current.st_dev,current.st_ino,current.st_size,current.st_mtime_ns,current.st_uid,current.st_mode,current.st_nlink)!=(original.st_dev,original.st_ino,original.st_size,original.st_mtime_ns,original.st_uid,original.st_mode,original.st_nlink):raise ValueError('Released original changed')
                handle=None;error=None
                try:
                    handle=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=output_fd)
                    opened=os.fstat(handle);h=hashlib.sha256();size=0
                    if (opened.st_dev,opened.st_ino)!=(original.st_dev,original.st_ino):raise ValueError('Released original substituted')
                    while True:
                        chunk=os.read(handle,min(65536,original.st_size-size+1))
                        if not chunk:break
                        size+=len(chunk)
                        if size>original.st_size:raise ValueError('Released original grew')
                        h.update(chunk)
                    expected=next(i['sha256']for i in inventory if i['name']==name)
                    if size!=original.st_size or h.hexdigest()!=expected:raise ValueError('Released original bytes changed')
                except BaseException as exc:error=exc
                finish(error,[lambda:os.close(handle)]if handle is not None else [])
        renew_retained()
        answer={'profile':RESULT_PROFILE,'status':header['status'],'qualification':'original_umf_preparation_only','configuration_sha256':pin.sha256,'original_truss_configuration_sha256':config.configuration.sha256,'source_revision':config.source_revision,'distribution':config.distribution,'version':config.version,'wheel_sha256':config.wheel.sha256,'provenance':header['provenance'],'files':list(inventory),'closing':True}
        if header['status']=='refused':answer['reason']=header['reason']
        receipt=json_bytes(answer,config.limits.receipt_bytes)
        if total+len(receipt)>config.limits.output_bytes:raise ValueError('Aggregate receipt release exceeded')
        finish(None,[pin.renew,config.renew,renew_retained]);retain('receipt.json',receipt);os.fsync(output_fd)
        finish(None,[pin.renew,config.renew,renew_retained])
        if header['status']=='refused':raise ValueError('Original Truss preparation refused; retained diagnostics are not acceptance')
    except BaseException as error:primary=error
    callbacks=[]
    if config is not None:callbacks.append(config.renew)
    if 'pin'in locals():callbacks.append(pin.renew)
    if output_fd is not None:callbacks.append(renew_retained if 'renew_retained'in locals()else renew_output)
    elif parent_fd is not None:callbacks.append(renew_parent)
    callbacks.extend(lambda fd=fd:os.close(fd)for fd in (fd,output_fd,parent_fd)if fd is not None)
    try:finish(primary,callbacks)
    except BaseException:
        # A positive receipt remains provisional until descriptor cleanup. Remove
        # only our exact inode under our unchanged original directory on failure.
        if config is not None and 'retained'in locals()and 'receipt.json'in retained:
            try:
                named=config.output.lstat();current=(config.output/'receipt.json').lstat();original=retained['receipt.json']
                if (named.st_dev,named.st_ino)==(created.st_dev,created.st_ino)and(current.st_dev,current.st_ino)==(original.st_dev,original.st_ino):
                    os.unlink(config.output/'receipt.json')
            except BaseException:pass
        raise
    return answer
