"""Explicit pinned native mapping observer; no compiler, primary-image or writer grant."""
import errno,hashlib,math,os,selectors,stat,subprocess,time
from pathlib import Path
from .lifecycle import finish
from .medical_commit_inspection_configuration import read_selected
from .medical_producer import decode_medical
from .resources import RESOURCE_ROOT

PROFILE='ashlar-darwin-mapped-vnode-observation/0.1'
FIELDS={'profile','pid','owner_uid','start_seconds','start_microseconds','regions',
    'file_backed_regions','executable_file_regions','no_vnode_regions',
    'selected_vnode_matches','terminal_errno','stable_bsd_identity'}


def mapped_vnode_source():
    """Packaged SDK C source; operators explicitly compile and pin the executable."""
    return RESOURCE_ROOT/'native'/'medical_mapped_vnodes.c'


def pinned_executable_identity(pin):
    """Hash the actual selected original executable and bind its named vnode."""
    pin.__post_init__();fd=None;primary=None;result=None
    try:
        fd=os.open(pin.path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        before=os.fstat(fd);named=pin.path.lstat()
        identity=lambda item:(item.st_dev,item.st_ino,item.st_mode,item.st_size,item.st_mtime_ns,item.st_nlink)
        if (not stat.S_ISREG(before.st_mode)or before.st_nlink!=1 or before.st_size!=pin.bytes
                or identity(before)!=identity(named)):raise PermissionError('Exact original mapped executable required')
        digest=hashlib.sha256();size=0
        while True:
            raw=os.read(fd,min(65536,pin.bytes-size+1))
            if not raw:break
            size+=len(raw)
            if size>pin.bytes:raise PermissionError('Original executable exceeds byte pin')
            digest.update(raw)
        if (size!=pin.bytes or digest.hexdigest()!=pin.sha256
                or identity(os.fstat(fd))!=identity(before)or identity(pin.path.lstat())!=identity(before)):
            raise PermissionError('Original executable bytes or vnode changed')
        result=before.st_dev,before.st_ino
    except BaseException as error:primary=error
    finish(primary,[]if fd is None else[lambda:os.close(fd)])
    return result


def execute_mapped_helper(argv,*,deadline,maximum_output_bytes):
    """Bound aggregate stdout/stderr before buffering; terminate and reap our child."""
    process=None;selector=None;streams=[];output=[bytearray(),bytearray()];primary=None
    try:
        if time.monotonic()>=deadline:raise PermissionError('Mapping observation deadline expired')
        process=subprocess.Popen(argv,env={'LC_ALL':'C','PATH':'/usr/bin:/bin'},cwd=Path(argv[0]).parent,
            stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        streams=[process.stdout,process.stderr];selector=selectors.DefaultSelector()
        for index,stream in enumerate(streams):
            os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,index)
        while selector.get_map():
            remaining=deadline-time.monotonic()
            if remaining<=0:raise PermissionError('Mapping observation deadline exceeded')
            for key,_ in selector.select(min(remaining,.05)):
                size=sum(len(item)for item in output)
                raw=os.read(key.fileobj.fileno(),min(65536,maximum_output_bytes-size+1))
                if not raw:selector.unregister(key.fileobj);continue
                if size+len(raw)>maximum_output_bytes:raise PermissionError('Bounded native mapping output required')
                output[key.data].extend(raw)
        remaining=deadline-time.monotonic()
        if remaining<=0 or process.wait(timeout=remaining)!=0 or output[1]:raise PermissionError('Complete native mapping observation required')
    except BaseException as error:primary=error
    callbacks=[]
    if process is not None:
        def stop():
            if process.poll()is None:process.kill()
        callbacks.extend((stop,lambda:process.wait(timeout=1)))
    if selector is not None:callbacks.append(selector.close)
    callbacks.extend(stream.close for stream in streams)
    finish(primary,callbacks)
    return bytes(output[0])


def observe_mapped_vnodes(selection,pid,identities,*,deadline):
    """Observe selected mapped identities; anonymous memory remains unknown."""
    if type(pid)is not int or not 0<pid<2**31 or not 1<=len(identities)<=33:
        raise ValueError('Bounded exact PID and original executable identity set required')
    if any(type(pair)is not tuple or len(pair)!=2 or any(type(v)is not int or not 0<=v<2**64 for v in pair)for pair in identities):
        raise ValueError('Exact unsigned native device/inode pairs required')
    helper=selection.mapped_vnode_helper;original=read_selected(helper)
    milliseconds=min(5000,math.floor((deadline-time.monotonic())*1000))
    if milliseconds<1:raise PermissionError('Complete mapping observation time required')
    argv=[str(helper.path),str(pid),str(selection.owner_uid),'4096',str(milliseconds)]
    argv.extend(str(value)for pair in identities for value in pair)
    primary=None;raw=None
    try:raw=execute_mapped_helper(argv,deadline=deadline,maximum_output_bytes=selection.maximum_output_bytes)
    except BaseException as error:primary=error
    def renew():
        if read_selected(helper)!=original:raise PermissionError('Original mapped observer executable changed')
    finish(primary,[renew])
    value=decode_medical(raw,min(selection.maximum_output_bytes,4194304))
    validate_mapped_vnode_observation(value,pid=pid,owner_uid=selection.owner_uid)
    if time.monotonic()>=deadline:raise PermissionError('Complete physical process observation deadline exceeded')
    return value


def validate_mapped_vnode_observation(value,*,pid,owner_uid):
    """Validate passive native observation facts, without acquiring authority."""
    if type(value)is not dict or set(value)!=FIELDS or value['profile']!=PROFILE:
        raise PermissionError('Closed native mapped-vnode observation required')
    integers=FIELDS-{'profile','stable_bsd_identity'}
    if any(type(value[key])is not int or value[key]<0 for key in integers):raise PermissionError('Exact native mapping integer carriers required')
    if (value['pid']!=pid or value['owner_uid']!=owner_uid or value['terminal_errno']!=errno.EINVAL
            or value['stable_bsd_identity']is not True or value['selected_vnode_matches']!=0
            or not 0<value['regions']<=4096 or value['file_backed_regions']==0
            or value['file_backed_regions']+value['no_vnode_regions']!=value['regions']
            or value['executable_file_regions']>value['file_backed_regions']or value['start_microseconds']>=1000000):
        raise PermissionError('Complete stable nonmatching mapped-vnode observation required')
    return None
