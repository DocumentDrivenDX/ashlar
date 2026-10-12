"""Bounded full-byte file observations; these results grant no runtime authority.

The production caller admits the named operator-trusted interpreter/OS startup
closure before this limited pre-admission bootstrap. No credential execution.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import time

FRAME_BYTES=65536
REPLY_BYTES=16*1024*1024


def file_facts(info):
    return (info.st_dev,info.st_ino,info.st_uid,info.st_mode,info.st_nlink,info.st_size,info.st_mtime_ns)


def _read_file(path,expected,deadline):
    fd=None;primary=None
    try:
        if time.monotonic()>=deadline:raise TimeoutError('File scan deadline exceeded')
        if path.resolve()!=path or path.is_symlink():raise PermissionError('Canonical selected file required')
        named=path.lstat()
        if (file_facts(named)!=expected or not stat.S_ISREG(named.st_mode) or named.st_nlink!=1):
            raise PermissionError('Selected file facts changed')
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        before=os.fstat(fd);digest=hashlib.sha256();count=0
        if file_facts(before)!=expected:raise PermissionError('Selected opened file changed')
        while True:
            if time.monotonic()>=deadline:raise TimeoutError('File scan deadline exceeded')
            block=os.read(fd,min(65536,expected[5]-count+1))
            if not block:break
            count+=len(block)
            if count>expected[5]:raise PermissionError('Selected file grew')
            digest.update(block)
        if (count!=expected[5] or file_facts(os.fstat(fd))!=expected
                or file_facts(path.lstat())!=expected or path.resolve()!=path):
            raise PermissionError('Selected file changed while read')
        return count,digest.hexdigest()
    except BaseException as error:
        primary=error;raise
    finally:
        if fd is not None:
            try:os.close(fd)
            except BaseException:
                if primary is None:raise
                try:setattr(primary,'cleanup_failed',True)
                except BaseException:pass


def _decode(raw):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate scanner field')
            result[key]=value
        return result
    def constant(value):raise ValueError('Nonfinite scanner field')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=constant)


def _worker():
    # A leader remains alive awaiting stdin until the owning parent closes its
    # group. No subprocesses, caller code, payload bytes, or external imports.
    import sys
    while True:
        raw=sys.stdin.buffer.readline(FRAME_BYTES+1)
        if not raw:return
        if len(raw)>FRAME_BYTES or not raw.endswith(b'\n'):raise ValueError('Bounded task frame required')
        task=_decode(raw)
        if type(task)is not dict or set(task)!= {'ordinal','path','facts','deadline'}:raise ValueError('Exact task required')
        ordinal=task['ordinal'];expected=task['facts'];deadline=task['deadline']
        if (type(ordinal)is not int or ordinal<0 or type(task['path'])is not str
                or type(expected)is not list or len(expected)!=7 or any(type(v)is not int for v in expected)
                or type(deadline)is not float or not math.isfinite(deadline)):
            raise ValueError('Exact task fields required')
        count,digest=_read_file(Path(task['path']),tuple(expected),deadline)
        reply=json.dumps({'ordinal':ordinal,'facts':expected,'count':count,'sha256':digest},separators=(',',':')).encode()+b'\n'
        if len(reply)>FRAME_BYTES:raise ValueError('Bounded reply required')
        sys.stdout.buffer.write(reply);sys.stdout.buffer.flush()


def scan_runtime_files(tasks,*,interpreter,deadline,maximum_bytes):
    """Observe exact ordered parent-selected files with four owned processes.

    This lower port does not authenticate tools, select a profile, admit runtime
    permissions, or accept worker code/environment. Production owner must admit
    the genuine fixed bootstrap. The absolute deadline is never reset.
    """
    from .lifecycle import finish
    if (type(tasks)is not tuple or len(tasks)>50000 or type(interpreter)is not type(Path('/'))
            or interpreter.resolve()!=interpreter or interpreter.is_symlink()
            or type(deadline)is not float or not math.isfinite(deadline)
            or type(maximum_bytes)is not int or not 1<=maximum_bytes<=2*1024**3):
        raise ValueError('Exact finite scan selection required')
    total=0
    for task in tasks:
        if (type(task)is not tuple or len(task)!=2 or type(task[0])is not type(Path('/'))
                or not task[0].is_absolute() or type(task[1])is not tuple or len(task[1])!=7
                or any(type(v)is not int for v in task[1]) or task[1][5]<0):
            raise ValueError('Exact selected file facts required')
        total+=task[1][5]
    if total>maximum_bytes:raise PermissionError('File scan byte capacity exceeded')
    if len({str(p)for p,_ in tasks})!=len(tasks):raise ValueError('Distinct original files required')
    if not tasks:return ()
    worker=Path(__file__).resolve()
    bootstrap=tuple((p,file_facts(p.lstat()))for p in (worker,interpreter))
    originals=tuple(_read_file(p,f,deadline)[1]for p,f in bootstrap)
    def bootstrap_current():
        for (p,f),digest in zip(bootstrap,originals):
            if _read_file(p,f,deadline)[1]!=digest:raise PermissionError('Original scanner bootstrap changed')
    def timed():
        if time.monotonic()>=deadline:raise TimeoutError('File scan deadline exceeded')
    processes=[];selector=selectors.DefaultSelector();states={};results=[None]*len(tasks)
    next_task=0;complete=0;reply_total=0;primary=None
    def enqueue(state):
        nonlocal next_task
        if next_task==len(tasks):return
        timed();ordinal=next_task;next_task+=1;p,f=tasks[ordinal]
        body=json.dumps({'ordinal':ordinal,'path':str(p),'facts':f,'deadline':deadline},separators=(',',':')).encode()+b'\n'
        if len(body)>FRAME_BYTES:raise ValueError('Selected task frame capacity exceeded')
        state['ordinal']=ordinal;state['send']=body;state['reply']=bytearray()
        selector.register(state['process'].stdin,selectors.EVENT_WRITE,(state,'send'))
    try:
        for _ in range(4):
            timed()
            process=subprocess.Popen((str(interpreter),'-I','-S','-B',str(worker),'--owned-file-scan'),
                stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                env={'PATH':'/usr/bin:/bin'},start_new_session=True,bufsize=0)
            processes.append(process)
            timed()
            state={'process':process,'ordinal':None,'send':b'','reply':bytearray()}
            states[process.pid]=state
            for pipe in (process.stdin,process.stdout,process.stderr):os.set_blocking(pipe.fileno(),False)
            selector.register(process.stdout,selectors.EVENT_READ,(state,'reply'))
            selector.register(process.stderr,selectors.EVENT_READ,(state,'error'))
        bootstrap_current()
        for state in states.values():enqueue(state)
        while complete<len(tasks):
            timed()
            for key,_ in selector.select(max(0,min(.05,deadline-time.monotonic()))):
                state,kind=key.data;process=state['process'];timed()
                if kind=='send':
                    timed();written=os.write(process.stdin.fileno(),state['send'])
                    state['send']=state['send'][written:]
                    if not state['send']:selector.unregister(process.stdin)
                    continue
                block=os.read(key.fileobj.fileno(),FRAME_BYTES+1)
                if not block:raise RuntimeError('Owned file scanner exited before release')
                if kind=='error':raise RuntimeError('Owned file scanner failure')
                reply_total+=len(block)
                if reply_total>REPLY_BYTES:raise PermissionError('File scan aggregate reply capacity exceeded')
                state['reply'].extend(block)
                if len(state['reply'])>FRAME_BYTES:raise PermissionError('File scan reply frame capacity exceeded')
                if b'\n'not in state['reply']:continue
                raw=bytes(state['reply'])
                if not raw.endswith(b'\n') or raw.count(b'\n')!=1:raise PermissionError('Unexpected scanner reply frames')
                value=_decode(raw);ordinal=state['ordinal']
                if type(value)is not dict or set(value)!= {'ordinal','facts','count','sha256'} or ordinal is None:
                    raise PermissionError('Unsolicited scanner reply')
                p,f=tasks[ordinal]
                if (type(value['ordinal'])is not int or value['ordinal']!=ordinal
                        or type(value['facts'])is not list or any(type(v)is not int for v in value['facts'])
                        or tuple(value['facts'])!=f or type(value['count'])is not int or value['count']!=f[5]
                        or type(value['sha256'])is not str or len(value['sha256'])!=64
                        or any(c not in '0123456789abcdef'for c in value['sha256']) or results[ordinal]is not None
                        or p.resolve()!=p or file_facts(p.lstat())!=f):
                    raise PermissionError('Original file observation correspondence failed')
                results[ordinal]=('file',str(p),f,value['sha256']);complete+=1
                state['ordinal']=None;state['reply']=bytearray();enqueue(state)
        bootstrap_current();timed()
    except BaseException as error:primary=error
    def terminate(process):
        # Leaders have never been polled/reaped: PID/group identity remains owned.
        try:os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:pass
    callbacks=[selector.close]
    callbacks.extend(lambda p=p:terminate(p)for p in processes)
    callbacks.extend(lambda p=p:p.wait(timeout=1)for p in processes)
    def remaining_output(pipe):
        # Killed groups and reaped leaders cannot add successful observations.
        # Any queued late/duplicate/failure output refuses release, not discarded.
        block=os.read(pipe.fileno(),FRAME_BYTES+1)
        if block:raise PermissionError('Unconsumed owned scanner output')
    callbacks.extend(lambda pipe=pipe:remaining_output(pipe)for p in processes for pipe in (p.stdout,p.stderr))
    callbacks.extend(lambda pipe=pipe:pipe.close()for p in processes for pipe in (p.stdin,p.stdout,p.stderr))
    finish(primary,callbacks)
    return tuple(results)


if __name__=='__main__':
    import sys
    if sys.argv[1:]!=['--owned-file-scan']:raise ValueError('Fixed owned scanner entry required')
    _worker()
