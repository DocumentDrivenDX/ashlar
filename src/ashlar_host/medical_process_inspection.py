"""Bounded Darwin process observation; evidence only, never executor fencing."""
import ctypes,os,re,selectors,subprocess,sys,time
from pathlib import Path
from .medical_commit_inspection_configuration import OriginalMedicalCommitInspectionConfig,read_selected
from .original_medical_local_configuration import parse_original_medical_local
from .lifecycle import finish


def physical_executable(pid):
    if sys.platform!='darwin':raise PermissionError('Darwin physical process observation required')
    library=ctypes.CDLL('/usr/lib/libproc.dylib',use_errno=True)
    library.proc_pidpath.argtypes=(ctypes.c_int,ctypes.c_void_p,ctypes.c_uint32)
    library.proc_pidpath.restype=ctypes.c_int
    buffer=ctypes.create_string_buffer(4096)
    count=library.proc_pidpath(pid,buffer,len(buffer))
    if not 0<count<len(buffer):raise PermissionError('Current physical process executable unavailable or raced')
    path=Path(buffer.value.decode('utf-8','strict')).resolve(strict=True)
    observed=path.stat()
    return str(path),(observed.st_dev,observed.st_ino)


def process_rows(selection,deadline):
    """Own only /bin/ps, bound both streams before buffering, and observe exit."""
    process=None;selector=None;primary=None;streams=[];outputs=[bytearray(),bytearray()]
    try:
        process=subprocess.Popen([str(selection.ps.path),'-ww','-axo','pid=,ppid=,pgid=,uid=,lstart=,args='],
            cwd=selection.ps.path.parent,env={'LC_ALL':'C','PATH':'/usr/bin:/bin'},stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        streams=[process.stdout,process.stderr];selector=selectors.DefaultSelector()
        for index,stream in enumerate(streams):
            os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,index)
        while selector.get_map():
            remaining=deadline-time.monotonic()
            if remaining<=0:raise PermissionError('Process observation deadline exceeded')
            for key,_ in selector.select(min(remaining,.05)):
                target=outputs[key.data]
                raw=os.read(key.fileobj.fileno(),min(65536,selection.maximum_output_bytes-len(target)+1))
                if not raw:selector.unregister(key.fileobj);continue
                if sum(len(item)for item in outputs)+len(raw)>selection.maximum_output_bytes:raise PermissionError('Process inventory byte bound exceeded')
                target.extend(raw)
        if process.wait(timeout=max(.01,deadline-time.monotonic()))!=0 or outputs[1]:raise PermissionError('Complete native process inventory required')
    except BaseException as error:primary=error
    callbacks=[]
    if process is not None:
        def stop():
            if process.poll()is None:process.kill()
        callbacks.extend((stop,lambda:process.wait(timeout=1)))
    if selector is not None:callbacks.append(selector.close)
    callbacks.extend(stream.close for stream in streams)
    finish(primary,callbacks)
    return bytes(outputs[0]),process.pid


class MedicalProcessInspection:
    """Captured exact selections plus current OS observations under the actual lease.

    Cooperating development writers and exclusive original storage access remain
    external assumptions. No absence observation prevents an arbitrary future launch.
    """
    def __init__(self,config):
        if type(config)is not OriginalMedicalCommitInspectionConfig:raise ValueError('Exact medical inspection configuration required')
        config.__post_init__();self.config=config;self.pid=os.getpid();self.parent=os.getppid();self.group=os.getpgrp()
        self.pins=(config.process_observer.ps,config.process_observer.java,*config.original_configurations)
        self.raw=tuple(read_selected(pin)for pin in self.pins)
        self.java_identity=self.identity(config.process_observer.java.path)
        self.python_identities=set()
        for raw in self.raw[2:]:
            old=parse_original_medical_local(raw)
            if (old.source!=config.recovery.source or old.installation_id!=config.recovery.installation_id
                    or old.inputs!=config.recovery.inputs or old.root!=config.recovery.root
                    or old.targets!=config.recovery.targets or old.installation!=config.recovery.installation
                    or old.journal!=config.recovery.journal or old.runtime.java!=config.process_observer.java
                    or hasattr(old,'request')and (old.request.sha256,old.request.bytes)!=(config.recovery.request.sha256,config.recovery.request.bytes)):
                raise PermissionError('Retained original execution belongs to another installation or JDK')
            if old.runtime.python.path.exists():self.python_identities.add(self.identity(old.runtime.python.path))
        self.configuration_roots=tuple(sorted({str(pin.path.parent)for pin in config.original_configurations}))
        self.allowed_java=();self.started=False;self.observations=[]
    @staticmethod
    def identity(path):
        observed=path.stat();return observed.st_dev,observed.st_ino
    def renew(self):
        self.config.__post_init__()
        finish(None,[lambda pin=pin,raw=raw:self.same(pin,raw)for pin,raw in zip(self.pins,self.raw)])
    @staticmethod
    def same(pin,raw):
        if read_selected(pin)!=raw:raise PermissionError('Original process selection changed')
    def inventory(self,deadline):
        self.renew();selected=self.config.process_observer
        raw,reaped_ps=process_rows(selected,deadline)
        rows={}
        for line in raw.decode('utf-8','strict').splitlines():
            fields=line.strip().split(None,9)
            if len(fields)!=10 or any(re.fullmatch('[0-9]+',v)is None for v in fields[:3])or re.fullmatch('-?[0-9]+',fields[3])is None:raise PermissionError('Exact complete OS process rows required')
            pid,parent,group,uid=map(int,fields[:4])
            # Darwin ps renders some unsigned uid_t values as signed (nobody=-2).
            if not -(2**31)<=uid<2**32:raise PermissionError('Exact Darwin uid_t process identity required')
            uid%=2**32
            if pid<=0 or pid in rows or len(rows)>=selected.maximum_processes:raise PermissionError('Bounded unique OS process inventory required')
            started=' '.join(fields[4:9])
            if re.fullmatch(r'[A-Z][a-z]{2} [A-Z][a-z]{2} [0-9]{1,2} [0-9]{2}:[0-9]{2}:[0-9]{2} [0-9]{4}',started)is None:
                raise PermissionError('Exact native process start observation required')
            rows[pid]=(parent,group,fields[9],started,uid)
        if self.pid not in rows or rows[self.pid][4]!=selected.owner_uid:raise PermissionError('Actual selected-owner inspector absent from OS observation')
        # This exact child was acquired and reaped by process_rows, not excluded by title.
        rows.pop(reaped_ps,None)
        return rows
    def descendant(self,pid,rows):
        seen=set()
        while pid in rows and pid not in seen:
            if pid==self.pid:return True
            seen.add(pid);pid=rows[pid][0]
        return False
    def observe(self,*,bind=False,before=False):
        deadline=time.monotonic()+self.config.process_observer.timeout_seconds
        def remaining():
            if time.monotonic()>=deadline:raise PermissionError('Complete physical process observation deadline exceeded')
        rows=self.inventory(deadline);relevant=[];java=[];excluded=[];other_uid=0
        owner_uid=self.config.process_observer.owner_uid
        for pid,item in rows.items():
            remaining()
            parent,group,args=item[:3]
            if item[4]!=owner_uid:
                other_uid+=1;continue
            medical=('ashlar_host.original_medical_local_cli'in args or 'ashlar_host.original_medical_local_publication'in args
                or any(name in args for name in ('publish-original-medical','recover-original-medical','replay-original-medical','initialize-original-medical')))
            original_root=any(re.search(r'(?<![A-Za-z0-9_./-])'+re.escape(root)+r'(?:/|(?=$|\s))',args)is not None for root in self.configuration_roots)
            path,identity=physical_executable(pid)
            # Repeat physical observation; an exited/reused PID never proves absence.
            if physical_executable(pid)!=(path,identity):raise PermissionError('Physical process identity raced')
            own_worker=(pid in (self.pid,self.parent)and group==self.group
                and 'ashlar_host.original_medical_local_cli'in args
                and re.search(r'--operation\s+commit-inspection(?:\s|$)',args)is not None)
            if (medical or original_root)and not own_worker:raise PermissionError('Original medical publisher worker or original operator path remains observable')
            if identity==self.java_identity:
                if before or not self.descendant(pid,rows)or group!=self.group:raise PermissionError('Original JDK executor remains outside the controlled inspector')
                java.append((pid,path,identity,parent,group,item[3]if len(item)>3 else None))
            elif identity in self.python_identities and not own_worker:
                raise PermissionError('Original physical Python executor remains observable')
            elif 'java'in Path(path).name.lower():
                excluded.append({'pid':pid,'physical_executable':path,'identity':list(identity)})
            relevant.append((pid,path,identity,parent,group,item[3]if len(item)>3 else None))
        current=tuple(sorted(java))
        if bind:
            if self.started or before or not current:raise PermissionError('One actual fresh inspector JVM binding required')
            self.allowed_java=current;self.started=True
        elif not before and current!=self.allowed_java:
            raise PermissionError('Controlled inspector JVM identity changed')
        # A second bounded inventory catches parent/group/command changes during resolution.
        remaining();closing=self.inventory(deadline)
        for pid,path,identity,parent,group,started in relevant:
            remaining()
            if pid not in closing or closing[pid]!=rows[pid]or physical_executable(pid)!=(path,identity):
                raise PermissionError('Relevant process exited or changed during observation')
        # Every newly appearing same-owner image is unresolved, regardless of title.
        if any(pid not in rows and item[4]==owner_uid for pid,item in closing.items()):
            raise PermissionError('Same-owner process appeared during observation')
        remaining()
        result={'observed_external_original_executors':0,'inspector_java':[item[0]for item in current],
            'physically_distinct_java':excluded,'owner_uid':owner_uid,
            'visibility_profile':self.config.process_observer.visibility_profile,'other_uid_processes_excluded':other_uid,'scope':'Current bounded same-UID Darwin physical-image observation. Other UIDs are outside the selected private development owner profile; full same-UID OS visibility and exclusive original storage access are external assumptions. No SUID/root qualification, future-launch exclusion or runtime grant.'}
        self.observations.append(result);self.renew();return result
    def before(self,lease,context):
        lease.renew(context);result=self.observe(before=True);lease.renew(context);return result
    def bind(self):return self.observe(bind=True)
    def closing(self):return self.observe()
