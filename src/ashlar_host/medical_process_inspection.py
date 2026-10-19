# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded Darwin process observation; evidence only, never executor fencing."""
import ctypes,datetime,errno,os,re,selectors,subprocess,sys,time
from pathlib import Path
from .medical_commit_inspection_configuration import OriginalMedicalCommitInspectionConfig,read_selected,inspection_pins,MAPPED_VISIBILITY,STRICT_VISIBILITY
from .original_medical_local_configuration import parse_original_medical_local
from .lifecycle import finish
from .medical_mapped_vnodes import pinned_executable_identity,observe_mapped_vnodes,validate_mapped_vnode_observation


PROCESS_SCOPE='Current bounded same-UID Darwin physical-image observation. Other UIDs are outside the selected private development owner profile; full same-UID OS visibility and exclusive original storage access are external assumptions. No SUID/root qualification, future-launch exclusion or runtime grant.'
MAPPED_ASSUMPTIONS='Cooperative unmodified original Java/Python executables retain file-backed executable mappings. Anonymous mappings remain unknown. Fallback observations do not establish primary executable identity, arbitrary in-memory/unpinned runtime exclusion or future writer fencing.'

class ProcessImageUnavailable(PermissionError):
    """Kernel/path error retained for the exact ENOENT-only optional fallback."""


def physical_executable(pid):
    if sys.platform!='darwin':raise PermissionError('Darwin physical process observation required')
    library=ctypes.CDLL('/usr/lib/libproc.dylib',use_errno=True)
    library.proc_pidpath.argtypes=(ctypes.c_int,ctypes.c_void_p,ctypes.c_uint32)
    library.proc_pidpath.restype=ctypes.c_int
    buffer=ctypes.create_string_buffer(4096)
    ctypes.set_errno(0)
    count=library.proc_pidpath(pid,buffer,len(buffer))
    if not 0<count<len(buffer):raise ProcessImageUnavailable(ctypes.get_errno()or errno.EIO,'Current physical process executable unavailable or raced')
    try:
        path=Path(buffer.value.decode('utf-8','strict')).resolve(strict=True)
        observed=path.stat()
    except OSError as error:raise ProcessImageUnavailable(error.errno or errno.EIO,'Current physical executable path unavailable')from error
    return str(path),(observed.st_dev,observed.st_ino)


def confirm_local_process_absent(pid):
    """A zero-signal kernel ESRCH observes local termination, not future fencing."""
    try:os.kill(pid,0)
    except ProcessLookupError as error:
        if error.errno==errno.ESRCH:return
        raise
    raise PermissionError('Image lookup ESRCH lacks independent kernel termination observation')


def confirm_local_process_terminal(pid):
    """Require independent image ESRCH and zero-signal ESRCH, never a missing row."""
    try:physical_executable(pid)
    except ProcessImageUnavailable as error:
        if error.errno!=errno.ESRCH:raise
    else:raise PermissionError('Missing process row lacks image lookup terminal custody')
    confirm_local_process_absent(pid)


def process_rows(selection,deadline):
    """Own only /bin/ps, bound both streams before buffering, and observe exit."""
    process=None;selector=None;primary=None;streams=[];outputs=[bytearray(),bytearray()]
    try:
        process=subprocess.Popen([str(selection.ps.path),'-ww','-axo','pid=,ppid=,pgid=,uid=,lstart=,args='],
            cwd=selection.ps.path.parent,env={'LC_ALL':'C','PATH':'/usr/bin:/bin','TZ':'UTC'},stdin=subprocess.DEVNULL,
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
        self.pins=inspection_pins(config)
        self.raw=tuple(read_selected(pin)for pin in self.pins)
        self.java_identity=self.identity(config.process_observer.java.path)
        self.python_identities=set();self.mapped_executables=[]
        selected_pythons={}
        for raw in self.raw[2:2+len(config.original_configurations)]:
            old=parse_original_medical_local(raw)
            if (old.source!=config.recovery.source or old.installation_id!=config.recovery.installation_id
                    or old.inputs!=config.recovery.inputs or old.root!=config.recovery.root
                    or old.targets!=config.recovery.targets or old.installation!=config.recovery.installation
                    or old.journal!=config.recovery.journal or old.runtime.java!=config.process_observer.java
                    or hasattr(old,'request')and (old.request.sha256,old.request.bytes)!=(config.recovery.request.sha256,config.recovery.request.bytes)):
                raise PermissionError('Retained original execution belongs to another installation or JDK')
            if old.runtime.python.path.exists():self.python_identities.add(self.identity(old.runtime.python.path))
            if old.runtime.python.path in selected_pythons and selected_pythons[old.runtime.python.path]!=old.runtime.python:
                raise PermissionError('Conflicting original Python executable pin')
            selected_pythons[old.runtime.python.path]=old.runtime.python
        if config.process_observer.visibility_profile==MAPPED_VISIBILITY:
            for pin in (config.process_observer.java,*selected_pythons.values()):
                self.mapped_executables.append((pin,pinned_executable_identity(pin)))
        self.configuration_roots=tuple(sorted({str(pin.path.parent)for pin in config.original_configurations}))
        self.allowed_java=();self.started=False;self.observations=[]
    @staticmethod
    def identity(path):
        observed=path.stat();return observed.st_dev,observed.st_ino
    def renew(self):
        self.config.__post_init__()
        finish(None,[lambda pin=pin,raw=raw:self.same(pin,raw)for pin,raw in zip(self.pins,self.raw)]+
            [lambda pin=pin,identity=identity:self.same_executable(pin,identity)for pin,identity in self.mapped_executables])
    @staticmethod
    def same_executable(pin,identity):
        if pinned_executable_identity(pin)!=identity:raise PermissionError('Original mapped executable vnode changed')
    def mapped(self,pid,row,deadline):
        self.renew()
        identities=tuple(sorted({identity for _,identity in self.mapped_executables}))
        observed=observe_mapped_vnodes(self.config.process_observer,pid,identities,deadline=deadline)
        started=datetime.datetime.fromtimestamp(observed['start_seconds'],datetime.timezone.utc)
        days=('Mon','Tue','Wed','Thu','Fri','Sat','Sun');months=('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')
        expected=f'{days[started.weekday()]} {months[started.month-1]} {started.day} {started.hour:02}:{started.minute:02}:{started.second:02} {started.year}'
        if observed['owner_uid']!=row[4]or expected!=row[3]:raise PermissionError('Mapped kernel process differs from original PS row')
        self.renew();return observed
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
        owner_uid=self.config.process_observer.owner_uid;records={}
        def terminal(pid,row):
            confirm_local_process_terminal(pid)
            return {'kind':'terminal','row':row,'fact':{'pid':pid,'owner_uid':owner_uid,'opening_kernel_errno':errno.ESRCH}}
        def observe_one(pid,row,previous=None):
            remaining()
            if pid not in records and len(records)>=self.config.process_observer.maximum_processes:raise PermissionError('Cumulative same-owner process identity bound exceeded')
            parent,group,args=row[:3]
            medical=('ashlar_host.original_medical_local_cli'in args or 'ashlar_host.original_medical_local_publication'in args
                or any(name in args for name in ('publish-original-medical','recover-original-medical','replay-original-medical','initialize-original-medical')))
            original_root=any(re.search(r'(?<![A-Za-z0-9_./-])'+re.escape(root)+r'(?:/|(?=$|\s))',args)is not None for root in self.configuration_roots)
            try:
                try:path,identity=physical_executable(pid)
                except ProcessImageUnavailable as error:
                    if error.errno==errno.ESRCH:raise
                    if error.errno!=errno.ENOENT or self.config.process_observer.visibility_profile!=MAPPED_VISIBILITY:raise
                    observed=self.mapped(pid,row,deadline);path=None
                    identity=(observed['owner_uid'],observed['start_seconds'],observed['start_microseconds'])
                else:
                    if physical_executable(pid)!=(path,identity):raise PermissionError('Physical process identity raced')
            except ProcessImageUnavailable as error:
                if error.errno!=errno.ESRCH:raise
                records[pid]=terminal(pid,row);return
            if previous is not None:
                if (previous['kind']!=('mapped'if path is None else'primary')
                        or previous['path']!=path or previous['identity']!=identity):
                    raise PermissionError('Auxiliary argument change crossed original physical process identity')
            own_worker=(pid in (self.pid,self.parent)and group==self.group
                and 'ashlar_host.original_medical_local_cli'in args
                and re.search(r'--operation\s+(?:commit-inspection|commit-reconciliation)(?:\s|$)',args)is not None)
            if (medical or original_root)and not own_worker:raise PermissionError('Original medical publisher worker or original operator path remains observable')
            if identity==self.java_identity:
                # Actual current inspector process lineage is the only selected-Java exception.
                if before or not self.descendant(pid,current_rows)or group!=self.group:raise PermissionError('Original JDK executor remains outside the controlled inspector')
            elif identity in self.python_identities and not own_worker:
                raise PermissionError('Original physical Python executor remains observable')
            record={'kind':'mapped'if path is None else'primary','row':row,'path':path,'identity':identity}
            if path is None:record['fact']={'opening':observed if previous is None else previous['fact']['opening']}
            records[pid]=record
        current_rows=self.inventory(deadline)
        for pid,row in current_rows.items():
            if row[4]==owner_uid:observe_one(pid,row)
        while True:
            remaining();closing=self.inventory(deadline);needs_closing=False
            for pid,record in list(records.items()):
                remaining();row=closing.get(pid)
                if record['kind']=='terminal':
                    if row is not None:raise PermissionError('Exited local process PID reappeared during observation')
                    confirm_local_process_terminal(pid);record['fact']['closing_kernel_errno']=errno.ESRCH;continue
                if row is None:
                    records[pid]=terminal(pid,record['row']);needs_closing=True;continue
                if any(row[index]!=record['row'][index]for index in (0,1,3,4)):
                    raise PermissionError('Relevant process identity changed during observation')
                if row[2]!=record['row'][2]:
                    current_rows=closing
                    observe_one(pid,row,previous=record);needs_closing=True;continue
                if record['kind']=='mapped':
                    observed=self.mapped(pid,row,deadline)
                    if record['identity']!=(observed['owner_uid'],observed['start_seconds'],observed['start_microseconds']):
                        raise PermissionError('Mapped process was replaced during observation')
                    record['fact']['closing']=observed
                else:
                    try:current=physical_executable(pid)
                    except ProcessImageUnavailable as error:
                        if error.errno!=errno.ESRCH:raise
                        records[pid]=terminal(pid,row);needs_closing=True;continue
                    if current!=(record['path'],record['identity']):raise PermissionError('Physical process identity changed during closing')
            new=[(pid,row)for pid,row in closing.items()if row[4]==owner_uid and pid not in records]
            if new:
                current_rows=closing
                for pid,row in new:observe_one(pid,row)
                needs_closing=True
            if not needs_closing:break
        remaining()
        java=tuple(sorted((pid,record['path'],record['identity'],record['row'][0],record['row'][1],record['row'][3])
            for pid,record in records.items()if record['kind']=='primary'and record['identity']==self.java_identity))
        if bind:
            if self.started or before or not java:raise PermissionError('One actual fresh inspector JVM binding required')
            self.allowed_java=java;self.started=True
        elif not before and java!=self.allowed_java:raise PermissionError('Controlled inspector JVM identity changed')
        excluded=[{'pid':pid,'physical_executable':record['path'],'identity':list(record['identity'])}
            for pid,record in records.items()if record['kind']=='primary'and record['identity']!=self.java_identity
            and 'java'in Path(record['path']).name.lower()]
        result={'observed_external_original_executors':0,'inspector_java':[item[0]for item in java],
            'physically_distinct_java':excluded,'mapped_vnode_fallbacks':[record['fact']for record in records.values()if record['kind']=='mapped'],
            'terminal_local_processes':[record['fact']for record in records.values()if record['kind']=='terminal'],
            'owner_uid':owner_uid,'visibility_profile':self.config.process_observer.visibility_profile,
            'other_uid_processes_excluded':sum(row[4]!=owner_uid for row in closing.values()),'scope':PROCESS_SCOPE}
        if self.config.process_observer.visibility_profile==MAPPED_VISIBILITY:result['mapped_vnode_assumptions']=MAPPED_ASSUMPTIONS
        self.observations.append(result);self.renew();return result
    def before(self,lease,context):
        lease.renew(context);result=self.observe(before=True);lease.renew(context);return result
    def bind(self):return self.observe(bind=True)
    def closing(self):return self.observe()


def validate_medical_process_observations(observations):
    """Validate retained exact process facts; this grants no current authority.

    Current owners must still acquire their own lease, source and kernel observations.
    """
    if type(observations)is not list or not 1<=len(observations)<=8:
        raise ValueError('Bounded complete medical process observation sequence required')
    common={'observed_external_original_executors','inspector_java','physically_distinct_java',
        'mapped_vnode_fallbacks','terminal_local_processes','owner_uid','visibility_profile',
        'other_uid_processes_excluded','scope'}
    def integer(value,maximum):return type(value)is int and 0<=value<=maximum
    def pid(value):return integer(value,2**31-1)and value>0
    def bounded(value):return type(value)is list and len(value)<=4096
    selected_uid=None;selected_profile=None
    for value in observations:
        if type(value)is not dict:raise ValueError('Exact medical process facts required')
        profile=value.get('visibility_profile');wanted=common|({'mapped_vnode_assumptions'}if profile==MAPPED_VISIBILITY else set())
        if (profile not in (STRICT_VISIBILITY,MAPPED_VISIBILITY)or set(value)!=wanted
                or not integer(value['owner_uid'],2**32-1)
                or type(value['observed_external_original_executors'])is not int or value['observed_external_original_executors']!=0
                or not integer(value['other_uid_processes_excluded'],4096)or value['scope']!=PROCESS_SCOPE):
            raise ValueError('Closed qualified medical process observation required')
        if selected_uid is None:selected_uid=value['owner_uid'];selected_profile=profile
        if (selected_uid,selected_profile)!=(value['owner_uid'],profile):raise ValueError('Original process observation owner/profile differs')
        seen=set()
        if not bounded(value['inspector_java'])or any(not pid(item)for item in value['inspector_java']):raise ValueError('Bounded inspector PID inventory required')
        for item in value['inspector_java']:
            if item in seen:raise ValueError('Duplicate process identity facts')
            seen.add(item)
        if not bounded(value['physically_distinct_java']):raise ValueError('Bounded distinct Java identity facts required')
        for item in value['physically_distinct_java']:
            if (type(item)is not dict or set(item)!={'pid','physical_executable','identity'}or not pid(item['pid'])
                    or type(item['physical_executable'])is not str or not item['physical_executable'].startswith('/')
                    or not 0<len(item['physical_executable'].encode())<=4096 or any(ord(c)<32 for c in item['physical_executable'])
                    or type(item['identity'])is not list or len(item['identity'])!=2
                    or any(not integer(part,2**64-1)for part in item['identity'])or item['pid']in seen):
                raise ValueError('Exact physical Java identity facts required')
            seen.add(item['pid'])
        if not bounded(value['mapped_vnode_fallbacks'])or (profile==STRICT_VISIBILITY and value['mapped_vnode_fallbacks']):raise ValueError('Profile-bound mapping observations required')
        if profile==MAPPED_VISIBILITY and value['mapped_vnode_assumptions']!=MAPPED_ASSUMPTIONS:raise ValueError('Exact cooperative mapping assumptions required')
        for item in value['mapped_vnode_fallbacks']:
            if type(item)is not dict or set(item)!={'opening','closing'}:raise ValueError('Paired mapped observation facts required')
            opening=item['opening'];closing=item['closing']
            if type(opening)is not dict or not pid(opening.get('pid')):raise ValueError('Exact mapped PID required')
            for observed in (opening,closing):validate_mapped_vnode_observation(observed,pid=opening['pid'],owner_uid=selected_uid)
            if any(opening[key]!=closing[key]for key in ('pid','owner_uid','start_seconds','start_microseconds'))or opening['pid']in seen:
                raise ValueError('Original mapped process identity changed')
            seen.add(opening['pid'])
        if not bounded(value['terminal_local_processes']):raise ValueError('Bounded kernel terminal observations required')
        for item in value['terminal_local_processes']:
            if (type(item)is not dict or set(item)!={'pid','owner_uid','opening_kernel_errno','closing_kernel_errno'}
                    or not pid(item['pid'])or type(item['owner_uid'])is not int or item['owner_uid']!=selected_uid
                    or type(item['opening_kernel_errno'])is not int or item['opening_kernel_errno']!=errno.ESRCH
                    or type(item['closing_kernel_errno'])is not int or item['closing_kernel_errno']!=errno.ESRCH or item['pid']in seen):
                raise ValueError('Exact repeated local kernel terminal facts required')
            seen.add(item['pid'])
    return None
