"""Current complete runtime custody when the canonical reader uses another SDK.

Actual selected-worker interpreter/import/constructor admission occurs at opening
and closing. Whole selected namespace/file/JAR checks continue during the worker.
Neither a plan nor this runtime owner grants source/publication authority.
"""
from contextlib import contextmanager
from pathlib import Path
import hashlib,json,os
from .graphframes_local import (GraphFramesLocalRuntimeSelection,read_local_runtime_plan,
    runtime_tree,local_original_equal)
from .graphframes_release import GraphFramesRuntimeConfig,capture_graphframes_worker,VERSIONS
from .graphframes_runtime_probe import runtime_bytes
from .puppy_release_cli import original_file
from .lifecycle import finish


class IsolatedGraphFramesRuntimePolicy:
    def __init__(self,selection,runtime,output):
        if type(selection)is not GraphFramesLocalRuntimeSelection or type(runtime)is not GraphFramesRuntimeConfig:
            raise ValueError('Nominal isolated runtime selection required')
        selection.__post_init__();runtime.__post_init__()
        self.selection,self.runtime=selection,runtime
        self.raw,self.identity,self.plan,self.inventory=read_local_runtime_plan(selection.runtime_plan,selection.runtime_plan_sha256)
        protected=(selection.producer_source,)+tuple(Path(p)for p in self.plan['roots'])
        if any(output==p or output in p.parents or p in output.parents for p in protected):
            raise ValueError('Runtime outputs must be separate from source and complete runtime roots')
        self.runtime_raw=runtime_bytes(runtime);self.runtime_path=output/'runtime-selection.json'
        fd=os.open(self.runtime_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);error=None
        try:
            if os.write(fd,self.runtime_raw)!=len(self.runtime_raw):raise OSError('Complete private runtime selection required')
            os.fsync(fd)
        except BaseException as e:error=e
        finish(error,[lambda:os.close(fd)])
        self.original=(selection,runtime,output,self.raw,self.identity,self.inventory,self.runtime_raw,self.runtime_path)
        self.observations=[];self.active=True;self.probes=0

    def renew_graphframes_runtime(self,inventory,runtime,context):
        if (not self.active or context is not self or inventory is not self.inventory or runtime is not self.runtime
                or (self.selection,self.runtime,self.runtime_path.parent,self.raw,self.identity,self.inventory,self.runtime_raw,self.runtime_path)!=self.original):
            raise PermissionError('Original active runtime owner required')
        raw,identity=original_file(self.selection.runtime_plan,16777216)
        if raw!=self.raw or identity!=self.identity or self.runtime_path.read_bytes()!=self.runtime_raw:
            raise PermissionError('Selected runtime plan/input changed')
        if self.plan!=json.loads(self.raw):raise PermissionError('Original runtime roots changed')
        for root,expected in self.plan['roots'].items():
            if not local_original_equal(runtime_tree(Path(root)),expected):
                raise PermissionError('Complete selected runtime namespace/files changed')
        for p in self.plan['absent_import_paths']:
            if Path(p).exists()or Path(p).is_symlink():raise PermissionError('Absent import path appeared')
        if runtime.python!=Path(self.plan['python']):raise PermissionError('Another worker interpreter selected')
        for path in (runtime.java_home,runtime.jars):
            if not any(path==Path(root)or Path(root)in path.parents for root in self.plan['roots']):
                raise PermissionError('Complete selected Java/JAR roots required')
        inventory.verify(runtime);runtime.renew()

    def admit_graphframes_runtime(self,runtime,context):
        self.renew_graphframes_runtime(self.inventory,runtime,context)
        return self.inventory

    def check_worker(self):
        self.renew_graphframes_runtime(self.inventory,self.runtime,self)
        args=[str(self.runtime.python),'-I','-B','-m','ashlar_host.graphframes_runtime_probe',
            '--runtime-plan',str(self.selection.runtime_plan),'--runtime-plan-sha256',self.selection.runtime_plan_sha256,
            '--source',str(self.selection.producer_source),'--runtime',str(self.runtime_path)]
        environment={'PATH':str(self.runtime.java_home/'bin')+':/usr/bin:/bin','JAVA_HOME':str(self.runtime.java_home),
            'PYTHONDONTWRITEBYTECODE':'1','PYTHONUTF8':'1'}
        index=self.probes;self.probes+=1
        def retain(value):
            path=self.runtime_path.parent/('runtime-probe-'+str(index)+'-transport.json')
            body=json.dumps(value,sort_keys=True,separators=(',',':')).encode()
            fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);error=None
            try:
                if os.write(fd,body)!=len(body):raise OSError('Complete private probe capture required')
                os.fsync(fd)
            except BaseException as e:error=e
            finish(error,[lambda:os.close(fd)])
        primary=None;out=None
        try:
            out,err=capture_graphframes_worker(args,cwd=self.runtime_path.parent,environment=environment,
                renew=lambda:self.renew_graphframes_runtime(self.inventory,self.runtime,self),
                timeout_seconds=30,maximum_output_bytes=65536,retain_transport=retain)
            if err:raise PermissionError('Current selected worker probe refused')
            value=json.loads(out)
            expected={'profile':'ashlar-current-isolated-graphframes-runtime/0.1',
                'runtime_sha256':hashlib.sha256(self.runtime_raw).hexdigest(),
                'runtime_plan_sha256':self.selection.runtime_plan_sha256,'python':str(self.runtime.python),
                'worker_revision':self.inventory.worker_revision,'versions':VERSIONS,
                'constructor':'graphframes.classic.graphframe.GraphFrame'}
            if type(value)is not dict or set(value)!=set(expected)|{'numpy_version'}or any(value[k]!=v for k,v in expected.items())or type(value['numpy_version'])is not str or not value['numpy_version']:
                raise PermissionError('Actual selected worker observation differs')
            self.observations.append(value)
        except BaseException as error:primary=error
        finish(primary,[lambda:self.renew_graphframes_runtime(self.inventory,self.runtime,self)])


@contextmanager
def hold_isolated_graphframes_runtime(selection,runtime,output):
    owner=IsolatedGraphFramesRuntimePolicy(selection,runtime,output)
    primary=None
    try:
        owner.check_worker()
        yield owner
    except BaseException as error:primary=error
    try:finish(primary,[owner.check_worker])
    finally:owner.active=False
