"""Bounded selected-runtime correspondence, never native/source authority."""
from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import stat
import sys
from .config import ProducerConfig
from .original_medical_local_configuration import OriginalMedicalRuntime,OriginalMedicalInputs,OriginalMedicalBounds
from .lifecycle import finish

# Explicit SDK/JVM correspondence, not ambient defaults. Timeout getter uses seconds.
NATIVE_PYTHON_SETTINGS=(
    ('spark.python.authenticate.socketTimeout','15s','SPARK_AUTH_SOCKET_TIMEOUT','15'),
    ('spark.buffer.size','65536','SPARK_BUFFER_SIZE','65536'),
)

PROFILE='ashlar-original-medical-runtime-selection/0.1'
REFERENCE_BYTES=8388608
REFERENCE_TOTAL=33554432
RECORDS=20000
SELECTED_FILES=2048
SELECTED_BYTES=134217728
# Immutable independently reviewed native evidence anchors, not asserted statuses.
NATIVE_EVIDENCE={('4.0.1','4.0.0'):('ashlar-supply-chain-installed-native-evidence/0.1','11cc06e98071eac6ef56c7665d566f4d22f6a54419703e63538a52916c971f23'),('3.5.3','3.2.1'):('ashlar-original-commerce-native-evidence/0.1','cf147a1ecabb14b9f975e38f82aff232b58874cb05fa6a5707e8a582385050c5')}


def _exact(value,keys):
    if type(value)is not dict or set(value)!=set(keys):raise ValueError('Closed medical runtime record required')


def _integer(value,maximum,minimum=0):
    if type(value)is not int or not minimum<=value<=maximum:raise ValueError('Bounded exact medical runtime integer required')


def _path(value):
    if type(value)is not str or not value or len(value.encode())>4096 or '\x00'in value:raise ValueError('Explicit medical runtime path required')
    path=Path(value)
    if not path.is_absolute():raise ValueError('Absolute medical runtime path required')
    return path


def _pin(value,maximum):
    _exact(value,('path','bytes','sha256'));_path(value['path']);_integer(value['bytes'],maximum)
    if type(value['sha256'])is not str or re.fullmatch('[0-9a-f]{64}',value['sha256'])is None:raise ValueError('Exact medical runtime digest required')
    return dict(value)


def _same(left,right):
    if type(left)is not type(right):return False
    if type(left)is dict:return set(left)==set(right)and all(_same(left[k],right[k])for k in left)
    if type(left)is list:return len(left)==len(right)and all(_same(a,b)for a,b in zip(left,right))
    return left==right

def _selection(pin):return {'path':str(pin.path),'bytes':pin.bytes,'sha256':pin.sha256}


def _decode(raw):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate medical runtime member')
            result[key]=value
        return result
    def nonfinite(_):raise ValueError('Nonfinite medical runtime value')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=nonfinite)


class _File:
    def __init__(self,pin):
        self.pin=pin;self.path=_path(pin['path']);self.fd=None;self.original=None
    def acquire(self):
        if self.path.resolve()!=self.path:raise PermissionError('Canonical selected medical file required')
        self.fd=os.open(self.path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        held=os.fstat(self.fd)
        if not stat.S_ISREG(held.st_mode):raise PermissionError('Regular selected medical file required')
        self.original=(held.st_dev,held.st_ino,held.st_uid,held.st_mode,held.st_nlink)
        return self.read()
    def read(self):
        return self._scan(collect=True)
    def verify(self):
        self._scan(collect=False)
    def _scan(self,*,collect):
        if self.fd is None:raise PermissionError('Held medical runtime file required')
        if self.path.resolve()!=self.path:raise PermissionError('Canonical selected medical file changed')
        before=os.fstat(self.fd);named=self.path.lstat()
        facts=lambda s:(s.st_dev,s.st_ino,s.st_uid,s.st_mode,s.st_nlink)
        if facts(before)!=self.original or facts(named)!=self.original or not stat.S_ISREG(named.st_mode)or before.st_size!=self.pin['bytes']:raise PermissionError('Selected medical file custody changed')
        os.lseek(self.fd,0,os.SEEK_SET);parts=[] if collect else None;count=0;digest=hashlib.sha256()
        while True:
            block=os.read(self.fd,min(65536,self.pin['bytes']-count+1))
            if not block:break
            count+=len(block)
            if count>self.pin['bytes']:raise PermissionError('Selected medical file bound exceeded')
            if collect:parts.append(block)
            digest.update(block)
        after=os.fstat(self.fd);named=self.path.lstat()
        if self.path.resolve()!=self.path:raise PermissionError('Canonical selected medical file changed')
        if facts(after)!=self.original or facts(named)!=self.original or (after.st_size,after.st_mtime_ns)!=(before.st_size,before.st_mtime_ns)or count!=self.pin['bytes']or digest.hexdigest()!=self.pin['sha256']:raise PermissionError('Selected medical file bytes changed')
        return b''.join(parts) if collect else None
    def close(self):
        if self.fd is not None:
            fd=self.fd;self.fd=None;os.close(fd)


class OriginalMedicalRuntimeCustody:
    """Finite original files/descriptors and explicit import-only observations.

    Selected qualification references retain evidence, not a self-issued success
    grant. Actual Spark session/version/JAR checks belong to the native owner.
    """
    def __init__(self,runtime,inputs,bounds,producer):
        self.runtime=runtime;self.inputs=inputs;self.bounds=bounds;self.producer=producer
        self.files=[];self.by_path={};self.descriptors={};self.import_acquired=False;self.closed=False;self.plan=None
    def _acquire(self,pin):
        key=pin['path']
        if key in self.by_path:
            owner=self.by_path[key]
            if owner.pin!=pin:raise ValueError('Conflicting medical selected file')
            return owner.read()
        owner=_File(pin);self.files.append(owner);self.by_path[key]=owner
        return owner.acquire()
    def _configuration(self):
        return json.dumps([asdict(value)for value in (self.runtime,self.inputs,self.bounds,self.producer)],default=str,sort_keys=True,separators=(',',':'),allow_nan=False)
    def _renew_configuration(self):
        for value in (self.runtime,self.inputs,self.bounds,self.producer):value.__post_init__()
        if self._configuration()!=self.original_configuration:raise PermissionError('Original medical runtime configuration changed')
    def _open(self):
        for value,expected in ((self.runtime,OriginalMedicalRuntime),(self.inputs,OriginalMedicalInputs),(self.bounds,OriginalMedicalBounds),(self.producer,ProducerConfig)):
            if type(value)is not expected:raise ValueError('Exact medical runtime configuration owners required')
            value.__post_init__()
        self.original_configuration=self._configuration()
        inputs=(self.inputs.model,self.inputs.graph,self.inputs.baseline)
        if sum(pin.bytes for pin in inputs)>self.bounds.input_bytes:raise ValueError('Medical input aggregate exceeded')
        self.input_bytes=tuple(self._acquire(_selection(pin))for pin in inputs)
        raw=self._acquire(_selection(self.runtime.qualification));plan=_decode(raw)
        _exact(plan,('profile','runtime','imports','selected_files','namespaces','evidence','scope'))
        if plan['profile']!=PROFILE or plan['scope']!='selected-runtime-correspondence-only':raise ValueError('Exact medical runtime selection profile required')
        runtime={name:(_selection(value)if hasattr(value,'sha256')else str(value)if type(value)is type(Path())else [_selection(pin)for pin in value]if name=='jars'else value)for name,value in vars(self.runtime).items()if name!='qualification'}
        if not _same(plan['runtime'],runtime):raise PermissionError('Complete selected medical runtime differs')
        if (self.runtime.spark_version,self.runtime.delta_version)not in (('4.0.1','4.0.0'),('3.5.3','3.2.1')):raise ValueError('Explicit qualified medical SDK profile required')
        selected=plan['selected_files']
        if type(selected)is not list or not 1<=len(selected)<=SELECTED_FILES:raise ValueError('Bounded selected medical resources required')
        pins=[_pin(value,67108864)for value in selected]
        if len({pin['path']for pin in pins})!=len(pins)or sum(pin['bytes']for pin in pins)>SELECTED_BYTES:raise ValueError('Distinct bounded selected resources required')
        self.selected={pin['path']:pin for pin in pins}
        for pin in (self.runtime.python,self.runtime.java,self.runtime.jvm_config,*self.runtime.jars):
            if self.selected.get(str(pin.path))!=_selection(pin):raise PermissionError('Selected executable and JAR pin required')
        for path in (self.producer.bun,self.producer.git):
            if str(path)not in self.selected:raise PermissionError('Explicit producer executable pin required')
        for pin in pins:self._acquire(pin)
        refs=plan['namespaces'];evidence=plan['evidence']
        if type(refs)is not list or {ref.get('kind')for ref in refs if type(ref)is dict}!={'namespace','metadata','closure'}or len(refs)!=3 or type(evidence)is not list or not 1<=len(evidence)<=5:raise ValueError('Complete known runtime descriptors and evidence required')
        allrefs=refs+evidence
        if len(allrefs)>8:raise ValueError('Runtime reference count exceeded')
        reference_pins=[]
        for ref in allrefs:
            _exact(ref,('kind','record','profile','scope'));pin=_pin(ref['record'],REFERENCE_BYTES);_integer(pin['bytes'],REFERENCE_BYTES,1);reference_pins.append(pin)
            if type(ref['scope'])is not str or not ref['scope']or len(ref['scope'].encode())>4096 or type(ref['profile'])is not str:raise ValueError('Explicit bounded runtime evidence scope required')
        if len({p['path']for p in reference_pins})!=len(reference_pins)or sum(p['bytes']for p in reference_pins)>REFERENCE_TOTAL:raise ValueError('Bounded distinct medical references required')
        for ref,pin in zip(allrefs,reference_pins):
            value=_decode(self._acquire(pin))
            if ref in refs:self.descriptors[ref['kind']]=value
            else:
                if ref['kind']not in ('spark-guard','spark-delta-native')or type(value)is not dict:raise ValueError('Known original evidence correspondence required')
        anchor=NATIVE_EVIDENCE[(self.runtime.spark_version,self.runtime.delta_version)]
        if not any(ref['kind']=='spark-delta-native'and(ref['profile'],ref['record']['sha256'])==anchor for ref in evidence):raise PermissionError('Exact separate independently reviewed native Delta evidence required')
        expected={'namespace':'ashlar-evolution-runtime-namespace-j/0.1','metadata':'ashlar-evolution-runtime-metadata-j/0.1','closure':'ashlar-evolution-installed-runtime-closure/0.1'}
        if any(ref['profile']!=expected[ref['kind']]for ref in refs):raise ValueError('Known descriptor owner required')
        self._validate_descriptors()
        self._validate_imports(plan['imports'])
        self.plan=plan
        self._producer_identity=self._directory(self.producer.source)
        self.renew()
    def _directory(self,path):
        named=path.lstat()
        if path.resolve()!=path or not stat.S_ISDIR(named.st_mode):raise PermissionError('Canonical selected runtime directory required')
        return named.st_dev,named.st_ino,named.st_uid,named.st_mode
    def _validate_descriptors(self):
        namespace=self.descriptors['namespace'];metadata=self.descriptors['metadata'];closure=self.descriptors['closure']
        _exact(namespace,('roots','links','site_startups'));_exact(metadata,('files','qualification'));_exact(closure,('files','format','scope'))
        if closure['format']!='ashlar-evolution-installed-runtime-closure/0.1':raise ValueError('Known complete runtime closure format required')
        for value in (metadata['files'],closure['files']):
            if type(value)is not list or not 1<=len(value)<=RECORDS:raise ValueError('Per-descriptor medical record bound required')
        meta={};closed={}
        for row in metadata['files']:
            _exact(row,('path','dev','ino','mode','mtimeNs','bytes'));_path(row['path'])
            for key in ('dev','ino','mode','mtimeNs','bytes'):_integer(row[key],2**64-1)
            if row['path']in meta:raise ValueError('Duplicate runtime metadata path')
            meta[row['path']]=row
        for row in closure['files']:
            pin=_pin(row,2**40)
            if pin['path']in closed:raise ValueError('Duplicate closure path')
            closed[pin['path']]=pin
        if set(meta)!=set(closed)or any(meta[path]['bytes']!=pin['bytes']for path,pin in closed.items()):raise PermissionError('Original metadata/closure correspondence differs')
        for path,pin in self.selected.items():
            if path not in closed or closed[path]!=pin:raise PermissionError('Original selected resource closure differs')
        if type(namespace['roots'])is not dict or type(namespace['site_startups'])is not dict or type(namespace['links'])is not list or sum(len(v)for v in namespace['roots'].values()if type(v)is dict)>RECORDS or len(namespace['links'])>RECORDS:raise ValueError('Bounded complete namespace required')
        for root,entries in namespace['roots'].items():
            rootpath=_path(root)
            if type(entries)is not dict:raise ValueError('Closed namespace entries required')
            for name,row in entries.items():
                path=_path(name)
                if rootpath not in path.parents:raise ValueError('Namespace member outside selected root')
                self._entry(row)
        for root,entries in namespace['site_startups'].items():
            _path(root)
            if type(entries)is not dict or len(entries)>RECORDS:raise ValueError('Bounded startup namespace required')
            for name,row in entries.items():_path(name);self._entry(row)
        for row in namespace['links']:
            _exact(row,('path','target','resolved'));_path(row['path']);_path(row['resolved'])
            if type(row['target'])is not str or len(row['target'].encode())>4096:raise ValueError('Bounded original link required')
        required=(self.runtime.java_home,self.runtime.pyspark_root,self.runtime.delta_root)
        roots=tuple(_path(root)for root in namespace['roots'])
        if any(not any(root==path or root in path.parents for root in roots)for path in required):raise PermissionError('Complete configured runtime root coverage required')
        self._namespace_identities={str(root):self._directory(root)for root in roots}
        self._startup_identities={root:self._directory(_path(root))for root in namespace['site_startups']}
        self.metadata=meta
    def _entry(self,row):
        if type(row)is not dict:raise ValueError('Closed original namespace kind required')
        kind=row.get('kind')
        if kind=='file':_exact(row,('kind','bytes'));_integer(row['bytes'],2**40)
        elif kind=='directory':_exact(row,('kind',))
        elif kind=='symlink':
            _exact(row,('kind','target','resolved'));_path(row['resolved'])
            if type(row['target'])is not str or len(row['target'].encode())>4096:raise ValueError('Bounded namespace link required')
        else:raise ValueError('Unknown original namespace kind')
    def _validate_imports(self,imports):
        if type(imports)is not list or len(imports)!=2 or [r.get('module')for r in imports if type(r)is dict]!=['pyspark','delta']:raise ValueError('Closed ordered medical SDK imports required')
        for row,distribution,version,root in zip(imports,('pyspark','delta-spark'),(self.runtime.spark_version,self.runtime.delta_version),(self.runtime.pyspark_root,self.runtime.delta_root)):
            _exact(row,('module','distribution','version','origin','metadata'));origin=_path(row['origin']);pin=_pin(row['metadata'],1048576)
            if row['distribution']!=distribution or row['version']!=version or root not in origin.parents or str(origin.resolve())not in self.selected or self.selected.get(pin['path'])!=pin:raise PermissionError('Exact selected SDK origin/metadata required')
            if pin['path'].split('/')[-1]!='METADATA':raise ValueError('Original distribution metadata required')
    def _namespace(self):
        plan=self.descriptors['namespace']
        for root,expected in plan['roots'].items():
            if self._directory(Path(root))!=self._namespace_identities[root]:raise PermissionError('Runtime namespace root changed')
            actual={}
            for path in Path(root).rglob('*'):
                if path.name=='.DS_Store':continue
                if len(actual)>=RECORDS:raise ValueError('Runtime traversal bound exceeded')
                if path.is_symlink():actual[str(path)]={'kind':'symlink','target':os.readlink(path),'resolved':str(path.resolve())}
                elif path.is_file():actual[str(path)]={'kind':'file','bytes':path.stat().st_size}
                elif path.is_dir():actual[str(path)]={'kind':'directory'}
                else:raise PermissionError('Unknown selected runtime entry')
            if actual!=expected:raise PermissionError('Complete selected namespace changed')
        for root,expected in plan['site_startups'].items():
            if self._directory(Path(root))!=self._startup_identities[root]:raise PermissionError('Runtime startup root changed')
            actual={}
            for path in Path(root).iterdir():
                if path.suffix=='.pth'or path.name in ('sitecustomize.py','usercustomize.py','sitecustomize','usercustomize'):
                    if path.is_symlink()or not path.is_file():raise PermissionError('Selected startup alias')
                    actual[str(path)]={'kind':'file','bytes':path.stat().st_size}
            if actual!=expected:raise PermissionError('Complete startup namespace changed')
        for link in plan['links']:
            path=Path(link['path'])
            if not path.is_symlink()or os.readlink(path)!=link['target']or str(path.resolve())!=link['resolved']:raise PermissionError('Original runtime link changed')
    def _metadata(self):
        for row in self.metadata.values():
            path=Path(row['path']);s=path.stat()
            if path.is_symlink()or (s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_mode)!=(row['dev'],row['ino'],row['bytes'],row['mtimeNs'],row['mode']):raise PermissionError('Original runtime metadata changed')
    def renew(self):
        if self.closed:raise PermissionError('Medical runtime owner closed')
        callbacks=([self._renew_configuration]if hasattr(self,'original_configuration')else[])+[owner.verify for owner in self.files if owner.fd is not None]
        if self.plan is not None:
            callbacks += [self._namespace,self._metadata,lambda:self._renew_producer()]
        finish(None,callbacks)
    def _renew_producer(self):
        if self._directory(self.producer.source)!=self._producer_identity:raise PermissionError('Producer source directory changed')
    def verify_imports(self):
        if self.closed or self.plan is None:raise PermissionError('Admitted medical runtime required before imports')
        self.renew();self.import_acquired=True
        if Path(sys.executable).resolve()!=self.runtime.python.path or '.'.join(map(str,sys.version_info[:3]))!=self.runtime.python_version:raise PermissionError('Actual selected Python process differs')
        primary=None
        try:
            for row in self.plan['imports']:
                module=importlib.import_module(row['module']);origin=Path(module.__file__)
                if str(origin)!=row['origin']or origin.resolve()!=Path(row['origin']).resolve()or getattr(module,'__version__',row['version'])!=row['version']:raise PermissionError('Actual selected SDK import origin/version differs')
                metadata=importlib.metadata.PathDistribution(Path(row['metadata']['path']).parent)
                if metadata.metadata.get('Name')!=row['distribution']or metadata.version!=row['version']:raise PermissionError('Actual selected distribution metadata differs')
        except BaseException as error:primary=error
        finish(primary,[self.renew])
    @property
    def jar_paths(self):
        if self.closed or self.plan is None:raise PermissionError('Current medical runtime required')
        self.renew();return tuple(pin.path for pin in self.runtime.jars)
    def native_environment(self):
        if self.closed or self.plan is None:raise PermissionError('Current medical runtime required')
        self.renew()
        environment={'PATH':'/usr/bin:/bin','JAVA_HOME':str(self.runtime.java_home),'PYSPARK_PYTHON':str(self.runtime.python.path),'PYSPARK_DRIVER_PYTHON':str(self.runtime.python.path),'PYTHONDONTWRITEBYTECODE':'1','LANG':'C','LC_ALL':'C','TZ':'UTC'}
        environment.update({name:value for _,_,name,value in NATIVE_PYTHON_SETTINGS})
        if sys.platform=='darwin':
            # Explicit CoreFoundation default; never inherit user preferences.
            # Apple webdavfs WebDAV_Mount.c derives this startup key from getuid.
            environment['__CF_USER_TEXT_ENCODING']='0x%X:0x0:0x0'%os.getuid()
        return environment
    @classmethod
    @contextmanager
    def open(cls,runtime,inputs,bounds,producer):
        owner=cls(runtime,inputs,bounds,producer);primary=None
        try:owner._open();yield owner
        except BaseException as error:primary=error
        finish(primary,[owner.renew]+([owner.verify_imports]if owner.import_acquired else[])+[file.close for file in owner.files]+[lambda:setattr(owner,'closed',True)])
