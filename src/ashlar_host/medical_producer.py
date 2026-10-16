"""Selected public medical revalidation; no source/native authority grant."""
from dataclasses import dataclass
import decimal
import hashlib
import json
import os
from pathlib import Path
import stat
from ashlar.medical_source import SOURCE_SHA,GRAPH_SHA,PUBLIC_SHA,UMF_PIN
from .config import ProducerConfig
from .evolution_producer import capture
from .lifecycle import finish
from .resources import RESOURCE_ROOT


@dataclass(frozen=True)
class MedicalFile:
    path:Path
    maximum:int
    def __post_init__(self):
        if type(self.path)is not type(Path('/')) or not self.path.is_absolute() or self.path.resolve()!=self.path or type(self.maximum)is not int or not 0<self.maximum<=4194304:
            raise ValueError('Exact bounded canonical medical input required')
        s=self.path.lstat()
        if not stat.S_ISREG(s.st_mode) or s.st_nlink!=1 or not 0<s.st_size<=self.maximum:raise ValueError('Original single-link medical file required')
        object.__setattr__(self,'size',s.st_size)
        object.__setattr__(self,'identity',(s.st_dev,s.st_ino,s.st_uid,s.st_mode))
    def read(self):
        fd=None;primary=None;raw=None
        try:
            fd=os.open(self.path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK);before=os.fstat(fd)
            if (before.st_dev,before.st_ino,before.st_uid,before.st_mode)!=self.identity or before.st_nlink!=1 or before.st_size!=self.size or not 0<before.st_size<=self.maximum:raise ValueError('Original medical file changed')
            pieces=[];size=0
            while True:
                part=os.read(fd,min(65536,self.maximum-size+1))
                if not part:break
                size+=len(part)
                if size>self.maximum:raise ValueError('Medical input bound exceeded')
                pieces.append(part)
            after=os.fstat(fd);named=self.path.lstat()
            if (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)!=(before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns) or (named.st_dev,named.st_ino,named.st_uid,named.st_mode)!=self.identity or after.st_nlink!=1 or named.st_nlink!=1:raise ValueError('Original medical file changed')
            raw=b''.join(pieces)
        except BaseException as error:primary=error
        finish(primary,[lambda:os.close(fd)]if fd is not None else [])
        return raw


def decode_medical(raw,maximum=4194304):
    if type(raw)is not bytes or not 0<len(raw)<=maximum:raise ValueError('Bounded medical JSON required')
    def pairs(items):
        value={}
        for key,item in items:
            if key in value:raise ValueError('Duplicate medical JSON member')
            value[key]=item
        return value
    def nonfinite(_):raise ValueError('Nonfinite medical JSON refused')
    return json.loads(raw,object_pairs_hook=pairs,parse_float=decimal.Decimal,parse_constant=nonfinite)


def same_medical_json(left,right):
    """Exact decoded JSON carriers; no Python Boolean/numeric equality aliases."""
    if type(left)is not type(right):return False
    if type(left)is dict:return set(left)==set(right)and all(same_medical_json(left[k],right[k])for k in left)
    if type(left)is list:return len(left)==len(right)and all(same_medical_json(a,b)for a,b in zip(left,right))
    if type(left)is decimal.Decimal:return left.as_tuple()==right.as_tuple()
    if type(left)in (str,int,bool,type(None)):return left==right
    raise ValueError('Closed medical JSON carrier required')


def verify_medical_revalidation(model,graph,baseline,observed):
    if any(type(raw)is not bytes for raw in (model,graph,baseline,observed)) or tuple(hashlib.sha256(raw).hexdigest()for raw in (model,graph,baseline))!=(SOURCE_SHA,GRAPH_SHA,PUBLIC_SHA):raise PermissionError('Exact historical medical inputs required')
    original=decode_medical(baseline);actual=decode_medical(observed)
    if set(actual)!={'profile','umfRevision','modelSha256','graphSha256','baselineSha256','receipt','booleanReceipts'} or actual['profile']!='ashlar-original-medical-public-revalidation/0.1' or actual['umfRevision']!=UMF_PIN or (actual['modelSha256'],actual['graphSha256'],actual['baselineSha256'])!=(SOURCE_SHA,GRAPH_SHA,PUBLIC_SHA):raise PermissionError('Exact public medical revalidation required')
    fresh=actual['receipt'];old=original['receipt']
    if type(fresh)is not dict or fresh.get('operation')!='validate-core-dataset-values-compact' or fresh.get('version')!='1.0.0' or fresh.get('scope')!='supplied-dataset-only' or fresh.get('datasetValidation',{}).get('valid')is not True or fresh.get('datasetValidation',{}).get('complete')is not True:raise PermissionError('Complete public medical dataset required')
    # Complete semantic outputs remain comparable; provenance is retained separately,
    # never promoted to authority or required to have identical runtime spelling.
    for name,value in old.items():
        if name!='provenance' and (name not in fresh or not same_medical_json(fresh[name],value)):raise PermissionError('Original public medical semantic result differs')
    if not same_medical_json(fresh.get('source'),decode_medical(model)) or not same_medical_json(fresh.get('input'),original['input']) or len(fresh['records'])!=51 or len(fresh['keys'])!=51 or len(fresh['relationships'])!=62:raise PermissionError('Complete original medical input/result required')
    booleans=actual['booleanReceipts']
    if type(booleans)is not list or len(booleans)!=3:raise PermissionError('Complete original Boolean receipts required')
    for expected,returned in zip(original['booleanReceipts'],booleans):
        if type(returned)is not dict or any(k not in returned or not same_medical_json(returned[k],v)for k,v in expected.items()if k!='provenance') or returned.get('validation',{}).get('valid')is not True or returned.get('validation',{}).get('complete')is not True:raise PermissionError('Original public Boolean result differs')
    return actual


@dataclass(frozen=True)
class _MedicalSemanticWitness:
    """Complete constructor correspondence, never current runtime authority."""
    owner: object
    config: ProducerConfig
    files: tuple
    script: MedicalFile
    result_file: MedicalFile
    signature: tuple
    result_bytes: bytes
    input_bytes: tuple
    script_bytes: bytes
    pin_arguments: tuple
    pin_environment: tuple
    pin_limits: tuple


def _medical_descriptor_signature(f):
    path_type = type(Path('/'))
    if (type(f) is not MedicalFile or type(f.path) is not path_type
            or type(f.maximum) is not int or type(f.size) is not int
            or type(f.identity) is not tuple or len(f.identity) != 4
            or any(type(n) is not int for n in f.identity)):
        raise PermissionError('Exact medical witness descriptor required')
    return (f.path, f.maximum, f.size, f.identity)


def _medical_witness_signature(owner):
    """Exact immutable values only; no overloaded equality or parsed meaning."""
    path_type = type(Path('/'))
    if type(owner.config) is not ProducerConfig or type(owner.files) is not tuple or len(owner.files) != 3:
        raise PermissionError('Original medical witness owner/configuration differs')
    cfg = owner.config
    paths = (cfg.source, cfg.bun, cfg.git, owner.output)
    numbers = (cfg.timeout_seconds, cfg.maximum_output_bytes, cfg.maximum_receipt_bytes)
    if any(type(p) is not path_type for p in paths) or any(type(n) is not int for n in numbers):
        raise PermissionError('Exact medical witness configuration carriers required')
    if (type(owner.originals) is not tuple or len(owner.originals) != 3
            or any(type(raw) is not bytes for raw in owner.originals)
            or any(type(raw) is not bytes for raw in (owner.raw, owner.script_bytes, owner.stdout, owner.stderr))
            or type(owner.parent_identity) is not tuple or len(owner.parent_identity) != 4
            or any(type(n) is not int for n in owner.parent_identity)
            or type(owner.environment) is not dict or len(owner.environment) != 9
            or any(type(k) is not str or type(v) is not str for k,v in owner.environment.items())):
        raise PermissionError('Exact complete medical witness bytes and environment required')
    return (paths, numbers, tuple(_medical_descriptor_signature(f) for f in owner.files),
            _medical_descriptor_signature(owner.script), _medical_descriptor_signature(owner.result_file), owner.parent_identity,
            tuple(sorted(owner.environment.items())), owner.originals,
            owner.raw, owner.script_bytes, owner.stdout, owner.stderr)


class HeldMedicalDataset:
    """One actual public call; subsequent renewals retain exact original evidence."""
    def __setattr__(self,name,value):
        if name == '_semantic_witness' and hasattr(self,name):
            raise AttributeError('Original medical semantic witness is immutable')
        object.__setattr__(self,name,value)
    def __delattr__(self,name):
        if name == '_semantic_witness':
            raise AttributeError('Original medical semantic witness is immutable')
        object.__delattr__(self,name)
    def __init__(self,config,model,graph,baseline,output):
        if type(self) is not HeldMedicalDataset:raise ValueError('Exact selected medical producer owner required')
        if type(config)is not ProducerConfig:raise ValueError('Exact public producer config required')
        config.__post_init__();self.config=config
        self.files=tuple(MedicalFile(p,4194304)for p in (model,graph,baseline))
        if sum(f.size for f in self.files)>4194304:raise ValueError('Aggregate original medical input exceeded')
        self.originals=tuple(f.read()for f in self.files)
        if tuple(hashlib.sha256(raw).hexdigest()for raw in self.originals)!=(SOURCE_SHA,GRAPH_SHA,PUBLIC_SHA):raise PermissionError('Exact original medical inputs required')
        if type(output)is not type(Path('/')) or not output.is_absolute() or output.parent.resolve()!=output.parent or output.exists()or output.is_symlink():raise ValueError('Fresh canonical producer output required')
        parent=output.parent.lstat()
        if not stat.S_ISDIR(parent.st_mode)or parent.st_uid!=os.getuid()or stat.S_IMODE(parent.st_mode)!=0o700:raise ValueError('Private original producer output parent required')
        self.parent_identity=(parent.st_dev,parent.st_ino,parent.st_uid,parent.st_mode)
        self.output=output;self.script=MedicalFile(RESOURCE_ROOT/'check_medical_dataset.ts',1048576);self.script_bytes=self.script.read()
        self.environment={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','TZ':'UTC','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_CONFIG_SYSTEM':'/dev/null','GIT_TERMINAL_PROMPT':'0','TMPDIR':str(output.parent)}
        primary=None
        try:
            self._pin()
            self.stdout,self.stderr=capture([str(config.bun),str(self.script.path),str(config.source),str(output),str(model),str(graph),str(baseline),str(config.git)],cwd=output.parent,environment=self.environment,timeout_seconds=config.timeout_seconds,maximum_output_bytes=config.maximum_output_bytes)
            self.result_file=MedicalFile(output,config.maximum_receipt_bytes);self.raw=self.result_file.read()
            verify_medical_revalidation(*self.originals,self.raw)
            self._semantic_witness = _MedicalSemanticWitness(self, self.config, self.files,
                self.script, self.result_file, _medical_witness_signature(self), self.raw, self.originals, self.script_bytes,
                (str(config.source), str(config.git), str(output.parent), UMF_PIN),
                tuple(sorted(self.environment.items())), (config.timeout_seconds, config.maximum_output_bytes))
        except BaseException as error:primary=error
        finish(primary,[self._pin,self._inputs])
    def _require_witness(self):
        current = _medical_witness_signature(self)
        witness = self._semantic_witness
        if (type(self) is not HeldMedicalDataset or type(witness) is not _MedicalSemanticWitness
                or witness.owner is not self or witness.config is not self.config
                or any(a is not b for a,b in zip(witness.files,self.files))
                or witness.script is not self.script or witness.result_file is not self.result_file
                or current != witness.signature):
            raise PermissionError('Original complete medical semantic witness differs')
    def _pin(self):
        witness = getattr(self, '_semantic_witness', None)
        if witness is None:
            source,git,cwd,revision = str(self.config.source),str(self.config.git),str(self.output.parent),UMF_PIN
            environment = tuple(sorted(self.environment.items()))
            limits = (self.config.timeout_seconds,self.config.maximum_output_bytes)
        else:
            if (type(witness) is not _MedicalSemanticWitness or witness.owner is not self
                    or type(witness.pin_arguments) is not tuple or len(witness.pin_arguments) != 4
                    or any(type(v) is not str for v in witness.pin_arguments)
                    or type(witness.pin_environment) is not tuple or len(witness.pin_environment) != 9
                    or any(type(pair) is not tuple or len(pair) != 2
                           or any(type(v) is not str for v in pair) for pair in witness.pin_environment)
                    or type(witness.pin_limits) is not tuple or len(witness.pin_limits) != 2
                    or any(type(v) is not int for v in witness.pin_limits)):
                raise PermissionError('Original medical producer pin witness differs')
            source,git,cwd,revision = witness.pin_arguments
            environment,limits = witness.pin_environment,witness.pin_limits
        def check(args,expected):
            out,err=capture([git,'-C',source,*args],cwd=Path(cwd),environment=dict(environment),
                timeout_seconds=limits[0],maximum_output_bytes=limits[1])
            if type(out) is not bytes or type(err) is not bytes or out.decode('utf-8','strict').strip()!=expected or err:
                raise PermissionError('Clean pinned public medical UMF required')
        finish(None,([] if witness is None else [self._require_witness])+[
            lambda:check(['rev-parse','HEAD'],revision),lambda:check(['status','--porcelain'],'')])
    def _inputs(self):
        witness = getattr(self, '_semantic_witness', None)
        files = self.files if witness is None else witness.files
        originals = self.originals if witness is None else witness.input_bytes
        script = self.script if witness is None else witness.script
        script_bytes = self.script_bytes if witness is None else witness.script_bytes
        output = self.output if witness is None else witness.signature[0][-1]
        parent_identity = self.parent_identity if witness is None else witness.signature[5]
        def parent():
            observed=output.parent.lstat()
            if (observed.st_dev,observed.st_ino,observed.st_uid,observed.st_mode)!=parent_identity:raise PermissionError('Original medical producer output parent changed')
        def file(f,original,expected):
            if _medical_descriptor_signature(f) != expected:raise PermissionError('Original medical input descriptor changed')
            if f.read()!=original:raise PermissionError('Original medical producer input drifted')
        descriptors = tuple(_medical_descriptor_signature(f) for f in files) if witness is None else witness.signature[2]
        script_descriptor = _medical_descriptor_signature(script) if witness is None else witness.signature[3]
        finish(None,([] if witness is None else [self._require_witness])+[parent]+[lambda f=f,raw=raw,expected=expected:file(f,raw,expected)for f,raw,expected in zip(files,originals,descriptors)]+[lambda:file(script,script_bytes,script_descriptor)])
    def renew(self):
        def result():
            witness = self._semantic_witness
            if (type(witness) is not _MedicalSemanticWitness or witness.owner is not self
                    or _medical_descriptor_signature(witness.result_file) != witness.signature[4]):
                raise PermissionError('Original medical result descriptor changed')
            if witness.result_file.read()!=witness.result_bytes:raise PermissionError('Original medical revalidation bytes changed')
            self._require_witness()
        finish(None,[self._require_witness,self._pin,self._inputs,result,self._require_witness])
    def __call__(self,model,graph,baseline):
        primary=None
        try:
            if any(type(raw)is not bytes for raw in (model,graph,baseline))or (model,graph,baseline)!=self.originals:raise PermissionError('Original medical producer call differs')
        except BaseException as error:primary=error
        # This call owns current renewal on success, mismatch and cancellation.
        finish(primary,[self.renew]);return self.raw
    def metadata(self):
        self.renew()
        return {'umf_revision':UMF_PIN,'baseline_sha256':PUBLIC_SHA,'revalidation_sha256':hashlib.sha256(self.raw).hexdigest(),'actual_revalidation':decode_medical(self.raw),'stdout':self.stdout.decode('utf-8','strict'),'stderr':self.stderr.decode('utf-8','strict'),'qualification':'Public medical supplied-dataset revalidation only; no native or source authority'}
