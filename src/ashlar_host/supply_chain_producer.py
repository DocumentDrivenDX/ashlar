"""Actual pinned public dataset invocation and held emitted receipt custody."""
import hashlib
import os
import stat
import weakref
from pathlib import Path
from dataclasses import dataclass
from .evolution_producer import capture,snapshot
from .resources import RESOURCE_ROOT
from .supply_chain_finite_source import UMF_REVISION,FiniteSupplyChainSource,validate_supply_chain_dataset_receipt
from .lifecycle import finish

@dataclass(frozen=True)
class SupplyChainProducerConfig:
    source:Path
    bun:Path
    git:Path
    timeout_seconds:int
    maximum_output_bytes:int
    maximum_receipt_bytes:int
    def __post_init__(self):
        if any(type(p)is not type(Path('/'))or not p.is_absolute()for p in (self.source,self.bun,self.git)):
            raise ValueError('Exact absolute selected producer paths required')
        if any(type(value)is not int or not 0<value<=limit for value,limit in ((self.timeout_seconds,60),(self.maximum_output_bytes,1048576),(self.maximum_receipt_bytes,8388608))):
            raise ValueError('Explicit finite selected producer bounds required')

class HeldSupplyChainDataset:
    """Execute once; renew original inputs, emitted receipt and selected producer.

    Current receipt renewal rechecks the original deterministic validation inputs
    and immutable selected Git source, not a fabricated second producer execution.
    Explicit selected runtime/resource evidence remains an outer qualification.
    """
    def __setattr__(self,name,value):
        if name in ('_inputs','_invocation')and hasattr(self,name):
            raise AttributeError('Original producer capture is immutable')
        object.__setattr__(self,name,value)
    def __init__(self,config,model,graph,output):
        if type(config)is not SupplyChainProducerConfig:raise ValueError('Typed selected public producer required')
        config.__post_init__()
        self.config=config;self.model=Path(model);self.graph=Path(graph);self.output=Path(output)
        paths=(self.model,self.graph,self.output,config.source,config.bun,config.git)
        if any(not p.is_absolute()or any(q.is_symlink()for q in (p,*p.parents))for p in paths):raise ValueError('Explicit original producer paths required')
        if self.output.exists():raise ValueError('Fresh emitted public receipt required')
        self.script=RESOURCE_ROOT/'check_supply_chain_dataset.ts';self.script_bytes=snapshot(self.script,1024*1024)
        self.originals=(snapshot(self.model,1024*1024),snapshot(self.graph,1024*1024));self.raw=None
        self.environment={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','TZ':'UTC','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_CONFIG_SYSTEM':'/dev/null','GIT_TERMINAL_PROMPT':'0','GIT_OPTIONAL_LOCKS':'0'}
        values=_config_values(config)
        source_info=config.source.lstat()
        if not stat.S_ISDIR(source_info.st_mode):raise ValueError('Original producer source directory required')
        argv=(str(config.bun),str(self.script),str(config.source),str(self.output),str(self.model),str(self.graph),str(config.git))
        files=tuple(_file(path,maximum,single_link=single)for path,maximum,single in
            ((self.model,1048576,True),(self.graph,1048576,True),(self.script,1048576,True),
             (config.bun,256*1024*1024,False),(config.git,32*1024*1024,False)))
        if tuple(f.digest for f in files[:3])!=tuple(hashlib.sha256(raw).hexdigest()for raw in (*self.originals,self.script_bytes)):
            raise ValueError('Original producer input snapshots differ')
        self._inputs=_CaptureInputs(config,values,self.model,self.graph,self.output,self.script,self.script_bytes,
            self.originals,tuple(sorted(self.environment.items())),argv,files,
            (source_info.st_dev,source_info.st_ino,source_info.st_uid,source_info.st_mode))
        primary=None;origin=None
        try:
            self._pin()
            # Git pin operations may have changed an input. Admit every original
            # input immediately before production, without another Git call.
            finish(None,[self._guard_inputs,lambda:_source_identity(self._inputs)]
                +[lambda original=f:_file(original.path,original.maximum,original)for f in files])
            self.stdout,self.stderr=capture(argv,cwd=self.output.parent,environment=dict(self._inputs.environment),
                timeout_seconds=values[3],maximum_output_bytes=values[4])
            if (type(self.stdout)is not bytes or type(self.stderr)is not bytes
                    or len(self.stdout)>values[4] or self.stderr):
                raise ValueError('Exact original producer streams required')
            self.raw=snapshot(self.output,values[5])
            origin=_InvocationOrigin(self._inputs,_file(self.output,values[5]),self.raw,self.stdout,self.stderr)
            if origin.receipt.digest!=hashlib.sha256(self.raw).hexdigest():
                raise ValueError('Original emitted receipt snapshot differs')
            validate_supply_chain_dataset_receipt(*self.originals,self.raw)
        except BaseException as error:primary=error
        callbacks=[self._guard_inputs,lambda:_source_identity(self._inputs),self._pin]
        callbacks.extend(lambda original=f:_file(original.path,original.maximum,original)for f in files)
        if origin is not None:callbacks.append(lambda:_verify_origin(origin))
        finish(primary,callbacks)
        invocation=object.__new__(OriginalSupplyChainDatasetInvocation)
        _INVOCATIONS[invocation]=origin
        self._invocation=invocation
    def _guard_inputs(self):
        original=self._inputs
        if (_config_values(self.config)!=original.config
                or (self.model,self.graph,self.output,self.script,self.script_bytes,self.originals,
                    tuple(sorted(self.environment.items())))!=
                   (original.model,original.graph,original.output,original.script,original.script_bytes,
                    original.originals,original.environment)):
            raise ValueError('Original producer owning inputs differ')
        if hasattr(self,'_invocation'):
            origin=_origin(self._invocation)
            if (self.raw,self.stdout,self.stderr)!=(origin.raw,origin.stdout,origin.stderr):
                raise ValueError('Original producer output correspondence differs')
    def original_invocation(self):
        finish(None,[self._guard_inputs,lambda:_verify_origin(_origin(self._invocation))])
        return self._invocation
    def _pin(self):
        _pin_inputs(self._inputs)
    def _files(self):
        finish(None,[self._guard_inputs,lambda:_verify_origin(_origin(self._invocation))])
    def __call__(self,model,graph):
        primary=None
        try:
            if type(model)is not bytes or type(graph)is not bytes or (model,graph)!=self._inputs.originals:
                raise ValueError('Original public dataset inputs differ')
        except BaseException as error:primary=error
        finish(primary,[self._guard_inputs,lambda:_verify_origin(_origin(self._invocation))])
        return _origin(self._invocation).raw
    def metadata(self):
        finish(None,[self._guard_inputs,lambda:_verify_origin(_origin(self._invocation))])
        origin=_origin(self._invocation)
        return {'umf_revision':UMF_REVISION,'receipt_sha256':origin.receipt.digest,
            'stdout':origin.stdout.decode(),'stderr':origin.stderr.decode(),'qualification':'One actual public finite dataset validation; subsequent renewals verify original inputs/receipt/script and selected clean Git source. No native/source authority grant.'}

# A live invocation is minted only by the actual producer lifecycle. This is a
# same-process origin boundary, not authentication of a serialized archive.
@dataclass(frozen=True, eq=False, init=False)
class OriginalSupplyChainDatasetInvocation:
    def __init__(self):
        raise TypeError('Only the actual successful producer can mint an invocation')


@dataclass(frozen=True)
class _OriginalFile:
    path: Path
    maximum: int
    identity: tuple
    digest: str
    single_link: bool


def _file(path, maximum, expected=None, *, single_link=True):
    def facts(info):
        return (info.st_dev, info.st_ino, info.st_uid, info.st_mode,
                info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    if expected is not None:single_link=expected.single_link
    fd=None;primary=None;result=None
    try:
        if path.resolve()!=path or any(q.is_symlink()for q in (path,*path.parents)):
            raise ValueError('Original producer canonical file required')
        named=path.lstat()
        if (not stat.S_ISREG(named.st_mode) or named.st_nlink<1
                or single_link and named.st_nlink!=1 or named.st_size>maximum):
            raise ValueError('Original producer regular-file capacity required')
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        before=os.fstat(fd);digest=hashlib.sha256();count=0
        while True:
            raw=os.read(fd,min(65536,maximum-count+1))
            if not raw:break
            count+=len(raw)
            if count>maximum:raise ValueError('Original producer file capacity exceeded')
            digest.update(raw)
        if (facts(named)!=facts(before) or facts(named)!=facts(os.fstat(fd))
                or facts(named)!=facts(path.lstat()) or count!=named.st_size
                or path.resolve()!=path or any(q.is_symlink()for q in (path,*path.parents))):
            raise ValueError('Original producer file changed during read')
        result=_OriginalFile(path,maximum,facts(named),digest.hexdigest(),single_link)
        if expected is not None and result!=expected:raise ValueError('Original producer file correspondence differs')
    except BaseException as error:primary=error
    finish(primary,[]if fd is None else[lambda:os.close(fd)])
    return result


def _config_values(config):
    if type(config)is not SupplyChainProducerConfig:raise ValueError('Exact original producer configuration required')
    return (config.source,config.bun,config.git,config.timeout_seconds,
            config.maximum_output_bytes,config.maximum_receipt_bytes)


@dataclass(frozen=True)
class _CaptureInputs:
    selected_config: SupplyChainProducerConfig
    config: tuple
    model: Path
    graph: Path
    output: Path
    script: Path
    script_bytes: bytes
    originals: tuple
    environment: tuple
    argv: tuple
    files: tuple
    source_identity: tuple


@dataclass(frozen=True)
class _InvocationOrigin:
    inputs: _CaptureInputs
    receipt: _OriginalFile
    raw: bytes
    stdout: bytes
    stderr: bytes


_INVOCATIONS=weakref.WeakKeyDictionary()


def _origin(invocation):
    if type(invocation)is not OriginalSupplyChainDatasetInvocation:
        raise ValueError('Actual same-process original producer invocation required')
    try:return _INVOCATIONS[invocation]
    except KeyError:raise ValueError('Unminted producer invocation refused')from None


def _verify_tools(inputs):
    finish(None,[lambda:_source_identity(inputs)]
        +[lambda original=f:_file(original.path,original.maximum,original)for f in inputs.files[-2:]])


def _pin_inputs(inputs):
    source,bun,git,timeout,maximum,_=inputs.config
    # Whole original tools and canonical source BEFORE executing even read-only
    # Git commands; repeated after each use. Hash correspondence assumes the
    # original operator selected genuine tools, it does not authenticate them.
    _verify_tools(inputs)
    def check(args,expected):
        _verify_tools(inputs)
        primary=None
        try:
            out,err=capture([str(git),'-C',str(source),*args],cwd=inputs.output.parent,
                environment=dict(inputs.environment),timeout_seconds=timeout,maximum_output_bytes=maximum)
            if out.decode().strip()!=expected or err:raise ValueError('Clean original public UMF source required')
        except BaseException as error:primary=error
        finish(primary,[lambda:_verify_tools(inputs)])
    finish(None,[lambda:check(['rev-parse','HEAD'],UMF_REVISION),lambda:check(['status','--porcelain'],'')])


def _source_identity(inputs):
    source=inputs.config[0];info=source.lstat()
    if (source.resolve()!=source or any(p.is_symlink()for p in(source,*source.parents))
            or not stat.S_ISDIR(info.st_mode)
            or (info.st_dev,info.st_ino,info.st_uid,info.st_mode)!=inputs.source_identity):
        raise ValueError('Original producer source identity differs')


def _verify_origin(origin):
    inputs=origin.inputs
    def configuration():
        if (type(origin.raw)is not bytes or type(origin.stdout)is not bytes or type(origin.stderr)is not bytes
                or origin.receipt.digest!=hashlib.sha256(origin.raw).hexdigest()):
            raise ValueError('Original producer output witness differs')
        if _config_values(inputs.selected_config)!=inputs.config:
            raise ValueError('Original selected producer configuration differs')
    finish(None,[configuration,lambda:_source_identity(inputs),lambda:_pin_inputs(inputs)]
        +[lambda original=f:_file(original.path,original.maximum,original)for f in inputs.files]
        +[lambda:_file(origin.receipt.path,origin.receipt.maximum,origin.receipt),
          lambda:validate_supply_chain_dataset_receipt(*inputs.originals,origin.raw)])


class RetainedOriginalSupplyChainDataset:
    """Existing receipt from a genuine live origin; no durable archive restore."""
    def __setattr__(self,name,value):
        if name=='closed'and hasattr(self,name)and (self.closed is True or value is not True):
            raise AttributeError('Retained producer lifecycle cannot be reset')
        if name in ('_original','_invocation')and hasattr(self,name):
            raise AttributeError('Retained original invocation is immutable')
        object.__setattr__(self,name,value)
    def __init__(self,invocation):
        original=_origin(invocation)
        _verify_origin(original)
        self._invocation=invocation;self._original=original;self.closed=False
    def _guard(self):
        if self.closed or _origin(self._invocation)is not self._original:
            raise ValueError('Original retained producer owner differs')
    def renew(self):
        finish(None,[self._guard,lambda:_verify_origin(self._original)])
    def __call__(self,model,graph):
        primary=None
        try:
            self._guard()
            if type(model)is not bytes or type(graph)is not bytes or (model,graph)!=self._original.inputs.originals:
                raise ValueError('Original retained producer inputs differ')
        except BaseException as error:primary=error
        finish(primary,[lambda:_verify_origin(self._original)])
        return self._original.raw
    def metadata(self):
        self.renew()
        return {'umf_revision':UMF_REVISION,'receipt_sha256':self._original.receipt.digest,
            'qualification':'Same-process authentic original public producer receipt correspondence; no durable restart, current native/source/publication or ACK authority'}
    def close(self):
        try:self.renew()
        finally:self.closed=True


def reopen_original_supply_chain_source(invocation):
    """Same-process source composition only; never deserialize origin evidence."""
    retained=RetainedOriginalSupplyChainDataset(invocation)
    original=_origin(invocation)
    primary=None;result=None
    try:
        result=FiniteSupplyChainSource(original.inputs.model,original.inputs.graph,
                                      current_dataset=retained)
    except BaseException as error:primary=error
    finish(primary,[retained.renew])
    return result
