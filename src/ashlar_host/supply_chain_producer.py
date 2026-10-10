"""Actual pinned public dataset invocation and held emitted receipt custody."""
import hashlib
from pathlib import Path
from dataclasses import dataclass
from .evolution_producer import capture,snapshot
from .resources import RESOURCE_ROOT
from .supply_chain_finite_source import UMF_REVISION

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
    def __init__(self,config,model,graph,output):
        if type(config)is not SupplyChainProducerConfig:raise ValueError('Typed selected public producer required')
        config.__post_init__()
        self.config=config;self.model=Path(model);self.graph=Path(graph);self.output=Path(output)
        paths=(self.model,self.graph,self.output,config.source,config.bun,config.git)
        if any(not p.is_absolute()or any(q.is_symlink()for q in (p,*p.parents))for p in paths):raise ValueError('Explicit original producer paths required')
        if self.output.exists():raise ValueError('Fresh emitted public receipt required')
        self.script=RESOURCE_ROOT/'check_supply_chain_dataset.ts';self.script_bytes=snapshot(self.script,1024*1024)
        self.originals=(snapshot(self.model,1024*1024),snapshot(self.graph,1024*1024));self.raw=None
        self.environment={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','TZ':'UTC','GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_CONFIG_SYSTEM':'/dev/null','GIT_TERMINAL_PROMPT':'0'}
        self._pin()
        self.stdout,self.stderr=capture([str(config.bun),str(self.script),str(config.source),str(self.output),str(self.model),str(self.graph),str(config.git)],cwd=self.output.parent,
            environment=self.environment,timeout_seconds=config.timeout_seconds,maximum_output_bytes=config.maximum_output_bytes)
        self.raw=snapshot(self.output,self.config.maximum_receipt_bytes);self._pin();self._files()
    def _pin(self):
        config=self.config
        for args,expected in ((['rev-parse','HEAD'],UMF_REVISION),(['status','--porcelain'],'')):
            out,err=capture([str(config.git),'-C',str(config.source),*args],cwd=self.output.parent,environment=self.environment,
                timeout_seconds=config.timeout_seconds,maximum_output_bytes=config.maximum_output_bytes)
            if out.decode().strip()!=expected or err:raise ValueError('Clean original public UMF source required')
    def _files(self):
        if (snapshot(self.model,1024*1024),snapshot(self.graph,1024*1024))!=self.originals or snapshot(self.script,1024*1024)!=self.script_bytes or snapshot(self.output,self.config.maximum_receipt_bytes)!=self.raw:
            raise ValueError('Original emitted public dataset custody differs')
    def __call__(self,model,graph):
        if type(model)is not bytes or type(graph)is not bytes or (model,graph)!=self.originals:raise ValueError('Original public dataset inputs differ')
        self._pin();self._files();return self.raw
    def metadata(self):
        self._pin();self._files()
        return {'umf_revision':UMF_REVISION,'receipt_sha256':hashlib.sha256(self.raw).hexdigest(),
            'stdout':self.stdout.decode(),'stderr':self.stderr.decode(),'qualification':'One actual public finite dataset validation; subsequent renewals verify original inputs/receipt/script and selected clean Git source. No native/source authority grant.'}
