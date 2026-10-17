"""Closed installed medical batch selection; paths never select compiler trust."""
from dataclasses import dataclass,fields,is_dataclass
import json
from pathlib import Path
from collections.abc import Mapping
from ashlar.weft_implicit_arithmetic_distribution import ImplicitArithmeticDistributionPaths,open_implicit_arithmetic_distribution
from .medical_producer import decode_medical
from .medical_query_batch import MedicalQueryBatch
from .original_medical_local_configuration import OriginalMedicalReadConfig,parse_original_medical_local

PROFILE='ashlar-original-medical-query-batch/0.1'


@dataclass(frozen=True)
class OriginalMedicalQueryConfig:
    read:OriginalMedicalReadConfig
    compiler:ImplicitArithmeticDistributionPaths
    cases:tuple
    def __post_init__(self):
        if type(self.read)is not OriginalMedicalReadConfig:raise ValueError('Exact original medical read required')
        self.read.__post_init__()
        MedicalQueryBatch(self.compiler,self.cases)
        if self.compiler.package is not None:raise ValueError('Existing indexed compiler installation required')
        for path in (self.compiler.index,self.compiler.output):
            if type(path)is not type(Path('/'))or not path.is_absolute()or path.resolve()!=path:
                raise ValueError('Canonical explicit compiler paths required')
            if path==self.output or self.output in path.parents or path in self.output.parents:
                raise ValueError('Compiler and result output must be separate')
    @property
    def runtime(self):return self.read.runtime
    @property
    def inputs(self):return self.read.inputs
    @property
    def bounds(self):return self.read.bounds
    @property
    def producer(self):return self.read.producer
    @property
    def output(self):return self.read.output


def parse_original_medical_query(raw):
    value=decode_medical(raw,262144)
    if type(value)is not dict or set(value)!={'profile','read','compiler','cases'}or value['profile']!=PROFILE:
        raise ValueError('Closed original medical query configuration required')
    compiler=value['compiler']
    if type(compiler)is not dict or set(compiler)!={'index','installation'}or any(type(v)is not str for v in compiler.values())or type(value['cases'])is not list:
        raise ValueError('Explicit existing compiler and finite cases required')
    result=OriginalMedicalQueryConfig(parse_original_medical_local(json.dumps(value['read']).encode()),
        ImplicitArithmeticDistributionPaths(Path(compiler['index']),Path(compiler['installation'])),tuple(value['cases']))
    result.__post_init__();return result


def retain_medical_query_selection(config):
    """Entry/final exact compiler verification; never caches a query grant."""
    if type(config)is not OriginalMedicalQueryConfig:raise ValueError('Exact original medical query selection required')
    def plain(value):
        if is_dataclass(value):return {f.name:plain(getattr(value,f.name))for f in fields(value)}
        if isinstance(value,Mapping):return {k:plain(v)for k,v in value.items()}
        if type(value)in (tuple,list):return [plain(v)for v in value]
        if isinstance(value,Path):return str(value)
        return value
    def selection():
        config.__post_init__()
        return json.dumps(plain(config),sort_keys=True,separators=(',',':'))
    original=selection();ready=open_implicit_arithmetic_distribution(config.compiler).ready_bytes
    def renew():
        if selection()!=original:raise PermissionError('Original medical query selection changed')
        if open_implicit_arithmetic_distribution(config.compiler).ready_bytes!=ready:
            raise PermissionError('Original indexed medical compiler changed')
        if selection()!=original:raise PermissionError('Closing medical query selection changed')
    return renew
