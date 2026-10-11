"""Installed041 original archaeology/ecology compile/admission; never native authority."""
from dataclasses import dataclass
from pathlib import Path
import json
from ashlar.weft_count_star_distribution import (CountStarDistributionPaths,
    compile_count_star_distribution,installed_count_star_schema_bundle)
from .count_star_admission import CountStarAdmissionConfig,admit_count_star_artifact
from .count_star_schema import make_offline_count_star_schema_validation
from .lifecycle import finish
from .supply_chain_request import source_snapshot
from .finite_pack import FinitePackDefinition
from .pack_count_star_request import pack_count_star_request
from .pack_count_star_oracle import pack_count_star_result_oracle

@dataclass(frozen=True)
class PackCountStarCompileConfig:
    paths:CountStarDistributionPaths
    maximum_artifact_bytes:int
    def __post_init__(self):
        if type(self.paths)is not CountStarDistributionPaths or type(self.maximum_artifact_bytes)is not int or not 1<=self.maximum_artifact_bytes<=16*1024*1024:
            raise ValueError('Explicit installed distribution and finite artifact bound required')
        if self.paths.package is not None or any(type(p)is not type(Path('/'))or not p.is_absolute()for p in (self.paths.index,self.paths.output)):
            raise ValueError('Absolute existing installation and index paths required')

def compile_pack_count_star_cases(config,definition,pack,model,graph,bindings,manifest,registry,aliases,*,observe):
    """Compile/recompile all original selected cases; record bytes before parsing.

    Public offline admission and independent original source values are provisional
    compiler evidence. No publication reader/source/ACK authority is manufactured.
    """
    if type(config)is not PackCountStarCompileConfig or not callable(observe):raise ValueError('Typed configuration and original-byte observer required')
    if type(definition)is not FinitePackDefinition or definition.name not in ('archaeology','ecology'):raise ValueError('Owned original pack definition required')
    config.__post_init__()
    original=definition.cases(model,graph,pack)
    snapshot=source_snapshot([bindings,manifest,registry,aliases])
    # Build all requests before compiler/schema/file effects; malformed original
    # metadata cannot turn into a partially selected corpus.
    requests=[(name,pack_count_star_request(definition,name,pack,model,graph,*snapshot))for name,_ in original]
    prepared=[]
    for name,request in requests:
        raw=json.dumps(request,separators=(',',':'),ensure_ascii=False).encode()+b'\n'
        if len(raw)>config.maximum_artifact_bytes:raise ValueError('Complete original request resource bound exceeded')
        prepared.append((name,request,raw))
    schemas=dict(installed_count_star_schema_bundle(config.paths))
    selected={name:schemas[name]for name in ('compile-request-v0.4.1.schema.json','compile-response-v0.4.1.schema.json','logical-plan-v0.4.1.schema.json')}
    admission=CountStarAdmissionConfig(config.maximum_artifact_bytes,make_offline_count_star_schema_validation(selected))
    results=[];primary=None
    try:
        for name,request,raw in prepared:
            responses=[]
            for iteration in range(2):
                response=compile_count_star_distribution(config.paths,raw)
                observe(name,iteration,raw,response)
                if type(response)is not bytes or len(response)>config.maximum_artifact_bytes:raise ValueError('Original compiler response bound exceeded')
                responses.append(response)
            if responses[0]!=responses[1]:raise ValueError('Original fresh recompile differs')
            artifact=json.loads(responses[0]);admit_count_star_artifact(request,artifact,json.loads(responses[1]),config=admission)
            results.append({'id':name,'request':request,'response':artifact,'requestBytes':raw,
                            'responseBytes':responses[0],'sourceOracle':pack_count_star_result_oracle(definition,name,model,graph)})
    except BaseException as error:primary=error
    def closing():
        if source_snapshot([bindings,manifest,registry,aliases])!=snapshot:
            raise ValueError('Original closing input metadata differs')
    finish(primary,[closing])
    return {'cases':results,'qualification':'Complete exact original archaeology/ecology041 compiles/recompiles and public offline admission only; source oracles carry no publication/ACK/native authority'}
