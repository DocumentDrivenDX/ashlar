"""Bounded public041 compilation/admission inside an original local read scope.

Compilation and offline artifact admission do not discharge source/native query
obligations. Exact bytes are observed before parsing, including refused output.
"""
from dataclasses import dataclass
from pathlib import Path
import json
from ashlar.schema import decode_retained_json
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar.weft_count_star_distribution import CountStarDistributionPaths,compile_count_star_distribution,installed_count_star_schema_bundle
from .configured_weft_binding import configured_count_star_request
from .count_star_admission import CountStarAdmissionConfig,SCHEMA_SHA256,admit_count_star_artifact
from .count_star_schema import make_offline_count_star_schema_validation
from .lifecycle import finish


def _require(value):
    if not value:raise ValueError('Configured Weft compilation refused')

@dataclass(frozen=True)
class ConfiguredWeftCompileConfig:
    paths:CountStarDistributionPaths
    maximum_artifact_bytes:int
    def __post_init__(self):
        _require(type(self.paths)is CountStarDistributionPaths and self.paths.package is None)
        _require(all(type(p)is type(Path())and p.is_absolute()for p in (self.paths.index,self.paths.output)))
        _require(type(self.maximum_artifact_bytes)is int and 1<=self.maximum_artifact_bytes<=16777216)


def compile_configured_count_star(scope,aliases,sql,config,*,observe):
    """Two exact compiles, installed offline schemas and all-outcome closing.

    observe(iteration,request_bytes,response_bytes) persists private raw evidence;
    it must return None. Partial observations on refusal are never a result.
    """
    _require(type(config)is ConfiguredWeftCompileConfig and callable(observe));config.__post_init__()
    request=configured_count_star_request(scope,aliases,sql,layout_sha256=LAYOUT_SHA256)
    alias_original=dict(aliases)
    raw=json.dumps(request,ensure_ascii=False,separators=(',',':')).encode()+b'\n';_require(len(raw)<=config.maximum_artifact_bytes)
    responses=[];primary=None;result=None;schemas=None
    try:
        schemas=dict(installed_count_star_schema_bundle(config.paths))
        selected={name:schemas[name]for name in SCHEMA_SHA256}
        admission=CountStarAdmissionConfig(config.maximum_artifact_bytes,make_offline_count_star_schema_validation(selected))
        for iteration in range(2):
            response=compile_count_star_distribution(config.paths,raw)
            _require(observe(iteration,raw,response)is None)
            _require(type(response)is bytes and 0<len(response)<=config.maximum_artifact_bytes);responses.append(response)
        _require(responses[0]==responses[1])
        artifact=decode_retained_json(responses[0]);recompiled=decode_retained_json(responses[1])
        admitted=admit_count_star_artifact(request,artifact,recompiled,config=admission)
        result={'request':request,'response':artifact,'recompiled':recompiled,'requestBytes':raw,'responseBytes':responses[0],'recompiledBytes':responses[1],'admission_config':admission,'admitted':admitted}
    except BaseException as exc:primary=exc
    def request_closing():
        _require(aliases==alias_original and configured_count_star_request(scope,aliases,sql,layout_sha256=LAYOUT_SHA256)==request)
    def schemas_closing():
        if schemas is not None:_require(dict(installed_count_star_schema_bundle(config.paths))==schemas)
    finish(primary,[scope.renew,request_closing,schemas_closing])
    return result
