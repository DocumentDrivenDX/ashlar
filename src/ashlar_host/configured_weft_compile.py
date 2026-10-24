# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded public041 compilation/admission inside an original local read scope.

Compilation and offline artifact admission do not discharge source/native query
obligations. Exact bytes are observed before parsing, including refused output.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from ashlar.schema import decode_retained_json
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar.weft_count_star_distribution import CountStarDistributionPaths,compile_count_star_distribution,installed_count_star_schema_bundle
from ashlar.weft_current_distribution import CurrentWeftDistributionPaths,CurrentWeftDistribution
from .configured_weft_binding import configured_count_star_request
from .count_star_admission import CountStarAdmissionConfig,SCHEMA_SHA256,CURRENT_SCHEMA_SHA256,CURRENT_BACKEND,admit_count_star_artifact
from .count_star_schema import make_offline_count_star_schema_validation,make_offline_current_count_star_schema_validation
from .lifecycle import finish


def _require(value):
    if not value:raise ValueError('Configured Weft compilation refused')

@dataclass(frozen=True)
class ConfiguredWeftCompileConfig:
    paths:CountStarDistributionPaths|CurrentWeftDistributionPaths
    maximum_artifact_bytes:int
    def __post_init__(self):
        _require(type(self.paths) in (CountStarDistributionPaths,CurrentWeftDistributionPaths))
        if type(self.paths)is CountStarDistributionPaths:_require(self.paths.package is None)
        else:self.paths.__post_init__()
        selected=(self.paths.index,self.paths.output)if type(self.paths)is CountStarDistributionPaths else (self.paths.index,)
        _require(all(type(p)is type(Path())and p.is_absolute()for p in selected))
        _require(type(self.maximum_artifact_bytes)is int and 1<=self.maximum_artifact_bytes<=16777216)


def compile_configured_count_star(scope,aliases,sql,config,*,observe):
    """Two exact compiles, installed offline schemas and all-outcome closing.

    observe(iteration,request_bytes,response_bytes) persists private raw evidence;
    it must return None. Partial observations on refusal are never a result.
    """
    _require(type(config)is ConfiguredWeftCompileConfig and callable(observe));config.__post_init__()
    request=configured_count_star_request(scope,aliases,sql,layout_sha256=LAYOUT_SHA256)
    target=request.get('target',{})
    current=all(target.get(k)==CURRENT_BACKEND[k]for k in ('backendId','backendVersion','targetProfile'))
    _require((type(config.paths)is CurrentWeftDistributionPaths)==current)
    alias_original=dict(aliases)
    raw=json.dumps(request,ensure_ascii=False,separators=(',',':')).encode()+b'\n';_require(len(raw)<=config.maximum_artifact_bytes)
    responses=[];primary=None;result=None;schemas=None
    try:
        if current:
            compiler=CurrentWeftDistribution(config.paths)
            schemas=compiler.schema_bundle()
            selected={name:schemas[name]for name in CURRENT_SCHEMA_SHA256}
            admission=CountStarAdmissionConfig(config.maximum_artifact_bytes,make_offline_current_count_star_schema_validation(selected),profile='count-star-current')
            compile_once=lambda raw:compiler.compile_paths_keys_json(raw.decode('utf-8')).encode('utf-8')
        else:
            schemas=dict(installed_count_star_schema_bundle(config.paths))
            selected={name:schemas[name]for name in SCHEMA_SHA256}
            admission=CountStarAdmissionConfig(config.maximum_artifact_bytes,make_offline_count_star_schema_validation(selected))
            compile_once=lambda raw:compile_count_star_distribution(config.paths,raw)
        for iteration in range(2):
            response=compile_once(raw)
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
        if schemas is not None:
            observed=compiler.schema_bundle()if current else dict(installed_count_star_schema_bundle(config.paths))
            _require(observed==schemas)
            if current:_require(compiler._verify())
    finish(primary,[scope.renew,request_closing,schemas_closing])
    return result


def compile_configured_count_star_053(scope,aliases,sql,paths,*,observe):
    """Compile/admit with the separately pinned public Weft 0.4.53 profile.

    This path never invokes SQL. Callers can persist and review its exact bytes
    while the supplied aliases remain live before using another execution port.
    """
    from ashlar.weft_053_distribution import Weft053Distribution,Weft053Paths
    from .count_star_admission import CURRENT_053_SCHEMA_SHA256
    _require(type(paths)is Weft053Paths and callable(observe))
    request=configured_count_star_request(scope,aliases,sql,layout_sha256=LAYOUT_SHA256)
    target=request.get('target',{})
    _require(all(target.get(k)==CURRENT_BACKEND[k]for k in ('backendId','backendVersion','targetProfile')))
    alias_original=dict(aliases)
    raw=json.dumps(request,ensure_ascii=False,separators=(',',':')).encode()+b'\n'
    _require(len(raw)<=1048576)
    compiler=Weft053Distribution(paths);schemas=compiler.schema_bundle()
    selected={name:schemas[name]for name in CURRENT_053_SCHEMA_SHA256}
    from .count_star_schema import make_offline_current_count_star_schema_validation
    admission=CountStarAdmissionConfig(1048576,
        make_offline_current_count_star_schema_validation(selected,profile='count-star-current-053'),
        profile='count-star-current-053')
    responses=[];primary=None;result=None
    try:
        for iteration in range(2):
            response=compiler.compile_paths_keys_json(raw.decode('utf-8')).encode('utf-8')
            _require(observe(iteration,raw,response)is None and 0<len(response)<=1048576)
            responses.append(response)
        _require(responses[0]==responses[1])
        artifact=decode_retained_json(responses[0]);recompiled=decode_retained_json(responses[1])
        admitted=admit_count_star_artifact(request,artifact,recompiled,config=admission)
        result={'request':request,'response':artifact,'recompiled':recompiled,'requestBytes':raw,
            'responseBytes':responses[0],'recompiledBytes':responses[1],
            'admission_config':admission,'admitted':admitted,'compilerVersion':'0.4.53'}
    except BaseException as exc:primary=exc
    def request_closing():
        _require(aliases==alias_original and configured_count_star_request(scope,aliases,sql,layout_sha256=LAYOUT_SHA256)==request)
    def schemas_closing():
        _require(compiler.schema_bundle()==schemas and compiler._verify())
    finish(primary,[scope.renew,request_closing,schemas_closing])
    return result
