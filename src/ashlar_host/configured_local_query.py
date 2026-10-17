"""Explicit configured local query selection and bounded report projection."""
from dataclasses import dataclass
import json
from ashlar.weft_decode import ExactScalar
from ashlar.weft_path_decode import PathDecodeConfig
from .configured_weft_compile import ConfiguredWeftCompileConfig
from .path_capture import PathCaptureConfig
from .configured_query_aliases import open_configured_query_aliases
from .configured_weft_query import query_configured_projection
from .lifecycle import owned_context


def _require(value):
    if not value:raise ValueError('Configured local query selection refused')


@dataclass(frozen=True)
class ConfiguredLocalQueryConfig:
    sql:str
    compile:ConfiguredWeftCompileConfig
    capture:PathCaptureConfig
    decoder:PathDecodeConfig
    def __post_init__(self):
        _require(type(self.sql)is str and 0<len(self.sql.encode())<=65536 and '\0'not in self.sql)
        _require(type(self.compile)is ConfiguredWeftCompileConfig and type(self.capture)is PathCaptureConfig and type(self.decoder)is PathDecodeConfig)
        self.compile.__post_init__();self.capture.__post_init__();self.decoder.__post_init__()
        _require(self.compile.maximum_artifact_bytes<=1048576 and self.capture.maximum_rows<=1000
                 and self.capture.maximum_cell_bytes<=65536 and self.capture.maximum_total_cell_bytes<=1048576
                 and self.decoder.maximum_cell_bytes==self.capture.maximum_cell_bytes)


def _report(value):
    if type(value)is ExactScalar:
        _require(type(value.value)in (int,str)and type(value.original)is str)
        return {'kind':'exact-integer'if type(value.value)is int else'exact-string','value':value.value,'original':value.original}
    if type(value)is dict:return {k:_report(v)for k,v in value.items()}
    if type(value)in (list,tuple):return [_report(v)for v in value]
    _require(value is None or type(value)in (str,int,bool))
    return value


def run_configured_local_query(scope,transport,query):
    """Return a provisional report; runtime/supervisor owners still must close."""
    _require(type(query)is ConfiguredLocalQueryConfig);query.__post_init__()
    originals=[]
    def observe(iteration,request,response):
        _require(iteration==len(originals)and iteration<2 and type(request)is bytes and type(response)is bytes)
        originals.append((request,response))
    with owned_context(open_configured_query_aliases(scope,transport))as aliases:
        result=query_configured_projection(scope,aliases,query.sql,query.compile,capture=query.capture,
                                            decoder=query.decoder,observe=observe)
    _require(len(originals)==2 and originals[0]==originals[1]
             and originals[0]==(result['requestBytes'],result['responseBytes'])
             and originals[1][1]==result['recompiledBytes'])
    artifact=json.loads(result['responseBytes'])
    return {'sql':query.sql,'columns':artifact['columns'],'rows':result['evidence']['native_result']['rows'],
            'protocol':{'request':result['requestBytes'].decode(),'response':result['responseBytes'].decode(),
                        'recompiled':result['recompiledBytes'].decode()},'evidence':_report(result['evidence'])}
