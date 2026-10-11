"""Four original commerce queries through fixed indexed 042 public ports.

Evidence is provisional: caller owns aliases, reader/Spark lifetime and complete
closing installation/source/publication custody before any final report release.
"""
from ashlar.weft_implicit_arithmetic_distribution import (
    ImplicitArithmeticDistributionPaths,compile_implicit_arithmetic_distribution)
from .implicit_arithmetic_request import commerce_implicit_arithmetic_request
from .supply_chain_request import source_snapshot
from .commerce_original_scenarios import commerce_original_cases,original_commerce_scenario_oracle
from .implicit_arithmetic_profile import ImplicitArithmeticExecutionConfig
from .count_star_execution import execute_commerce_implicit_arithmetic
from .commerce_source_receipt import encode_bounded_commerce_json,parse_commerce_json


def execute_original_commerce_implicit_arithmetic_cases(opened,paths,*,config:ImplicitArithmeticExecutionConfig,pack_bytes):
    """Return all four provisional cases or refuse; no final lifecycle claim."""
    if type(paths)is not ImplicitArithmeticDistributionPaths or type(config)is not ImplicitArithmeticExecutionConfig:
        raise ValueError('Exact selected distribution and execution configuration required')
    config.__post_init__()
    cases=commerce_original_cases(pack_bytes)
    maximum=config.admission.maximum_artifact_bytes
    if type(opened.model)is not bytes or type(opened.graph)is not bytes:raise ValueError('Exact original source bytes required')
    model,graph=opened.model,opened.graph
    bindings,manifest,registry,aliases=(source_snapshot(v)for v in (opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases))
    result=[]
    for name,sql in cases:
        request=commerce_implicit_arithmetic_request(sql,model,bindings,manifest,registry,aliases,pack_bytes=pack_bytes)
        raw=(encode_bounded_commerce_json(request,maximum-1)+'\n').encode('utf8')
        response=compile_implicit_arithmetic_distribution(paths,raw)
        artifact=parse_commerce_json(response,maximum)
        recompiled_raw=compile_implicit_arithmetic_distribution(paths,raw)
        recompiled=parse_commerce_json(recompiled_raw,maximum)
        opened.provider.select_original_binding(request,context=opened.context)
        provisional=execute_commerce_implicit_arithmetic(opened,request,artifact,recompiled,
            config=config,original_oracle=original_commerce_scenario_oracle)
        result.append({'case':name,'originalRequestText':raw.decode('utf8'),
            'originalResponseText':response.decode('utf8'),'originalRecompiledText':recompiled_raw.decode('utf8'),
            'provisional':provisional})
    return tuple(result)
