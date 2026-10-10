"""Original five-query compiler command; input reports grant no native authority."""
import json
from pathlib import Path
from ashlar.weft_count_star_distribution import CountStarDistributionPaths
from .source import read_bounded
from .lifecycle import finish,owned_context
from .supply_chain_compile import SupplyChainCompileConfig,compile_supply_chain_cases

INPUT_LIMIT=1024*1024

def _metadata(raw):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate compiler metadata member')
            result[key]=value
        return result
    def integer(token):
        if len(token)>21:raise ValueError('Compiler metadata integer bound')
        return int(token)
    def refuse(token):raise ValueError('Exact compiler metadata values required')
    value=json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_float=refuse,parse_constant=refuse)
    if type(value)is not dict or set(value)!={'bindings','manifest','registry','aliases'}:
        raise ValueError('Closed compiler-only metadata required')
    return [value[k]for k in ('bindings','manifest','registry','aliases')]

def compile_supply_chain_command(*,pack,model,graph,metadata,index,installation,output,maximum_artifact_bytes):
    """Cooperating original files must remain byte-exact through closing.

    Retained raw observations precede parsing. They are compiler evidence only.
    Closing failure withholds the final report and preserves partial observations.
    """
    paths=[Path(p)for p in (pack,model,graph,metadata,index,installation,output)]
    if any(not p.is_absolute()or any(q.is_symlink()for q in (p,*p.parents))for p in paths):
        raise ValueError('Explicit absolute cooperating file paths required')
    pack,model,graph,metadata,index,installation,output=paths
    config=SupplyChainCompileConfig(CountStarDistributionPaths(index,installation),maximum_artifact_bytes)
    originals=[(p,read_bounded(p,INPUT_LIMIT))for p in (pack,model,graph,metadata)]
    borrowed=_metadata(originals[3][1])
    # Validate all original requests before allocating output or compiler effects.
    from .supply_chain_request import supply_chain_cases,supply_chain_count_star_request
    raw=[b for _,b in originals[:3]]
    for name,_ in supply_chain_cases(*raw):
        supply_chain_count_star_request(name,*raw,*borrowed)
    output.mkdir(mode=0o700)
    primary=None;result=None;observed=0
    try:
        with owned_context((output/'observations.jsonl').open('xb'))as log:
            def observe(name,iteration,request,response):
                nonlocal observed
                if any(type(body)is not bytes or len(body)>maximum_artifact_bytes for body in (request,response)):
                    raise ValueError('Compiler observation bound')
                observed+=2*(len(request)+len(response))+256
                if observed>32*1024*1024:raise ValueError('Complete compiler observation bound')
                for suffix,body in (('request',request),('response',response)):
                    with owned_context((output/(name+'-'+str(iteration)+'.'+suffix)).open('xb'))as stream:
                        if stream.write(body)!=len(body):raise ValueError('Compiler observation write refused')
                record=(json.dumps({'id':name,'iteration':iteration,'requestHex':request.hex(),'responseHex':response.hex()},separators=(',',':'))+'\n').encode()
                if log.write(record)!=len(record):raise ValueError('Compiler observation write refused')
                log.flush()
            result=compile_supply_chain_cases(config,*raw,*borrowed,observe=observe)
    except BaseException as error:primary=error
    def renew():
        if any(read_bounded(p,INPUT_LIMIT)!=b for p,b in originals):raise ValueError('Original closing compiler inputs differ')
    finish(primary,[renew])
    if sum(2*len(value)for row in result['cases']for value in row.values()if type(value)is bytes)>32*1024*1024:
        raise ValueError('Compiler report bound')
    report={'cases':[{k:(v.hex()if type(v)is bytes else v)for k,v in row.items()}for row in result['cases']],
            'qualification':result['qualification']}
    body=bytearray()
    for chunk in json.JSONEncoder(separators=(',',':'),ensure_ascii=True).iterencode(report):
        raw_chunk=chunk.encode()
        if len(body)+len(raw_chunk)+1>32*1024*1024:raise ValueError('Compiler report bound')
        body.extend(raw_chunk)
    body.extend(b'\n')
    with owned_context((output/'report.json').open('xb'))as stream:
        if stream.write(body)!=len(body):raise ValueError('Compiler report write refused')
    return {'cases':5,'report':str(output/'report.json'),'qualification':result['qualification']}
