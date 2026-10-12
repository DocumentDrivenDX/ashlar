"""Generate the supported local supply-chain invocation without engine I/O."""
import json
from pathlib import Path

from .delta_custody import LOCAL_OPERATION_CAPACITY_8M
from .supply_chain_native_cli import PROFILE_BINARY,parse_supply_chain_native_invocation


def make_supply_chain_native_configuration(*,output,jars,pack,model,graph,
        producer_source,bun,git,count_star_index,count_star_installation,
        paths_index,paths_installation):
    """Return parser-validated 0.3 JSON; selected resources are admitted at run time."""
    paths=locals().copy()
    if any(type(value)not in (str,type(Path('/')))for value in paths.values()):
        raise ValueError('Supply-chain configuration paths must be strings or Paths')
    paths={key:str(value)for key,value in paths.items()}
    value={'profile':PROFILE_BINARY,
        **{key:paths[key]for key in ('output','jars','pack','model','graph')},
        'producer':{'source':paths['producer_source'],'bun':paths['bun'],'git':paths['git'],
            'timeout_seconds':30,'maximum_output_bytes':1048576,'maximum_receipt_bytes':8388608},
        'compiler':{'index':paths['count_star_index'],'installation':paths['count_star_installation']},
        'operation_capacity':dict(LOCAL_OPERATION_CAPACITY_8M),
        'two_hop':{'distribution':{'index':paths['paths_index'],'installation':paths['paths_installation']},
            'maximum_artifact_bytes':1048576,
            'capture':{'maximum_rows':128,'maximum_cell_bytes':1048576,'maximum_total_cell_bytes':16777216},
            'decoder':{'maximum_cell_bytes':1048576}},
        'compiler_evidence_format':'binary-protocol/0.1'}
    raw=(json.dumps(value,indent=2,ensure_ascii=False,allow_nan=False)+'\n').encode('utf-8')
    parse_supply_chain_native_invocation(raw)
    return raw
