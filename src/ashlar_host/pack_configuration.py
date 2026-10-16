"""Print existing finite-pack invocation configuration without engine/source I/O.

The closed developer budget selects finite bounds, not authority or resources.
The actual publisher/query owns subsequent model, source, compiler and runtime
admission. Supply-chain retains its separately named configuration generator.
"""
from __future__ import annotations
from dataclasses import dataclass
import json
from pathlib import Path
from .delta_custody import LOCAL_OPERATION_CAPACITY_8M, bounded_local_json_bytes
from .pack_native_cli import CONFIG_LIMIT, PROFILE, SOURCE_PROFILE, parse_finite_pack_invocation


@dataclass(frozen=True)
class FinitePackBudget:
    """Exactly the small-local developer preset; no caller-edited limit map."""
    profile: str

    def __post_init__(self) -> None:
        if type(self.profile) is not str or self.profile != 'small-local':
            raise ValueError('Closed finite-pack budget profile required')


def make_finite_pack_configuration(*, pack: str, mode: str, profile: str,
        budget: FinitePackBudget, publication: Path, output: Path, jars: Path,
        pack_file: Path, model: Path, graph: Path, producer_source: Path,
        bun: Path, git: Path, index: Path | None = None,
        installation: Path | None = None, query_profile: str | None = None) -> bytes:
    """Return immutable JSON admitted by the existing owning typed parser.

    Publish: archaeology/ecology/payments. Generic query: archaeology/ecology.
    Payments queries require their existing explicit count/scenario route.
    No compiler selection is accepted for publication.
    """
    if type(budget) is not FinitePackBudget:
        raise ValueError('Exact finite-pack budget required')
    budget.__post_init__()
    if mode == 'publish' and any(value is not None for value in (index, installation, query_profile)):
        raise ValueError('Publication has no query-only selection')
    if mode == 'query' and (index is None or installation is None):
        raise ValueError('Query compiler selection required')
    selected = {name: value for name, value in (
        ('publication',publication),('output',output),('jars',jars),('pack_file',pack_file),
        ('model',model),('graph',graph),('producer_source',producer_source),('bun',bun),('git',git),
        ('index',index),('installation',installation)) if value is not None}
    if any(type(value) not in (str,type(Path('/'))) for value in selected.values()):
        raise ValueError('Explicit selected paths required')
    paths = {name: str(value) for name,value in selected.items()}
    value = {'profile':profile,'mode':mode,'pack':pack,
        **{name:paths[name] for name in ('publication','output','jars','pack_file','model','graph')},
        'producer':{'source':paths['producer_source'],'bun':paths['bun'],'git':paths['git'],
                    'timeout_seconds':30,'maximum_output_bytes':1048576,'maximum_receipt_bytes':33554432},
        'operation_capacity':dict(LOCAL_OPERATION_CAPACITY_8M)}
    if mode == 'query':
        value['query'] = {'index':paths['index'],'installation':paths['installation'],
            'maximum_artifact_bytes':1048576,'maximum_rows':128,'maximum_cell_bytes':1048576,
            'maximum_total_cell_bytes':16777216,'maximum_observation_bytes':16777216}
        if profile == SOURCE_PROFILE:
            value['query']['maximum_public_source_bytes'] = 2097152
    bounded_local_json_bytes(value, CONFIG_LIMIT)
    raw = json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')
    parse_finite_pack_invocation(raw, mode, query_profile=query_profile)
    return raw + b'\n' if len(raw) < CONFIG_LIMIT else raw


def make_finite_pack_export_configuration(*, pack: str, budget: FinitePackBudget,
        publication: Path, output: Path, jars: Path, pack_file: Path, model: Path,
        graph: Path, producer_source: Path, bun: Path, git: Path) -> bytes:
    """Generate the existing compiler-free export profile; no engine startup.

    Output selects the prospective export destination for separation validation;
    the existing export command receives that destination separately.
    """
    from .finite_export_cli import EXPORT_PROFILE, parse_finite_export_configuration
    raw = make_finite_pack_configuration(pack=pack,mode='publish',profile=PROFILE,
        budget=budget,publication=publication,output=output,jars=jars,
        pack_file=pack_file,model=model,graph=graph,producer_source=producer_source,
        bun=bun,git=git)
    value=json.loads(raw)
    value.pop('mode');value.pop('output');value['profile']=EXPORT_PROFILE
    raw=json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')
    bounded_local_json_bytes(value,CONFIG_LIMIT)
    parse_finite_export_configuration(raw,Path(output))
    return raw+b'\n' if len(raw)<CONFIG_LIMIT else raw
