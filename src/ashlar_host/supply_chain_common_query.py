"""Fixed common graph reads and genuine041 execution; outer custody is required."""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path

from ashlar.graph_release import GraphRelease
from ashlar.weft_count_star_distribution import (CountStarDistributionPaths,
    compile_count_star_distribution,installed_count_star_schema_bundle)
from ashlar.weft_path_decode import PathDecodeConfig
from .count_star_admission import CountStarAdmissionConfig
from .count_star_execution import CountStarExecutionConfig,execute_commerce_count_star
from .count_star_schema import make_offline_count_star_schema_validation
from .delta_custody import encoded
from .exact_report import report_body,report_value
from .lifecycle import finish
from .path_capture import PathCaptureConfig
from .supply_chain_common_count_requests import prepare_supply_chain_common_count_requests
from .supply_chain_common_read import OpenedSupplyChainCommonRead
from .supply_chain_finite_source import UMF_REVISION
from .supply_chain_graph_release import read_original_supply_chain_graph_release
from .supply_chain_query import finite_public_source
from .supply_chain_reader import FiniteSupplyChainProvider

PROTOCOL_BYTES=8*1024*1024
VIEW_BYTES=1024*1024
PUBLIC_SOURCE_BYTES=16*1024*1024
REPORT_BYTES=64*1024*1024


@dataclass(frozen=True)
class SupplyChainCommonQueryConfig:
    distribution: CountStarDistributionPaths
    maximum_artifact_bytes: int
    capture: PathCaptureConfig
    decoder: PathDecodeConfig
    def __post_init__(self):
        if (type(self.distribution)is not CountStarDistributionPaths or self.distribution.package is not None
                or any(not isinstance(path,Path)or not path.is_absolute()for path in (self.distribution.index,self.distribution.output))
                or type(self.maximum_artifact_bytes)is not int or not 1<=self.maximum_artifact_bytes<=VIEW_BYTES
                or type(self.capture)is not PathCaptureConfig or type(self.decoder)is not PathDecodeConfig
                or self.capture.maximum_rows>128 or self.capture.maximum_cell_bytes>VIEW_BYTES
                or self.capture.maximum_total_cell_bytes>16*1024*1024
                or self.decoder.maximum_cell_bytes>VIEW_BYTES):
            raise ValueError('Explicit original041 installation and typed finite bounds required')
        self.capture.__post_init__();self.decoder.__post_init__()


def _config_values(config):
    config.__post_init__()
    return (config.distribution.index,config.distribution.output,config.distribution.package,
        config.maximum_artifact_bytes,config.capture.maximum_rows,config.capture.maximum_cell_bytes,
        config.capture.maximum_total_cell_bytes,config.decoder.maximum_cell_bytes)


@dataclass(frozen=True)
class _OriginalCountRead:
    """Actual common owner plus internally bound genuine per-query provider."""
    opened: OpenedSupplyChainCommonRead
    provider: FiniteSupplyChainProvider
    context: object
    model: bytes
    graph: bytes
    original_native_files: dict
    def native_files(self):
        if self.opened.require_current(self.context)is not None:
            raise PermissionError('Current original common source incomplete')
        return self.opened.native_files()


def _graph_report(release):
    if (type(release)is not GraphRelease or type(release.payload)is not bytes
            or hashlib.sha256(release.payload).hexdigest()!=release.sha256):
        raise ValueError('Exact original graph release report required')
    text=release.payload.decode('utf-8')
    # Preserve exact original payload text AND every publication/vector/carrier
    # field; no generic dataclass/string coercion or scalar promotion.
    return {'$type':'GraphRelease','sha256':release.sha256,
            'original_payload_text':text,'document':json.loads(text)}


def _report(graph,results,index,raw_bytes,index_bytes):
    return {'graph_release':_graph_report(graph),'queries':results,'protocol_index':index,
        'protocol_raw_bytes':raw_bytes,'protocol_index_bytes':index_bytes,
        'protocol_total_bytes':raw_bytes+index_bytes,
        'available_cases':['all-objects','all-edges','one-hop','singleton','filtered-limit','two-hop-count'],
        'required_pending':['grouped-count'],
        'qualification':'Provisional original fixed common native evidence only after inner full graph/currentsource/schema closing. Observer storage and outer actual common source/runtime/installation closing required before final persistence. No compiler/source/native/engine/ACK authority created.'}


DEFAULT_COMMON_CASE_IDS=tuple('one-hop-'+str(i)for i in range(13))+('singleton','filtered-total','filtered-limit')
COMMON_CASE_IDS=DEFAULT_COMMON_CASE_IDS+('two-hop-count',)


def query_supply_chain_common(opened,config,*,observe,selected_case_ids=None):
    """Execute fixed available cases; group-count remains separately required.

    Observers store provisional evidence only. Returned evidence is provisional
    until their original storage and the outer actual common owner close.
    """
    if type(opened)is not OpenedSupplyChainCommonRead or type(config)is not SupplyChainCommonQueryConfig or not callable(observe):
        raise ValueError('Actual original common read and typed query configuration required')
    if selected_case_ids is not None:
        if (type(selected_case_ids)not in (tuple,list)or not selected_case_ids
                or any(type(case)is not str or case not in COMMON_CASE_IDS for case in selected_case_ids)
                or len(set(selected_case_ids))!=len(selected_case_ids)):
            raise ValueError('Nonempty unique original common case IDs required')
        selected_ids=frozenset(selected_case_ids)
    else:selected_ids=frozenset(DEFAULT_COMMON_CASE_IDS)
    original_config=_config_values(config)
    selected=SupplyChainCommonQueryConfig(CountStarDistributionPaths(*original_config[:3]),
        original_config[3],PathCaptureConfig(*original_config[4:7]),PathDecodeConfig(original_config[7]))
    def config_current():
        if _config_values(config)!=original_config:
            raise ValueError('Original selected common query configuration changed')
    if opened.require_current(opened.context)is not None:
        raise PermissionError('Original current common reader incomplete')
    context=opened.context;source=opened.source;driver=opened.driver
    source_files=dict(opened.native_files());schemas=None;opening=None;results=[];index=[]
    raw_bytes=0;index_bytes=0;primary=None
    registry=[{'table':target.table,'uuid':target.uuid}for target in driver.transport.targets.values()]
    # All fixed requests are prepared before compiler/schema/observer effects.
    prepared=[]
    families=(('one-hop','relationships-0-6'),('one-hop','relationships-7-12'),
        ('singleton','complete'),('filtered-limit','complete'))
    inventory=DEFAULT_COMMON_CASE_IDS
    if 'two-hop-count'in selected_ids:
        families+=(('two-hop-count','complete'),);inventory+=('two-hop-count',)
    for family,part in families:
        batch=prepare_supply_chain_common_count_requests(opened.pack,opened.model,opened.graph,
            opened.bindings,opened.manifest,registry,opened.aliases,family=family,part=part)
        for query in batch['queries']:
            raw=(encoded(query['request'])+'\n').encode()
            if len(raw)>selected.maximum_artifact_bytes:
                raise ValueError('Complete original common request exceeds artifact capacity')
            prepared.append((family,part,query,raw))
    if len(prepared)!=len(inventory):raise ValueError('Complete fixed common query inventory required')
    prepared=[entry for case,entry in zip(inventory,prepared)if case in selected_ids]
    try:
        opening=read_original_supply_chain_graph_release(opened)
        schemas=dict(installed_count_star_schema_bundle(selected.distribution))
        selected_schemas={name:schemas[name]for name in ('compile-request-v0.4.1.schema.json',
            'compile-response-v0.4.1.schema.json','logical-plan-v0.4.1.schema.json')}
        execution=CountStarExecutionConfig(CountStarAdmissionConfig(selected.maximum_artifact_bytes,
            make_offline_count_star_schema_validation(selected_schemas)),selected.capture,selected.decoder,
            finite_public_source(source,PUBLIC_SOURCE_BYTES),UMF_REVISION,maximum_public_source_bytes=PUBLIC_SOURCE_BYTES)
        for ordinal,(family,part,query,raw)in enumerate(prepared):
            responses=[]
            for iteration in (0,1):
                config_current()
                response=compile_count_star_distribution(selected.distribution,raw)
                if type(response)is not bytes or len(response)>selected.maximum_artifact_bytes:
                    raise ValueError('Complete original common response exceeds artifact capacity')
                entry={'ordinal':ordinal,'family':family,'part':part,'query_ordinal':query['ordinal'],
                    'iteration':iteration,'request_bytes':len(raw),'request_sha256':hashlib.sha256(raw).hexdigest(),
                    'response_bytes':len(response),'response_sha256':hashlib.sha256(response).hexdigest()}
                serialized=(encoded(entry)+'\n').encode()
                if raw_bytes+len(raw)+len(response)+index_bytes+len(serialized)>PROTOCOL_BYTES:
                    raise ValueError('Complete original common protocol/index exceeds eight MiB')
                # Bound type/bytes and the complete index BEFORE observer use.
                observe(ordinal,iteration,raw,response)
                raw_bytes+=len(raw)+len(response);index_bytes+=len(serialized)
                index.append(entry);responses.append(response)
            if responses[0]!=responses[1]:
                raise ValueError('Original independent common041 recompilation differs')
            request=query['request']
            if opened.require_current(context)is not None:
                raise PermissionError('Original current common query owner incomplete')
            provider,fixed_request=opened.query_provider(context,family=family,part=part,ordinal=query['ordinal'])
            if fixed_request!=request:
                raise ValueError('Actual owning fixed query request differs')
            actual=_OriginalCountRead(opened,provider,context,opened.model,opened.graph,source_files)
            def oracle(model,graph,request_copy,q=query):
                if (model,graph)!=(opened.model,opened.graph)or request_copy!=q['request']:
                    raise ValueError('Original independent common oracle differs')
                return {'rows':q['expected_rows'],'witnesses':{'family':family,'part':part,
                    'original_edge_witnesses':q.get('original_edge_witnesses',[]),
                    'original_object_keys':q.get('original_object_keys',[]),'original_path_occurrences':q.get('original_path_occurrences',[])},
                    'scope':'Original complete source comparison only; actual native edge witness remains opening/closing graph release'}
            evidence=execute_commerce_count_star(actual,request,json.loads(responses[0]),json.loads(responses[1]),
                config=execution,original_oracle=oracle)
            result={'ordinal':ordinal,'family':family,'part':part,'query_ordinal':query['ordinal'],'case_id':('one-hop-'+str(query['ordinal'])if family=='one-hop'else 'singleton'if family=='singleton'else 'two-hop-count'if family=='two-hop-count'else 'filtered-total'if query['ordinal']==0 else 'filtered-limit'),'evidence':evidence}
            # Exact typed aggregate admission precedes retaining this result;
            # complete evidence is never shortened to fit.
            report_body(_report(opening,results+[result],index,raw_bytes,index_bytes),REPORT_BYTES)
            results.append(result)
    except BaseException as error:primary=error
    def current():
        if opened.require_current(context)is not None or opened.driver is not driver or opened.source is not source:
            raise PermissionError('Original common closing owner differs')
        if dict(opened.native_files())!=source_files:
            raise ValueError('Original complete common source files differ')
    def closing_graph():
        closing=read_original_supply_chain_graph_release(opened)
        if opening is not None and closing!=opening:
            raise ValueError('Original common closing full native graph release differs')
    def closing_schema():
        if schemas is not None and dict(installed_count_star_schema_bundle(selected.distribution))!=schemas:
            raise ValueError('Original installed common041 schemas differ')
    finish(primary,[config_current,current,closing_graph,closing_schema])
    if len(results)!=len(prepared) or len(index)!=2*len(prepared):raise ValueError('Complete original common corpus/index required')
    combined=_report(opening,results,index,raw_bytes,index_bytes)
    report_body(combined,REPORT_BYTES)
    return report_value(combined,REPORT_BYTES)
