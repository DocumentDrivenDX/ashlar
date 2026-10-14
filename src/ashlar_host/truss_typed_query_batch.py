"""Compose admitted typed query families over one original publication owner."""
from __future__ import annotations
import json,os
from dataclasses import dataclass
from pathlib import Path
from ashlar.weft_implicit_arithmetic_distribution import ImplicitArithmeticDistributionPaths
from .weft_compatible_cli import CompatibleCompiler
from .delta_custody import encoded
from .evolution_cli import InvocationFile
from .exact_report import report_body
from .lifecycle import owned_context
from .truss_recovery import restore_applied_truss_publication
from .truss_resume_cli import run_selected_truss_command
from .truss_weft_query import (execute_truss_scalar_query,
    execute_truss_numeric_grouped_count_query,execute_truss_two_hop_count_query)

MAXIMUM_REPORT_BYTES=64*1048576
FAMILIES={'scalar','numeric-grouped-count','two-hop-count'}


@dataclass(frozen=True)
class CompatibleSelection:
    executable: str

@dataclass(frozen=True)
class IndexedSelection:
    index: str
    installation: str

@dataclass(frozen=True)
class TypedBatchQuery:
    name: str
    family: str
    sql: str
    revision: str
    document_id: str
    maximum_rows: int
    physical_revisions: tuple = ()

@dataclass(frozen=True)
class TrussTypedQueryBatch:
    queries: tuple
    compatible: CompatibleSelection | None
    indexed: IndexedSelection | None


def parse_truss_typed_query_batch(raw):
    if type(raw)is not bytes or not 0<len(raw)<=65536:raise ValueError('Bounded original typed query batch required')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate typed batch member')
            result[key]=value
        return result
    value=json.loads(raw,object_pairs_hook=pairs)
    if (type(value)is not dict or set(value)!={'profile','compilers','queries'}
            or value['profile']!='ashlar-truss-typed-query-batch/0.1'
            or type(value['compilers'])is not dict
            or type(value['queries'])is not list or not 1<=len(value['queries'])<=8):
        raise ValueError('Closed typed query batch required')
    required={'indexed'if type(q)is dict and q.get('family')=='two-hop-count'else'compatible'for q in value['queries']}
    if set(value['compilers'])!=required:raise ValueError('Exactly selected compiler transports required')
    for family in required:
        keys={'executable'}if family=='compatible'else{'index','installation'}
        selection=value['compilers'][family]
        if type(selection)is not dict or set(selection)!=keys:raise ValueError('Explicit compiler family selections required')
        for name,text in selection.items():
            if type(text)is not str or not text or '\0'in text or len(text.encode())>4096:raise ValueError('Bounded compiler path required')
    names=set();capacity=0
    for query in value['queries']:
        if type(query)is not dict or query.get('family')not in FAMILIES:raise ValueError('Admitted typed batch family required')
        expected={'name','family','sql','revision','document_id','maximum_rows'}
        if query['family']!='two-hop-count':expected.add('physical_revisions')
        if set(query)!=expected:raise ValueError('Exact family query selections required')
        for key,limit in (('name',128),('sql',65536),('revision',4096),('document_id',4096)):
            text=query[key]
            if type(text)is not str or not text or '\0'in text or len(text.encode())>limit:raise ValueError('Bounded original query strings required')
        if query['name']in names or type(query['maximum_rows'])is not int or not 1<=query['maximum_rows']<=10000:raise ValueError('Unique names and finite row bounds required')
        if 'physical_revisions'in query:
            revisions=query['physical_revisions']
            if (type(revisions)is not list or not 1<=len(revisions)<=8
                    or any(type(r)is not str or not r or '\0'in r or len(r.encode())>4096 for r in revisions)
                    or len(set(revisions))!=len(revisions)):raise ValueError('Explicit distinct physical revisions required')
        names.add(query['name']);capacity+=query['maximum_rows']
        if capacity>10000:raise ValueError('Batch aggregate row capacity exceeds 10000')
    return TrussTypedQueryBatch(
        tuple(TypedBatchQuery(**{**q,'physical_revisions':tuple(q.get('physical_revisions',()))})for q in value['queries']),
        CompatibleSelection(**value['compilers']['compatible'])if 'compatible'in required else None,
        IndexedSelection(**value['compilers']['indexed'])if 'indexed'in required else None)


def run_query_truss_typed_batch_command(configuration,output,*,batch):
    original=InvocationFile.read(Path(batch),65536);selected=parse_truss_typed_query_batch(original.raw)
    destination=Path(output);paths=[];compatible=None;indexed=None
    if selected.compatible is not None:
        executable=Path(selected.compatible.executable);paths.append((executable,False))
    if selected.indexed is not None:
        index=Path(selected.indexed.index);installation=Path(selected.indexed.installation)
        paths.extend(((index,False),(installation,True)))
    for path,directory in paths:
        if not path.is_absolute()or path.resolve()!=path or not(path.is_dir()if directory else path.is_file()):raise ValueError('Canonical existing public compiler paths required')
    if (not destination.is_absolute()or destination.resolve()!=destination
            or any(destination==path or destination in path.parents or path in destination.parents
                for path in (original.path,*(path for path,_ in paths)))):raise ValueError('Separate canonical typed batch output required')
    if selected.compatible is not None:compatible=CompatibleCompiler(executable)
    if selected.indexed is not None:indexed=ImplicitArithmeticDistributionPaths(index,installation)
    def execute(driver,config):
        with owned_context(driver.writer(driver.source_sessions.batch().feed,driver.context)):
            restore_applied_truss_publication(driver,config['request_digest'])
        manifest=encoded(driver.policy.active['manifest']);publication=driver.policy.active['manifest']['publication_id']
        results=[];protocol_bytes=0;rows=0
        for query in selected.queries:
            original.renew(65536)
            if encoded(driver.policy.active['manifest'])!=manifest:raise ValueError('Original batch manifest changed')
            kwargs={key:getattr(query,key)for key in ('revision','document_id','maximum_rows')}
            family=query.family
            if family=='two-hop-count':answer=execute_truss_two_hop_count_query(driver,indexed,query.sql,**kwargs)
            else:
                kwargs['physical_revisions']=tuple(query.physical_revisions)
                port=execute_truss_scalar_query if family=='scalar'else execute_truss_numeric_grouped_count_query
                answer=port(driver,compatible,query.sql,**kwargs)
            if answer['publication_id']!=publication or encoded(driver.policy.active['manifest'])!=manifest:raise ValueError('Mixed full publication manifests refused')
            fields=('request_bytes','response_bytes','recompiled_bytes')if family=='two-hop-count'else('request_bytes','response_bytes')
            if any(type(answer.get(key))is not bytes for key in fields)or type(answer.get('rows'))is not list or len(answer['rows'])>query.maximum_rows:raise ValueError('Complete bounded original protocol and rows required')
            if family!='two-hop-count'and 'recompiled_bytes'in answer:raise ValueError('Unexpected compatible recompile claim')
            protocol_bytes+=sum(len(answer[key])for key in fields);rows+=len(answer['rows'])
            if protocol_bytes>MAXIMUM_REPORT_BYTES or rows>10000:raise ValueError('Complete typed batch exceeds aggregate bounds')
            report={key:value for key,value in answer.items()if key not in fields}
            report.update(name=query.name,family=family)
            for key in fields:report[key.replace('_bytes','_original_json')]=answer[key].decode('utf8')
            results.append(report)
            candidate={'profile':'ashlar-truss-typed-query-batch-result/0.1','publication_id':publication,'manifest_original_json':manifest,'queries':results,'protocol_bytes':protocol_bytes,'rows':rows}
            report_body(candidate,MAXIMUM_REPORT_BYTES)
        original.renew(65536)
        if encoded(driver.policy.active['manifest'])!=manifest:raise ValueError('Closing typed batch manifest changed')
        return candidate
    def write(result,path):
        original.renew(65536);raw=report_body(result,MAXIMUM_REPORT_BYTES)
        with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600),'wb')as stream:
            if stream.write(raw)!=len(raw):raise OSError('Incomplete private typed batch report')
            stream.flush();os.fsync(stream.fileno())
        return result
    return run_selected_truss_command(configuration,output,execute,report_writer=write)
