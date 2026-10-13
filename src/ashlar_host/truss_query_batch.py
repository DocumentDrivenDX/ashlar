"""One existing publication owner for a finite gallery of unchanged Weft queries."""
from dataclasses import dataclass
import json,os
from pathlib import Path
from ashlar.weft_distribution import DistributionPaths
from .delta_custody import encoded
from .evolution_cli import InvocationFile
from .exact_report import report_body
from .lifecycle import owned_context
from .truss_recovery import restore_applied_truss_publication
from .truss_resume_cli import run_selected_truss_command
from .truss_weft_query import execute_truss_string_query

MAXIMUM_REPORT_BYTES=64*1048576

@dataclass(frozen=True)
class TrussBatchQuery:
    name: str
    sql: str
    revision: str
    document_id: str
    maximum_rows: int

@dataclass(frozen=True)
class TrussQueryBatch:
    queries: tuple


def parse_truss_query_batch(raw):
    if type(raw)is not bytes or not 0<len(raw)<=65536:raise ValueError('Bounded original query batch required')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate query batch member')
            result[key]=value
        return result
    value=json.loads(raw,object_pairs_hook=pairs)
    if type(value)is not dict or set(value)!={'profile','queries'}or value['profile']!='ashlar-truss-query-batch/0.1'or type(value['queries'])is not list or not 1<=len(value['queries'])<=8:raise ValueError('Closed finite Truss query batch required')
    queries=[];names=set();capacity=0
    for query in value['queries']:
        if type(query)is not dict or set(query)!={'name','sql','revision','document_id','maximum_rows'}:raise ValueError('Closed named query selection required')
        for key,limit in (('name',128),('sql',65536),('revision',4096),('document_id',4096)):
            text=query[key]
            if type(text)is not str or not text or '\0'in text or len(text.encode())>limit:raise ValueError('Bounded original query strings required')
        if query['name']in names or type(query['maximum_rows'])is not int or not 1<=query['maximum_rows']<=10000:raise ValueError('Unique names and finite query row bounds required')
        names.add(query['name']);capacity+=query['maximum_rows']
        if capacity>10000:raise ValueError('Batch aggregate row capacity exceeds 10000')
        queries.append(TrussBatchQuery(**query))
    return TrussQueryBatch(tuple(queries))


def run_query_truss_graph_batch_command(configuration,output,*,index,installation,batch):
    original=InvocationFile.read(Path(batch),65536);selected=parse_truss_query_batch(original.raw)
    paths=DistributionPaths(Path(index),Path(installation));destination=Path(output)
    for path,directory in ((paths.index,False),(paths.output,True)):
        if not path.is_absolute()or path.resolve()!=path or not(path.is_dir()if directory else path.is_file()):raise ValueError('Explicit existing public compiler paths required')
    if not destination.is_absolute()or any(destination==path or destination in path.parents or path in destination.parents for path in (original.path,paths.index,paths.output)):
        raise ValueError('Separate query batch output required')
    def execute(driver,config):
        with owned_context(driver.writer(driver.source_sessions.batch().feed,driver.context)):
            restore_applied_truss_publication(driver,config['request_digest'])
        manifest=encoded(driver.policy.active['manifest']);publication_id=driver.policy.active['manifest']['publication_id']
        results=[];protocol_bytes=0;rows=0
        for query in selected.queries:
            original.renew(65536)
            if encoded(driver.policy.active['manifest'])!=manifest:raise ValueError('Original batch publication changed')
            answer=execute_truss_string_query(driver,paths,query.sql,revision=query.revision,document_id=query.document_id,maximum_rows=query.maximum_rows)
            if answer['publication_id']!=publication_id or encoded(driver.policy.active['manifest'])!=manifest:raise ValueError('Mixed query publications refused')
            request=answer['request_bytes'];response=answer['response_bytes']
            if any(type(value)is not bytes for value in (request,response))or type(answer['rows'])is not list or len(answer['rows'])>query.maximum_rows:raise ValueError('Complete bounded original query protocol and rows required')
            protocol_bytes+=len(request)+len(response);rows+=len(answer['rows'])
            if protocol_bytes>MAXIMUM_REPORT_BYTES or rows>10000:raise ValueError('Complete batch exceeds aggregate bounds')
            report={key:value for key,value in answer.items()if key not in ('request_bytes','response_bytes')}
            report.update(name=query.name,request_original_json=request.decode('utf8'),response_original_json=response.decode('utf8'))
            results.append(report)
            candidate={'profile':'ashlar-truss-query-batch-result/0.1','publication_id':publication_id,'manifest_original_json':manifest,'queries':results,'protocol_bytes':protocol_bytes,'rows':rows}
            report_body(candidate,MAXIMUM_REPORT_BYTES)
        return candidate
    def write(result,path):
        original.renew(65536)
        raw=report_body(result,MAXIMUM_REPORT_BYTES)
        with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600),'wb')as stream:
            if stream.write(raw)!=len(raw):raise OSError('Incomplete private batch report')
            stream.flush();os.fsync(stream.fileno())
        return result
    return run_selected_truss_command(configuration,output,execute,report_writer=write)
