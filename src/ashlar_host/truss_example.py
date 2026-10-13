"""Portable, finite public Truss → Delta → Weft example; existing runtimes only."""
import json,os,sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from .lifecycle import finish,owned_context
PROFILE='ashlar-local-truss-example/0.1'

@dataclass(frozen=True)
class TrussExampleConfiguration:
    profile: str
    bun: Path
    java_home: Path
    jars: tuple
    weft_index: Path
    weft_installation: Path
    export_graph: bool = False
    destination_id: Optional[str] = None
    query_batch: Optional[Path] = None
    exercise_mutations: bool = False



def preflight(configuration,output):
    configuration=Path(configuration);output=Path(output)
    if not configuration.is_absolute()or configuration.resolve()!=configuration:raise ValueError('Canonical configuration required')
    from .evolution_cli import InvocationFile
    original=InvocationFile.read(configuration,65536)
    raw=original.raw
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate configuration key')
            result[key]=value
        return result
    config=json.loads(raw,object_pairs_hook=pairs)
    required={'profile','bun','java_home','jars','weft_index','weft_installation'}
    if type(config)is not dict or not required<=set(config)<=required|{'export_graph','destination_id','query_batch','exercise_mutations'}or config['profile']!=PROFILE:raise ValueError('Exact Truss example configuration required')
    export_graph=config.get('export_graph',False)
    if type(export_graph)is not bool:raise ValueError('Explicit boolean graph export selection required')
    exercise_mutations=config.get('exercise_mutations',False)
    if type(exercise_mutations)is not bool:raise ValueError('Explicit boolean mutation selection required')
    from .truss_feed_sessions import receipt_destination_id,require_truss_receipt_sdk
    destination_id=receipt_destination_id(config.get('destination_id'))
    if 'destination_id'in config and destination_id is None:raise ValueError('Explicit destination identity required')
    if destination_id is not None:require_truss_receipt_sdk()
    if type(config['jars'])is not list or len(config['jars'])!=2:raise ValueError('Two existing Delta JARs required')
    paths=[]
    for value in [config['bun'],config['java_home'],*config['jars'],config['weft_index'],config['weft_installation']]:
        if type(value)is not str or not 0<len(value)<=4096:raise ValueError('Explicit existing runtime paths required')
        path=Path(value)
        if not path.is_absolute()or path.resolve()!=path or not path.exists():raise ValueError('Canonical existing runtime paths required')
        paths.append(path)
    if (not paths[0].is_file()or not paths[1].is_dir()or not (paths[1]/'bin/java').is_file()or not all(p.is_file()for p in paths[2:5])or not paths[5].is_dir()
            or len(set(paths))!=len(paths)):raise ValueError('Distinct existing Bun/JDK/JAR/compiler selections required')
    if not output.is_absolute()or output.parent.resolve()!=output.parent or not output.parent.is_dir()or output.exists():raise ValueError('Fresh canonical output directory required')
    if any(output==p or output in p.parents or p in output.parents for p in [configuration,*paths]):raise ValueError('Separate output and runtime inputs required')
    batch_original=None;batch_path=None
    if 'query_batch'in config:
        value=config['query_batch']
        if type(value)is not str or not 0<len(value)<=4096:raise ValueError('Explicit query batch path required')
        batch_path=Path(value)
        if not batch_path.is_absolute()or batch_path.resolve()!=batch_path or not batch_path.is_file():raise ValueError('Canonical existing query batch required')
        if any(batch_path==p or batch_path in p.parents or p in batch_path.parents for p in [configuration,output,*paths]):raise ValueError('Separate query batch input required')
        batch_original=InvocationFile.read(batch_path,65536)
        from .truss_query_batch import parse_truss_query_batch
        parse_truss_query_batch(batch_original.raw)
    return TrussExampleConfiguration(PROFILE,paths[0],paths[1],tuple(paths[2:4]),paths[4],paths[5],export_graph,destination_id,batch_path,exercise_mutations),output,original,batch_original


def private_file(path,value):
    raw=value if type(value)is bytes else (json.dumps(value,indent=2)+'\n').encode()
    with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb')as stream:
        if stream.write(raw)!=len(raw):raise OSError('Incomplete private example output')
        stream.flush();os.fsync(stream.fileno())
    return path


def run_truss_example_command(configuration,output):
    config,root,original,batch_original=preflight(configuration,output)
    from .truss_runtime import require_truss_psycopg_runtime
    require_truss_psycopg_runtime()
    from .truss_local_setup import run_setup_truss_local_command
    from .truss_graph_mutation import run_mutate_truss_graph_command
    from .truss_note_cli import run_publish_truss_graph_command
    from .truss_query_cli import run_query_truss_graph_command
    from .truss_note_publication import read_page
    import psycopg
    from truss import LocalPostgres
    names=('JAVA_HOME','PYSPARK_PYTHON','PYSPARK_DRIVER_PYTHON');previous={n:os.environ.get(n)for n in names}
    root.mkdir(mode=0o700)
    def restore_environment():
        for name,value in previous.items():
            if value is None:os.environ.pop(name,None)
            else:os.environ[name]=value
    primary=None;answer=None
    try:
        os.environ.update(JAVA_HOME=str(config.java_home),PYSPARK_PYTHON=sys.executable,PYSPARK_DRIVER_PYTHON=sys.executable)
        model=private_file(root/'cohort-inverse.umf.json',(Path(__file__).parent/'resources/cohort-inverse.umf.json').read_bytes())
        origin=private_file(root/'origin.json',{'operator':'ashlar-local-truss-example'})
        setup=private_file(root/'setup.json',{'data_directory':str(root/'data'),'bun':str(config.bun),'document_path':str(model),'document_id':'cohort','document_revision':'r1','origin_path':str(origin),'request_key':'example-cohort-accept'})
        bootstrap=run_setup_truss_local_command(setup,root/'accepted.json')
        with owned_context(LocalPostgres(root/'data'))as runtime:
            parameters=psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri)
            administrator=psycopg.conninfo.make_conninfo(**parameters);parameters['user']=bootstrap['roles']['consumer']
            ordinary=psycopg.conninfo.make_conninfo(**parameters)
            mutation=private_file(root/'mutation-configuration.json',{'connection_uri':ordinary,'bun':str(config.bun),'document_path':str(model),'document_revision':'r1','origin_path':str(origin),'request_key':'example-cohort-create','a':{'label':'Alpha','value':'Value-A'},'b':{'label':'Beta','value':'Value-B'}})
            created=run_mutate_truss_graph_command(mutation,root/'mutation.json')
            publish=private_file(root/'publication-configuration.json',{'connection_uri':ordinary,'administrator_uri':administrator,'reader_role':bootstrap['roles']['reader'],'consumer_id':'example-delta','feed':'example-truss','jars':[str(path)for path in config.jars],'maximum_pages':2,**({'export_graph':True}if config.export_graph else {}),**({'destination_id':config.destination_id}if config.destination_id is not None else {})})
            published=run_publish_truss_graph_command(publish,root/'publication')
            intent=json.loads((root/'publication/publication-intent-2.json').read_bytes())
            query=private_file(root/'query-configuration.json',{'connection_uri':ordinary,'publication_directory':str(root/'publication'),'jars':[str(path)for path in config.jars],**{k:intent[k]for k in ('feed','source_page_path','source_page_sha256','request_digest')},**({'destination_id':config.destination_id}if config.destination_id is not None else {})})
            continued=None
            if config.exercise_mutations:
                from .truss_graph_lifecycle import run_update_delete_truss_target_command
                from .truss_continue_cli import run_continue_truss_graph_command
                from .truss_continuation_setup import run_configure_truss_continuation_command
                lifecycle_config=private_file(root/'lifecycle-configuration.json',{'connection_uri':ordinary,'bun':str(config.bun),'document_path':str(model),'document_revision':'r1','origin_path':str(origin),'object_id':created['objects'][1]['id'],'edge_id':int(created['edge']['identity']['id']),'replacement_label':'Beta-updated','update_request_key':'example-cohort-update','delete_request_key':'example-cohort-delete'})
                run_update_delete_truss_target_command(lifecycle_config,root/'lifecycle.json')
                continued=run_continue_truss_graph_command(query,root/'mutation-continuation.json',maximum_pages=2,wait_seconds=0)
                if len(continued['publications'])!=2:raise ValueError('Two complete mutation publications required')
                run_configure_truss_continuation_command(query,root/'mutation-continuation.json',root/'continued-query-configuration.json')
                query=root/'continued-query-configuration.json'
            def connection():return psycopg.connect(ordinary,autocommit=False,connect_timeout=5)
            before=read_page(connection,'example-delta').prior.content
            if config.query_batch is not None:
                batch_original.renew(65536)
                from .truss_query_batch import run_query_truss_graph_batch_command
                result=run_query_truss_graph_batch_command(query,root/'query-batch.json',index=str(config.weft_index),installation=str(config.weft_installation),batch=str(config.query_batch))
            else:
                result=run_query_truss_graph_command(query,root/'query.json',index=str(config.weft_index),installation=str(config.weft_installation),sql='SELECT b."B-label" AS label, RELATED_KEYS(b."B-A", 2) AS related FROM B b',revision='1',document_id='cohort',maximum_rows=2)
            after=read_page(connection,'example-delta').prior.content
            if continued is not None and result['publication_id']!=continued['publications'][-1]['manifest']['publication_id']:raise ValueError('Final mutation query publication differs')
            a=dict(created['objects'][0]['values']);b=dict(created['objects'][1]['values'])
            expected=[{'label':b['B-label'],'related':{'items':[[a['A-value'],a['A-label']]],'truncated':False}}]
            if config.exercise_mutations:expected=[]
            if (config.query_batch is None and result['rows']!=expected)or before!=after:raise ValueError('Original source query correspondence differs')
            if len(published['publications'])!=2:raise ValueError('Two complete original publications required')
            answer={'profile':PROFILE,'publication_id':result['publication_id'],'publications':2+(2 if continued is not None else 0),**({'rows':result['rows']}if config.query_batch is None else {'query_batch':str(root/'query-batch.json'),'queries':[{'name':item['name'],'rows':item['rows']}for item in result['queries']]}),'source_checkpoint_unchanged':True,'qualification':('Fresh local public Truss acceptance/mutation, canonical Delta publication and original Weft inverse query; no cloud or production authority.'if config.query_batch is None else 'Fresh local public Truss acceptance/mutation, canonical Delta publication and named original Weft batch queries; each query retains its executor qualification; no cloud or production authority.')}
    except BaseException as error:primary=error
    finish(primary,[restore_environment,lambda:original.renew(65536),*([lambda:batch_original.renew(65536)]if batch_original is not None else [])])
    if config.exercise_mutations:
        answer.update(lifecycle=str(root/'lifecycle.json'),mutation_continuation=str(root/'mutation-continuation.json'),continued_query_configuration=str(root/'continued-query-configuration.json'))
        answer['qualification']='Fresh local public Truss mutation lifecycle and canonical Delta continuation; selected original Weft query execution retains its executor qualification; no cloud or production authority.'
    if config.destination_id is not None:
        answer.update(destination_id=config.destination_id,ack_receipt_profile='ashlar-truss-destination-receipt/0.1')
    if config.export_graph:
        pair=published.get('graph_release')
        if type(pair)is not dict or not {'path','sha256','custody_path','custody_sha256','qualification'}<=set(pair):raise ValueError('Requested original graph export pair missing')
        if config.exercise_mutations:answer['graph_export_publication_id']=published['publications'][-1]['manifest']['publication_id']
        answer.update(release={'path':pair['path'],'sha256':pair['sha256']},custody={'path':pair['custody_path'],'sha256':pair['custody_sha256']},graph_export_qualification=pair['qualification'])
    if config.query_batch is not None or config.exercise_mutations:
        from .exact_report import report_body
        private_file(root/'result.json',report_body(answer,64*1048576))
    else:
        private_file(root/'result.json',answer)
    return answer
