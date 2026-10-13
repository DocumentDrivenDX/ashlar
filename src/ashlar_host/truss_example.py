"""Portable, finite public Truss → Delta → Weft example; existing runtimes only."""
import json,os,sys
from dataclasses import dataclass
from pathlib import Path
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
    if type(config)is not dict or set(config)!={'profile','bun','java_home','jars','weft_index','weft_installation'}or config['profile']!=PROFILE:raise ValueError('Exact Truss example configuration required')
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
    return TrussExampleConfiguration(PROFILE,paths[0],paths[1],tuple(paths[2:4]),paths[4],paths[5]),output,original


def private_file(path,value):
    raw=value if type(value)is bytes else (json.dumps(value,indent=2)+'\n').encode()
    with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb')as stream:
        if stream.write(raw)!=len(raw):raise OSError('Incomplete private example output')
        stream.flush();os.fsync(stream.fileno())
    return path


def run_truss_example_command(configuration,output):
    config,root,original=preflight(configuration,output)
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
            publish=private_file(root/'publication-configuration.json',{'connection_uri':ordinary,'administrator_uri':administrator,'reader_role':bootstrap['roles']['reader'],'consumer_id':'example-delta','feed':'example-truss','jars':[str(path)for path in config.jars],'maximum_pages':2})
            published=run_publish_truss_graph_command(publish,root/'publication')
            intent=json.loads((root/'publication/publication-intent-2.json').read_bytes())
            query=private_file(root/'query-configuration.json',{'connection_uri':ordinary,'publication_directory':str(root/'publication'),'jars':[str(path)for path in config.jars],**{k:intent[k]for k in ('feed','source_page_path','source_page_sha256','request_digest')}})
            def connection():return psycopg.connect(ordinary,autocommit=False,connect_timeout=5)
            before=read_page(connection,'example-delta').prior.content
            result=run_query_truss_graph_command(query,root/'query.json',index=str(config.weft_index),installation=str(config.weft_installation),sql='SELECT b."B-label" AS label, RELATED_KEYS(b."B-A", 2) AS related FROM B b',revision='1',document_id='cohort',maximum_rows=2)
            after=read_page(connection,'example-delta').prior.content
            a=dict(created['objects'][0]['values']);b=dict(created['objects'][1]['values'])
            expected=[{'label':b['B-label'],'related':{'items':[[a['A-value'],a['A-label']]],'truncated':False}}]
            if result['rows']!=expected or before!=after:raise ValueError('Original source query correspondence differs')
            if len(published['publications'])!=2:raise ValueError('Two complete original publications required')
            answer={'profile':PROFILE,'publication_id':result['publication_id'],'publications':2,'rows':result['rows'],'source_checkpoint_unchanged':True,'qualification':'Fresh local public Truss acceptance/mutation, canonical Delta publication and original Weft inverse query; no cloud or production authority.'}
    except BaseException as error:primary=error
    finish(primary,[restore_environment,lambda:original.renew(65536)])
    private_file(root/'result.json',answer)
    return answer
