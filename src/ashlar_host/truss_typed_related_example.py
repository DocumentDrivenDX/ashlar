"""Finite public Truss typed records/edges → Delta → forward/inverse key traversal."""
import os,sys
from dataclasses import dataclass
from pathlib import Path
from .evolution_cli import InvocationFile
from .private_graph_custody import decode_custody_bytes
from .lifecycle import finish,owned_context
from .truss_example import private_file
from .exact_report import report_body
PROFILE='ashlar-local-truss-typed-related-example/0.1'

@dataclass(frozen=True)
class TypedRelatedExampleConfiguration:
    profile:str
    bun:Path
    java_home:Path
    jars:tuple
    compiler:Path

def preflight(configuration,output):
    path=Path(configuration);root=Path(output)
    if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical configuration required')
    original=InvocationFile.read(path,65536);raw=decode_custody_bytes(original.raw)
    if type(raw)is not dict or set(raw)!={'profile','bun','java_home','jars','compiler'}or raw['profile']!=PROFILE:raise ValueError('Exact typed relationship example configuration required')
    if type(raw['jars'])is not list or len(raw['jars'])!=2:raise ValueError('Two existing Delta JARs required')
    paths=[]
    for value in (raw['bun'],raw['java_home'],*raw['jars'],raw['compiler']):
        if type(value)is not str or not 0<len(value)<=4096:raise ValueError('Explicit runtime paths required')
        selected=Path(value)
        if not selected.is_absolute()or selected.resolve()!=selected or not selected.exists():raise ValueError('Canonical existing runtime required')
        paths.append(selected)
    if not paths[0].is_file()or not paths[1].is_dir()or not (paths[1]/'bin/java').is_file()or not all(p.is_file()for p in paths[2:5])or len(set(paths))!=5:raise ValueError('Distinct Bun/JDK/JAR inputs required')
    if not root.is_absolute()or root.parent.resolve()!=root.parent or not root.parent.is_dir()or root.exists()or root.is_symlink():raise ValueError('Fresh canonical output required')
    if any(root==p or root in p.parents or p in root.parents for p in (path,*paths)):raise ValueError('Separate output required')
    return TypedRelatedExampleConfiguration(PROFILE,paths[0],paths[1],tuple(paths[2:4]),paths[4]),root,original

def run_truss_typed_related_example_command(configuration,output):
    config,root,original=preflight(configuration,output)
    from .truss_runtime import require_truss_psycopg_runtime
    require_truss_psycopg_runtime()
    from .truss_local_setup import run_setup_truss_local_command
    from .truss_record_mutation import run_create_truss_record_command
    from .truss_typed_edge_command import run_create_truss_edge_command
    from .truss_note_cli import run_publish_truss_graph_command
    from .truss_resume_setup import run_configure_truss_resume_command
    from .truss_record_read import run_read_truss_record_command
    from .truss_typed_related_query_cli import run_query_truss_typed_related_keys_command
    from .truss_note_publication import read_page
    import psycopg
    from truss import LocalPostgres
    names=('JAVA_HOME','PYSPARK_PYTHON','PYSPARK_DRIVER_PYTHON');previous={n:os.environ.get(n)for n in names}
    root.mkdir(mode=0o700)
    def restore():
        for name,value in previous.items():
            if value is None:os.environ.pop(name,None)
            else:os.environ[name]=value
    primary=None;answer=None
    try:
        os.environ.update(JAVA_HOME=str(config.java_home),PYSPARK_PYTHON=sys.executable,PYSPARK_DRIVER_PYTHON=sys.executable)
        model=private_file(root/'typed-related-record.umf.json',(Path(__file__).parent/'resources/typed-related-record.umf.json').read_bytes())
        origin=private_file(root/'origin.json',{'operator':'ashlar-local-truss-typed-related-example'})
        selection={'bun':str(config.bun),'document_path':str(model),'document_id':'typed-related-record','document_revision':'r1','origin_path':str(origin)}
        setup=private_file(root/'setup.json',{'data_directory':str(root/'data'),**selection,'request_key':'example-related-accept'})
        bootstrap=run_setup_truss_local_command(setup,root/'accepted.json')
        with owned_context(LocalPostgres(root/'data'))as runtime:
            parameters=psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri)
            administrator=psycopg.conninfo.make_conninfo(**parameters);parameters['user']=bootstrap['roles']['consumer'];ordinary=psycopg.conninfo.make_conninfo(**parameters)
            objects=[];samples=[('R1','9007199254740993','1.2300',True),('R2','2','2.0000',False)]
            for number,(name,quantity,amount,active)in enumerate(samples,1):
                creation=private_file(root/f'create-record-{number}.json',{'connection_uri':ordinary,**selection,'module_id':'m','record_id':'TypedRecord','request_key':'example-related-create-'+str(number),'values':{'record-id':{'kind':'string','text':name},'quantity':{'kind':'integer','text':quantity},'amount':{'kind':'decimal','text':amount},'active':{'kind':'boolean','value':active}}})
                objects.append(run_create_truss_record_command(creation,root/f'created-record-{number}.json')['object'])
            edges=[]
            for number,(source,target)in enumerate(((0,1),(1,1)),1):
                def endpoint(index):return {'document_id':'typed-related-record','module_id':'m','record_id':'TypedRecord','entity_id':objects[index]['id']}
                creation=private_file(root/f'create-edge-{number}.json',{'connection_uri':ordinary,**selection,'module_id':'m','relationship_id':'related','request_key':'example-related-edge-'+str(number),'source':endpoint(source),'target':endpoint(target),'values':{},'order_key':{'state':'null'}})
                edges.append(run_create_truss_edge_command(creation,root/f'created-edge-{number}.json')['edge'])
            publish=private_file(root/'publication-configuration.json',{'connection_uri':ordinary,'administrator_uri':administrator,'reader_role':bootstrap['roles']['reader'],'consumer_id':'example-related-delta','feed':'example-related','jars':[str(p)for p in config.jars],'maximum_pages':5,'maximum_page_bytes':2097152})
            published=run_publish_truss_graph_command(publish,root/'publication')
            if len(published['publications'])!=5:raise ValueError('Five complete catalog/object/edge publications required')
            reader=root/'reader-configuration.json'
            run_configure_truss_resume_command(publish,root/'publication/publication-intent-5.json',reader)
            def connection():return psycopg.connect(ordinary,autocommit=False,connect_timeout=5)
            before=read_page(connection,'example-related-delta',maximum_bytes=2097152).prior.content
            publication=published['publications'][-1]['manifest']['publication_id'];singletons=[]
            for number,(obj,(name,quantity,amount,active))in enumerate(zip(objects,samples),1):
                observed=run_read_truss_record_command(reader,root/f'record-{number}.json',type_id=obj['type_id'],entity_id=obj['id'])
                record=observed['record'];fields=record['fields']
                wanted={'record-id':('value',name),'quantity':('value',{'integer_token':quantity}),'amount':('value',{'decimal_token':amount}),'active':('value',active),'note':('absent',None),'tag':('absent',None)}
                if len(fields)!=6 or {f['field_id']:(f['state'],f['value'])for f in fields}!=wanted or (record['document_id'],record['module_id'],record['record_id'])!=('typed-related-record','m','TypedRecord')or observed['type_id']!=obj['type_id']or observed['entity_id']!=obj['id']or observed['publication_id']!=publication:raise ValueError('Exact typed singleton/publication differs')
                singletons.append(observed)
            wanted={'forward':[{'id':'R1','targets':{'items':[['R2']],'truncated':False}},{'id':'R2','targets':{'items':[['R2']],'truncated':False}}],'inverse':[{'id':'R1','targets':{'items':[],'truncated':False}},{'id':'R2','targets':{'items':[['R1'],['R2']],'truncated':False}}]}
            queries={}
            from collections import Counter
            from .delta_custody import encoded
            for direction,relationship in (('forward','related'),('inverse','incoming')):
                query=run_query_truss_typed_related_keys_command(reader,root/(direction+'-query.json'),compiler=config.compiler,sql='SELECT r."record-id" AS id,RELATED_KEYS(r.'+relationship +',2) AS targets FROM TypedRecord r',revision='1',document_id='typed-related-record',maximum_rows=2)
                if query['publication_id']!=publication or Counter(map(encoded,query['rows']))!=Counter(map(encoded,wanted[direction])):raise ValueError('Exact complete typed relationship occurrence bag differs')
                queries[direction]=query
            after=read_page(connection,'example-related-delta',maximum_bytes=2097152).prior.content
            if before!=after:raise ValueError('Read-only relationship query advanced source checkpoint')
            answer={'profile':PROFILE,'publication_id':publication,'publications':5,'objects':objects,'edges':edges,'singletons':singletons,'queries':queries,'source_checkpoint_unchanged':True,'qualification':'Fresh local public typed Truss record/edge creation, Delta publication, exact singletons and forward/inverse RELATED_KEYS on one current revision; caller-asserted ACK. Self-loop source acceptance defines the qualified subset. No cloud or production qualification.'}
    except BaseException as error:primary=error
    finish(primary,[restore,lambda:original.renew(65536)])
    private_file(root/'result.json',report_body(answer,1048576));return answer
