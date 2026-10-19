# SPDX-License-Identifier: MIT OR Apache-2.0
"""Finite local public Truss typed record → Delta → named singleton example."""
import os,sys
from dataclasses import dataclass
from pathlib import Path
from .evolution_cli import InvocationFile
from .private_graph_custody import decode_custody_bytes
from .lifecycle import finish,owned_context
from .truss_example import private_file
from .exact_report import report_body
PROFILE='ashlar-local-truss-record-example/0.1'

@dataclass(frozen=True)
class RecordExampleConfiguration:
    profile:str
    bun:Path
    java_home:Path
    jars:tuple

def preflight(configuration,output):
    path=Path(configuration);root=Path(output)
    if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical configuration required')
    original=InvocationFile.read(path,65536);raw=decode_custody_bytes(original.raw)
    if type(raw)is not dict or set(raw)!={'profile','bun','java_home','jars'}or raw['profile']!=PROFILE:raise ValueError('Exact typed record example configuration required')
    if type(raw['jars'])is not list or len(raw['jars'])!=2:raise ValueError('Two existing Delta JARs required')
    paths=[]
    for value in (raw['bun'],raw['java_home'],*raw['jars']):
        if type(value)is not str or not 0<len(value)<=4096:raise ValueError('Explicit runtime paths required')
        selected=Path(value)
        if not selected.is_absolute()or selected.resolve()!=selected or not selected.exists():raise ValueError('Canonical existing runtime required')
        paths.append(selected)
    if not paths[0].is_file()or not paths[1].is_dir()or not (paths[1]/'bin/java').is_file()or not all(p.is_file()for p in paths[2:])or len(set(paths))!=4:raise ValueError('Distinct Bun/JDK/JAR inputs required')
    if not root.is_absolute()or root.parent.resolve()!=root.parent or not root.parent.is_dir()or root.exists()or root.is_symlink():raise ValueError('Fresh canonical output required')
    if any(root==p or root in p.parents or p in root.parents for p in (path,*paths)):raise ValueError('Separate output required')
    return RecordExampleConfiguration(PROFILE,paths[0],paths[1],tuple(paths[2:])),root,original

def run_truss_record_example_command(configuration,output):
    config,root,original=preflight(configuration,output)
    from .truss_runtime import require_truss_psycopg_runtime
    require_truss_psycopg_runtime()
    from .truss_local_setup import run_setup_truss_local_command
    from .truss_record_mutation import run_create_truss_record_command
    from .truss_note_cli import run_publish_truss_graph_command
    from .truss_resume_setup import run_configure_truss_resume_command
    from .truss_record_read import run_read_truss_record_command
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
        model=private_file(root/'typed-record.umf.json',(Path(__file__).parent/'resources/typed-record.umf.json').read_bytes())
        origin=private_file(root/'origin.json',{'operator':'ashlar-local-truss-record-example'})
        selection={'bun':str(config.bun),'document_path':str(model),'document_id':'typed-record','document_revision':'r1','origin_path':str(origin)}
        setup=private_file(root/'setup.json',{'data_directory':str(root/'data'),**selection,'request_key':'example-typed-accept'})
        bootstrap=run_setup_truss_local_command(setup,root/'accepted.json')
        with owned_context(LocalPostgres(root/'data'))as runtime:
            parameters=psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri)
            administrator=psycopg.conninfo.make_conninfo(**parameters);parameters['user']=bootstrap['roles']['consumer'];ordinary=psycopg.conninfo.make_conninfo(**parameters)
            creation=private_file(root/'creation-configuration.json',{'connection_uri':ordinary,**selection,'module_id':'m','record_id':'TypedRecord','request_key':'example-typed-create','values':{'record-id':{'kind':'string','text':'R1'},'quantity':{'kind':'integer','text':'9007199254740993'},'amount':{'kind':'decimal','text':'1.2300'},'active':{'kind':'boolean','value':True}}})
            created=run_create_truss_record_command(creation,root/'created.json')
            publish=private_file(root/'publication-configuration.json',{'connection_uri':ordinary,'administrator_uri':administrator,'reader_role':bootstrap['roles']['reader'],'consumer_id':'example-typed-delta','feed':'example-typed','jars':[str(p)for p in config.jars],'maximum_pages':2})
            published=run_publish_truss_graph_command(publish,root/'publication')
            if len(published['publications'])!=2:raise ValueError('Two complete typed publications required')
            reader=root/'reader-configuration.json'
            run_configure_truss_resume_command(publish,root/'publication/publication-intent-2.json',reader)
            def connection():return psycopg.connect(ordinary,autocommit=False,connect_timeout=5)
            before=read_page(connection,'example-typed-delta').prior.content
            obj=created['object'];observed=run_read_truss_record_command(reader,root/'record.json',type_id=obj['type_id'],entity_id=obj['id'])
            after=read_page(connection,'example-typed-delta').prior.content
            record=observed['record'];fields=record['fields']
            expected={'record-id':('value','R1'),'quantity':('value',{'integer_token':'9007199254740993'}),'amount':('value',{'decimal_token':'1.2300'}),'active':('value',True),'note':('absent',None),'tag':('absent',None)}
            actual={f['field_id']:(f['state'],f['value'])for f in fields}
            if len(fields)!=6 or actual!=expected or (record['document_id'],record['module_id'],record['record_id'])!=('typed-record','m','TypedRecord'):raise ValueError('Complete original typed correspondence differs')
            if observed['type_id']!=obj['type_id']or observed['entity_id']!=obj['id']or observed['publication_id']!=published['publications'][-1]['manifest']['publication_id']or before!=after:raise ValueError('Original typed publication/identity/checkpoint differs')
            answer={'profile':PROFILE,'publication_id':observed['publication_id'],'type_id':obj['type_id'],'entity_id':obj['id'],'publications':2,'fields':fields,'source_checkpoint_unchanged':True,'qualification':'Fresh local public Truss typed record creation, canonical Delta publication and exact named singleton; caller-asserted ACK. No Weft query, cloud or production authority qualification.'}
    except BaseException as error:primary=error
    finish(primary,[restore,lambda:original.renew(65536)])
    private_file(root/'result.json',report_body(answer,1048576));return answer
