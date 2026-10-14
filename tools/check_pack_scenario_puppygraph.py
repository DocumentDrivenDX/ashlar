"""Legacy original17 native runner; public pure helpers own translation/codecs."""
import json,re
from ashlar_host.pack_scenario_queries import (plans,query_plan,execute,gremlin_plan,native_map_cells,MAP_STANDARD,MAP_PUPPY)
from ashlar_host.graphframes_pack_scenarios import ARITY,WITNESSES,validate_case
from ashlar_host import pack_scenario_queries as _queries
def _bindings(profile):
    import base64
    return json.loads(base64.b64decode(profile['source_admission']['original_bindings_base64']))
def raw_projection(profile,kind,language,selected=None):return _queries.raw_projection(profile,kind,language,selected,accepted_bindings=_bindings(profile))
def carrier_chunks(profile,kind,language):return _queries.carrier_chunks(profile,kind,language,accepted_bindings=_bindings(profile))
def carrier_chunk_check(profile,kind,language,query,selected,native_map_profile=MAP_STANDARD):return _queries.carrier_chunk_check(profile,kind,language,query,selected,native_map_profile,accepted_bindings=_bindings(profile))
def carrier_check(profile,kind,language,query,native_map_profile=MAP_STANDARD):return _queries.carrier_check(profile,kind,language,query,native_map_profile,accepted_bindings=_bindings(profile))
def held(prepared,language,query,observe,source_observe,native_map_profile=MAP_STANDARD):
    """Mandatory trusted native/source observers, opening and closing whole vectors."""
    original=source_observe();opening=observe();reports=[]
    for report in prepared['reports']:
        profile=report['profile'];carriers={k:carrier_check(profile,k,language,query,native_map_profile)for k in ('node','edge')}
        cases=execute(profile,language,query)
        closing_carriers={k:carrier_check(profile,k,language,query,native_map_profile)for k in ('node','edge')}
        reports.append({'pack':profile['pack'],'carriers':carriers,'cases':cases,'closing_carriers':closing_carriers})
    closing=observe()
    if closing!=opening or source_observe()!=original:raise ValueError('Closing whole native/source custody differs')
    return {'format':'ashlar-original17-puppygraph-native/0.1','language':language,'reports':reports,'opening':opening,'closing':closing,'original_files':original,'original_authored_cases':17,'native_map_profile':native_map_profile if language=='Gremlin'else None}


def observe_six(language,endpoint,model,databases,user,password):
    import subprocess
    from check_pack_puppygraph import observe_native
    expected={'/tmp/releases.duckdb','/tmp/commerce.duckdb','/tmp/augmentations.duckdb','/tmp/commerce-exact.duckdb','/tmp/packs.duckdb','/tmp/scenarios.duckdb'}
    if set(databases)!=expected:raise ValueError('Complete immutable six database inventory required')
    base=observe_native(language,endpoint,model,{k:v for k,v in databases.items()if k!='/tmp/scenarios.duckdb'},user,password)
    out=subprocess.run(['docker','exec','ashlar-release-puppy113','sha256sum',*sorted(databases)],check=True,capture_output=True,text=True).stdout
    actual={line.split()[1]:line.split()[0]for line in out.splitlines()}
    if actual!=databases:raise ValueError('Complete six database bytes differ')
    base['databases']=actual;return base


def staged_query(query,stage,script,bindings):
    if hasattr(query,'set_stage'):query.set_stage(stage)
    return query(script,bindings)


def journal_query(query,observe,path):
    if path is None:return query # injected unit/embedding port; production CLI requires journal
    import os,json
    from pathlib import Path
    path=Path(path);fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.close(fd);stage={}
    def append(value):
        with path.open('a')as stream:stream.write(json.dumps(value,sort_keys=True,ensure_ascii=False)+'\n');stream.flush();os.fsync(stream.fileno())
    def execute(script,bindings):
        frozen=json.loads(json.dumps({'stage':stage,'script':script,'bindings':bindings}));append(dict(frozen,outcome='submitted-before-native-query'))
        try:
            opening=observe();raw=query(frozen['script'],frozen['bindings']);closing=observe()
            if closing!=opening:raise ValueError('Whole native custody changed during query')
        except Exception as error:append(dict(frozen,outcome='failed',error_type=type(error).__name__,error=str(error)));raise
        append(dict(frozen,outcome='native-query-completed',native_rows=len(raw),opening=opening,closing=closing));return raw
    def set_stage(value):stage.clear();stage.update(json.loads(json.dumps(value)))
    execute.set_stage=set_stage;return execute


def check(prepared,language,endpoint,model,databases,user,password,source_observe,attempt_log=None,native_map_profile=MAP_STANDARD):
    observe=lambda:observe_six(language,endpoint,model,databases,user,password)
    observe();source_observe() # no native client before complete custody
    if language=='Cypher':
        from neo4j import GraphDatabase
        with GraphDatabase.driver(endpoint,auth=(user,password),connection_timeout=5)as driver:
            with driver.session()as session:result=held(prepared,language,journal_query(lambda s,b:session.run(s,**b).data(),observe,attempt_log),observe,source_observe,native_map_profile)
    elif language=='Gremlin':
        from gremlin_python.driver import client,serializer
        remote=client.Client(endpoint,'g',username=user,password=password,message_serializer=serializer.GraphSONSerializersV3d0())
        try:result=held(prepared,language,journal_query(lambda s,b:remote.submit(s,bindings=b).all().result(timeout=20),observe,attempt_log),observe,source_observe,native_map_profile)
        finally:remote.close()
    else:raise ValueError('Closed protocol required')
    if source_observe()!=result['original_files']or observe()!=result['closing']:raise ValueError('Post-client cleanup custody differs')
    return result


def admit(directory,trusted,candidates,umf):
    import hashlib
    from pathlib import Path
    from prepare_pack_scenario_puppygraph import entries
    directory=Path(directory)
    if set(trusted)!={'receipt.json','model.json','scenarios.duckdb'}:raise ValueError('Closed independently trusted preparation required')
    for n,h in trusted.items():
        if hashlib.sha256((directory/n).read_bytes()).hexdigest()!=h:raise ValueError('Original prepared bytes differ')
    prepared=json.loads((directory/'receipt.json').read_bytes())
    if prepared['format']!='ashlar-original17-puppygraph-preparation/0.1'or prepared['engine_executed']is not False or [r['profile']['pack']for r in prepared['reports']]!=['archaeology','ecology']:raise ValueError('Exact original complete source preparation required')
    if prepared['database_sha256']!=trusted['scenarios.duckdb']:raise ValueError('Native database custody differs')
    from prepare_pack_graph_scenarios import prepare
    from run_graph_release_graphframes import row_bag
    import duckdb
    if [c.get('pack')for c in candidates]!=['archaeology','ecology']:raise ValueError('Exact original two-source candidates required')
    with duckdb.connect(str(directory/'scenarios.duckdb'),read_only=True)as database:
        for candidate,r in zip(candidates,prepared['reports']):
            actual_profile=prepare(candidate,umf)
            if actual_profile!=r['profile']:raise ValueError('Complete original publication/profile correspondence differs')
            nodes,edges,longs=entries(actual_profile)
            if nodes!=r['nodes']or edges!=r['edges']:raise ValueError('Original profile-to-cell correspondence differs')
            for kind,expected in (('nodes',nodes),('edges',edges)):
                table=candidate['pack']+'_'+kind;names=list(expected[0]);schema=database.execute('DESCRIBE carrier.'+table).fetchall()
                if [(x[0],x[1])for x in schema]!=[(n,'BIGINT'if n in longs else'VARCHAR')for n in names]:raise ValueError('Complete original native schema differs')
                cur=database.execute('SELECT * FROM carrier.'+table);rows=[dict(zip(names,x))for x in cur.fetchall()]
                if row_bag(rows)!=row_bag(expected):raise ValueError('Complete original native cells differ')
    return prepared


def source_watcher(prepared,directory,model_path,databases_path,trusted_path,candidates):
    import hashlib
    from pathlib import Path
    paths={str(Path(directory)/n)for n in ('receipt.json','model.json','scenarios.duckdb')}|{str(model_path),str(databases_path),str(trusted_path)}
    for candidate in candidates:
        paths|={str(Path(candidate['publication'])/n)for n in ('original-ontology.json','original-graph.json','development-bindings.json','public-dataset.json','source.jsonl','report.json')}
        paths|={candidate['release'],candidate['custody']}
    return lambda:{p:hashlib.sha256(Path(p).read_bytes()).hexdigest()for p in sorted(paths)}

def admit_guarded(directory,trusted,candidates,umf,watcher):
    baseline=watcher();prepared=admit(directory,trusted,candidates,umf)
    if watcher()!=baseline:raise ValueError('Original source changed during admission')
    return prepared,baseline

if __name__=='__main__':
    import argparse,hashlib,os
    from pathlib import Path
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('prepared','trusted','model','databases','output','candidates','umf','attempt-log'):p.add_argument('--'+n,type=Path,required=True)
    for n in ('model-sha256','language','endpoint'):p.add_argument('--'+n,required=True)
    p.add_argument('--native-map-profile',choices=(MAP_PUPPY,),required=True)
    a=p.parse_args()
    if a.output.exists()or a.attempt_log.exists():raise ValueError('Fresh original complete report and attempt journal required')
    raw=a.model.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=a.model_sha256:raise ValueError('Trusted complete model bytes differ')
    model=json.loads(raw);addition=json.loads((a.prepared/'model.json').read_bytes())
    for role in ('catalog','node','edge'):
        for entry in addition[role]:
            if model[role].count(entry)!=1:raise ValueError('Exact prepared mapping missing or repeated')
    trusted=json.loads(a.trusted.read_bytes());prepared=json.loads((a.prepared/'receipt.json').read_bytes());databases=json.loads(a.databases.read_bytes())
    if databases.get('/tmp/scenarios.duckdb')!=trusted['scenarios.duckdb']:raise ValueError('Sealed native scenario database differs')
    candidates=json.loads(a.candidates.read_bytes())
    if [c['pack']for c in candidates]!=['archaeology','ecology']:raise ValueError('Exact original two-source candidates required')
    for c,r in zip(candidates,prepared['reports']):
        if c['release_sha256']!=r['profile']['original_release_sha256']or c['custody_sha256']!=r['profile']['original_custody_sha256']:raise ValueError('Exact original source pair differs')
        for n in ('release','custody'):
            if hashlib.sha256(Path(c[n]).read_bytes()).hexdigest()!=c[n+'_sha256']:raise ValueError('Original source bytes differ')
    watcher=source_watcher(prepared,a.prepared,a.model,a.databases,a.trusted,candidates);prepared,baseline=admit_guarded(a.prepared,trusted,candidates,a.umf,watcher)
    result=check(prepared,a.language,a.endpoint,model,databases,os.environ['ASHLAR_PUPPY_USER'],os.environ['ASHLAR_PUPPY_PASSWORD'],watcher,a.attempt_log,a.native_map_profile)
    if watcher()!=baseline:raise ValueError('Original pre-client source vector differs')
    a.output.write_text(json.dumps(result,sort_keys=True,ensure_ascii=False,indent=2)+'\n')
