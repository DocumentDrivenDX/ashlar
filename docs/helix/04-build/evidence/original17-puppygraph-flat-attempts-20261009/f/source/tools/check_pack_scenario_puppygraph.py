"""Original17 native analytic translation; activation is external and mandatory.

Property equality joins preserve relational multiplicity; independent edges are
checked as carriers, never substituted for authored joins. No host result repair.
"""
import json,re
from pack_scenario_native_plan import plans
from run_pack_scenario_graphframes import ARITY,WITNESSES,validate_case


MAP_STANDARD='tinkerpop-3.7.3-valuemap/0.1'
# Observed both roles in exact image pinned by observe_six/observe_native.
# String/null shape proof: b240759598d146742a601827a7a141274860eb74849bbecc695ce21501d8b41e.
# Native LONG qualification is separate from this first-chunk observation.
MAP_PUPPY='ashlar-puppygraph-1.13-flat-valuemap/0.1'


def query_plan(profile,name,language):
    scans,joins,predicates,outputs,group=plans(profile['pack'])[name]
    records=dict(s.split(':')for s in scans);fields={(f['record_identity'][2],f['original_field']['name']):f for f in profile['fields']};bindings={};label='Scenario'+profile['pack'].title()+'Node'
    def prop(token):
        alias,field=token.split('.');f=fields[(records[alias],field)];n=f['signed64_column']if field in ('earliest','latest','count')else f['lexical_column']
        if n is None:raise ValueError('Named finite scalar capacity required')
        return alias+'.'+n
    def parameter(value):
        key='ashlarScenarioValue'+str(len(bindings));bindings[key]=value;return '$'+key
    def condition(text):
        if text.startswith('NOT_NULL:'):return prop(text[9:])+' IS NOT NULL'
        if text.startswith('NULL:'):return prop(text[5:])+' IS NULL'
        if text.startswith('IN:'):
            _,field,*values=text.split(':');return prop(field)+' IN '+parameter(values)
        m=re.fullmatch(r'(.+?)(<=|<>|=|<)(.+)',text)
        if not m:raise ValueError('Closed native predicate required')
        a,operator,b=m.groups();rhs=parameter(b[7:])if b.startswith('STRING:')else b[8:]if b.startswith('INTEGER:')else prop(b)
        return prop(a)+' '+operator+' '+rhs
    if language=='Gremlin':return gremlin_plan(profile,name,scans,joins,predicates,outputs,group,records,fields,label)
    if language!='Cypher':raise ValueError('Closed native protocol required')
    required=[s for s in scans if not any(j.startswith('OPTIONAL:'+s.split(':')[0]+'.')for j in joins)]
    text='MATCH '+','.join('('+s.split(':')[0]+':'+label+')'for s in required)
    where=[]
    for s in required:
        alias,record=s.split(':');identity=next(f['record_identity']for f in profile['fields']if f['record_identity'][2]==record)
        typ=json.dumps({'document':identity[0],'module':identity[1],'element':identity[2]},sort_keys=True,separators=(',',':'));where.append(alias+'.original_type='+parameter(typ))
    where +=[condition(j)for j in joins if not j.startswith('OPTIONAL:')]+[condition(p)for p in predicates]
    if where:text+=' WHERE '+' AND '.join(where)
    for s in scans:
        alias,record=s.split(':');optional=[j[9:]for j in joins if j.startswith('OPTIONAL:'+alias+'.')]
        if optional:
            identity=next(f['record_identity']for f in profile['fields']if f['record_identity'][2]==record);typ=json.dumps({'document':identity[0],'module':identity[1],'element':identity[2]},sort_keys=True,separators=(',',':'))
            text+=' OPTIONAL MATCH ('+alias+':'+label+') WHERE '+alias+'.original_type='+parameter(typ)+' AND '+' AND '.join(condition(j)for j in optional)
    def expression(token):return 'count(DISTINCT '+prop(token[15:])+')'if token.startswith('COUNT_DISTINCT:')else prop(token)
    columns=['result_'+str(i)for i in range(len(outputs))];selected=[expression(x)+' AS '+n for x,n in zip(outputs,columns)]
    if name=='evidence-links':
        witnesses=['i.original_key','e.original_key',prop('i.id'),'p.original_key','f.original_key','p.'+fields[('pottery_results','form')]['presence_column'],'f.'+fields[('fauna_results','taxon')]['presence_column']]
        selected +=[x+' AS '+n for x,n in zip(witnesses,WITNESSES)];columns+=WITNESSES
    if group and group[1]is not None:
        text+=' WITH '+','.join(selected)+' WHERE result_1>'+str(group[1])+' RETURN '+','.join(columns)
    else:text+=' RETURN '+('DISTINCT 'if name=='comparability'else'')+','.join(selected)
    if name=='connected-measurements':text+=' ORDER BY result_0'
    if name=='evidence-links':text+=' ORDER BY interpretation_order_token'
    return text,bindings,columns


def execute(profile,language,query):
    reports=[]
    for case in profile['cases']:
        name=case['original_scenario']['id'];script,bindings,names=query_plan(profile,name,language);raw=staged_query(query,{'pack':profile['pack'],'stage':'authored-case','case':name},script,bindings)
        if any(type(r)is not dict or set(r)!=set(names)for r in raw):raise ValueError('Closed native result fields required')
        ordered=[[r[n]for n in names]for r in raw];types=['string']*len(names)
        if name in ('media','connected-measurements'):types[1]='bigint'
        actual=validate_case(profile,name,[{'name':n,'type':t}for n,t in zip(names,types)],ordered)
        reports.append({'original_scenario':case['original_scenario'],'query':script,'bindings':bindings,'actual_ordered_cells':ordered,'validated':actual})
    return reports


def gremlin_plan(profile,name,scans,joins,predicates,outputs,group,records,fields,label):
    bindings={}
    def bound(value):
        key='ashlarScenarioValue'+str(len(bindings));bindings[key]=value;return key
    def column(token):
        alias,field=token.split('.');f=fields[(records[alias],field)];n=f['signed64_column']if field in ('earliest','latest','count')else f['lexical_column']
        if n is None:raise ValueError('Named finite scalar required')
        return alias,n
    def val(token):
        a,n=column(token);return "__.select('"+a+"').values('"+n+"')"
    def condition(text):
        if text.startswith('NOT_NULL:'):return val(text[9:])
        if text.startswith('NULL:'):return '__.not('+val(text[5:])+')'
        if text.startswith('IN:'):
            _,field,*values=text.split(':');return val(field)+'.is(within('+bound(values)+'))'
        a,op,b=re.fullmatch(r'(.+?)(<=|<>|=|<)(.+)',text).groups();predicate={'=':'eq','<>':'neq','<':'lt','<=':'lte'}[op]
        if b.startswith('STRING:'):return val(a)+'.is('+predicate+'('+bound(b[7:])+'))'
        if b.startswith('INTEGER:'):return val(a)+'.is('+predicate+'('+b[8:]+'))'
        key='ashlarComparedValue'+str(len(bindings));bindings[key+'Marker']='private-label-only'
        # Both original scan occurrences stay on the traverser path.
        return val(b)+".as('"+key+"').select('"+a.split('.')[0]+"').values('"+column(a)[1]+"').where("+predicate+"('"+key+"'))"
    def scan(s):
        a,r=s.split(':');identity=next(f['record_identity']for f in profile['fields']if f['record_identity'][2]==r);typ=json.dumps({'document':identity[0],'module':identity[1],'element':identity[2]},sort_keys=True,separators=(',',':'))
        return ".V().hasLabel('"+label+"').has('original_type',"+bound(typ)+").as('"+a+"')"
    optional={j[9:].split('.')[0]for j in joins if j.startswith('OPTIONAL:')};text='g';available=set()
    pending=[j for j in joins if not j.startswith('OPTIONAL:')]+list(predicates)
    for s in scans:
        a=s.split(':')[0]
        if a not in optional:
            text+=scan(s);available.add(a)
            ready=[p for p in pending if set(re.findall(r'([a-z]+)\.',p))<=available]
            for p in ready:text+='.where('+condition(p)+')';pending.remove(p)
    if pending:raise ValueError('Every original predicate must bind existing scan occurrences')
    for s in scans:
        a=s.split(':')[0]
        if a in optional:
            checks=''.join('.where('+condition(j[9:])+')'for j in joins if j.startswith('OPTIONAL:'+a+'.'))
            text+='.optional(__'+scan(s)+checks+')'
    names=['result_'+str(i)for i in range(len(outputs))];steps=[]
    for o in outputs:
        t=o[15:]if o.startswith('COUNT_DISTINCT:')else o;steps.append('__.coalesce('+val(t)+',__.constant(null))')
    if name=='evidence-links':
        names+=WITNESSES
        for a,n in [('i','original_key'),('e','original_key'),column('i.id'),('p','original_key'),('f','original_key'),('p',fields[('pottery_results','form')]['presence_column']),('f',fields[('fauna_results','taxon')]['presence_column'])]:steps.append("__.coalesce(__.select('"+a+"').values('"+n+"'),__.constant(null))")
    if name=='evidence-links':text+=".order().by("+val('i.id')+")"
    text+='.project('+','.join(repr(n)for n in names)+')'+''.join('.by('+x+')'for x in steps)
    if group:
        # Group values retain occurrence bags; distinct is the authored aggregate.
        text+=".group().by(__.select('result_0')).by(__.select('result_1').is(neq(null)).dedup().count()).unfold().project('result_0','result_1').by(__.select(keys)).by(__.select(values))"
        if group[1]is not None:text+=".where(__.select('result_1').is(gt("+str(group[1])+')))'
    if name=='connected-measurements':text+=".order().by(__.select('result_0'))"
    if name=='comparability':text+='.dedup()'
    return text,bindings,names


def raw_projection(profile,kind,language,selected=None):
    from prepare_pack_scenario_puppygraph import entries
    nodes,edges,_=entries(profile);rows=nodes if kind=='node'else edges;names=list(rows[0])if selected is None else selected;label='Scenario'+profile['pack'].title()+kind.title()
    if language=='Cypher':
        pattern='(n:'+label+')'if kind=='node'else'(s:Scenario'+profile['pack'].title()+'Node)-[n:'+label+']->(t:Scenario'+profile['pack'].title()+'Node)'
        query='MATCH '+pattern+' RETURN '+','.join('n.'+({'id':'carrier_id','graph_id':'carrier_key'}.get(n,n))+' AS '+n for n in names)+',id(n) AS native_id'
        if kind=='edge':query+=',id(s) AS native_src,id(t) AS native_dst'
    elif language=='Gremlin':
        native_names=list(dict.fromkeys({'id':'carrier_id','graph_id':'carrier_key'}.get(n,n)for n in names))
        query='g.'+('V'if kind=='node'else'E')+"().hasLabel('"+label+"').project('cells','native_id'"+(",'native_src','native_dst'"if kind=='edge'else'')+").by(__.valueMap("+','.join(repr(n)for n in native_names)+")).by(__.id())"
        if kind=='edge':query+='.by(__.outV().id()).by(__.inV().id())'
    else:raise ValueError('Closed protocol required')
    return query,rows,names,label


def carrier_chunks(profile,kind,language):
    from prepare_pack_scenario_puppygraph import entries
    nodes,edges,_=entries(profile);rows=nodes if kind=='node'else edges;names=list(rows[0])
    if len({r['original_key']for r in rows})!=len(rows)or len({r['graph_id']for r in rows})!=len(rows):raise ValueError('Injective original carrier correlation required')
    if language!='Gremlin':return [names]
    identity=['id','graph_id','original_key','original_type']+(['src','dst']if kind=='edge'else [])
    remaining=[n for n in names if n not in identity]
    # Independent native projected bags, each repeats original identity/incidence.
    # No host assembly, filtering, deduplication or query-result repair.
    return [identity+remaining[i:i+12]for i in range(0,len(remaining),12)]


def native_map_cells(row,kind,names,expected,native_map_profile=MAP_STANDARD):
    """TinkerPop3.7.3 valueMap codec: vertex lists, edge scalars.

    https://tinkerpop.apache.org/docs/3.7.3/reference/#valuemap-step
    PuppyGraph1.13 behavior remains actual native qualification, not inferred.
    Raw property maps stay retained; missing physical-null cells are allowed
    only by exact independently admitted source-cell correspondence.
    """
    if kind not in ('node','edge'):raise ValueError('Closed native carrier role required')
    if native_map_profile not in (MAP_STANDARD,MAP_PUPPY):raise ValueError('Explicit closed native property-map codec required')
    mapping={n:{'id':'carrier_id','graph_id':'carrier_key'}.get(n,n)for n in names}
    required={'cells','native_id'}|({'native_src','native_dst'}if kind=='edge'else set())
    if type(row)is not dict or set(row)!=required or type(row['cells'])is not dict or not set(row['cells'])<=set(mapping.values()):raise ValueError('Closed role-specific native property map required')
    values=row['cells']
    def scalar(key):
        if key not in values:return None
        value=values[key]
        if native_map_profile==MAP_STANDARD and kind=='node':
            if type(value)is not list or len(value)>1:raise ValueError('Node zero/one property list required')
            return value[0]if value else None
        if type(value)is list:raise ValueError('Explicit scalar native property-map profile required')
        return value
    identity=scalar('carrier_key')
    source=next((r for r in expected if r['graph_id']==identity),None)
    if source is None:raise ValueError('Exact independently original carrier key required')
    decoded={}
    for n,key in mapping.items():
        value=scalar(key);original=source[n]
        if value is None:
            if original is not None:raise ValueError('Missing non-null original property refuses')
        elif type(original)is int:
            allowed=type(value)is int
            if not allowed:
                try:
                    from gremlin_python.statics import long as graphson_long
                    allowed=type(value)is graphson_long
                except ImportError:allowed=False
            if not allowed or not -(2**63)<=value<2**63:raise ValueError('Exact native signed64 scalar or trusted GraphSON long required')
            value=int(value)
        elif type(original)is not str or type(value)is not str:raise ValueError('Exact native String scalar type required')
        decoded[n]=value
    return decoded


def carrier_chunk_check(profile,kind,language,query,selected,native_map_profile=MAP_STANDARD):
    from run_graph_release_graphframes import row_bag
    from check_puppygraph_releases import native_text
    script,expected,names,label=raw_projection(profile,kind,language,selected);raw=staged_query(query,{'pack':profile['pack'],'stage':'carrier','kind':kind,'columns':names},script,{})
    allowed=set(names)|{'native_id'}|({'native_src','native_dst'}if kind=='edge'else set());actual=[]
    for row in raw:
        if language=='Gremlin':cells=native_map_cells(row,kind,names,expected,native_map_profile)
        else:
            if type(row)is not dict or set(row)!=allowed:raise ValueError('Complete closed original native carrier required')
            cells={n:row[n]for n in names}
        if native_text(row['native_id'])!=label+'['+cells['graph_id']+']':raise ValueError('Exact original native identity required')
        if kind=='edge':
            node='Scenario'+profile['pack'].title()+'Node'
            if native_text(row['native_src'])!=node+'['+cells['src']+']'or native_text(row['native_dst'])!=node+'['+cells['dst']+']':raise ValueError('Exact original native incidence required')
        actual.append(cells)
    if row_bag(actual)!=row_bag([{n:r[n]for n in names}for r in expected]):raise ValueError('Complete original native scalar/presence/raw bag differs')
    return {'original_native_rows':raw,'decoded_projected_rows':actual}if language=='Gremlin'else raw


def carrier_check(profile,kind,language,query,native_map_profile=MAP_STANDARD):
    chunks=carrier_chunks(profile,kind,language)
    if language=='Cypher':return carrier_chunk_check(profile,kind,language,query,chunks[0],native_map_profile)
    return {'chunks':[dict(columns=names,**carrier_chunk_check(profile,kind,language,query,names,native_map_profile))for names in chunks]}


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
