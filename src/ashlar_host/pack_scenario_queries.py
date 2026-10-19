# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original17 Puppy query/codec helpers; source and engine owners remain external."""
import base64,json,re
from collections import Counter
from ashlar.graph_release import release_columns as columns
from .graphframes_pack_scenarios import ARITY,WITNESSES,validate_case

def row_bag(rows):return Counter(json.dumps(dict(row),ensure_ascii=False,sort_keys=True,separators=(',',':'))for row in rows)
def native_text(value):
    if type(value)is dict and set(value)=={'@type','@value'}and value['@type']=='g:String':value=value['@value']
    if type(value)is not str:raise ValueError('Exact native string identity required')
    return value
def native_count(value):
    allowed=type(value)is int
    if not allowed:
        try:
            from gremlin_python.statics import long as graphson_long
            allowed=type(value)is graphson_long
        except ImportError:allowed=False
    if not allowed or not 0<=value<2**63:raise ValueError('Exact nonnegative signed64 count or trusted GraphSON long required')
    return int(value)
def staged_query(query,stage,script,bindings):
    if hasattr(query,'set_stage'):query.set_stage(stage)
    return query(script,bindings)

def plans(pack):
    # scans, equality joins, predicates, logical outputs, grouped count distinct.
    if pack=='archaeology':
        return {
        'cycle':(['a:stratigraphic_assertions','b:stratigraphic_assertions'],['a.source_context_id=b.target_context_id','a.target_context_id=b.source_context_id'],['a.id<b.id'],['a.id','b.id'],None),
        'dating':(['a:interpretations','b:interpretations'],['a.context_id=b.context_id'],['a.id<b.id','a.earliest<=b.latest','b.earliest<=a.latest'],['a.author','b.author'],None),
        'media':(['s:asset_subjects'],[],[],['s.asset_id','COUNT_DISTINCT:s.context_id'],('s.asset_id',1)),
        'missing-media':(['a:assets'],[],["a.availability=STRING:external"],['a.id'],None),
        'specialists':(['f:fauna_results','a:analyses','s:samples','l:find_lots','o:objects','p:pottery_results'],['a.id=f.analysis_id','s.id=a.sample_id','l.context_id=s.context_id','o.lot_id=l.id','p.object_id=o.id'],[],['f.nisp','f.mni','p.sherd_count','p.estimated_vessels'],None),
        'lineage':(['o:objects','l:find_lots','c:contexts'],['l.id=o.lot_id','c.id=l.context_id'],[],['o.id','c.native_locus'],None),
        'evidence-links':(['i:interpretations','e:interpretation_evidence','p:pottery_results','f:fauna_results'],['e.interpretation_id=i.id','OPTIONAL:p.id=e.pottery_result_id','OPTIONAL:f.id=e.fauna_result_id'],[],['i.author','p.form','f.taxon'],None),
        'sample':(['s:samples','a:analyses','r:soil_results'],['a.sample_id=s.parent_id','r.analysis_id=a.id'],['NOT_NULL:s.parent_id'],['s.parent_id','r.preparation'],None)}
    if pack=='ecology':
        common=['o:occurrences','e:effort'];join=['e.id=o.effort_id']
        return {
        'effort-event':(common,join,['e.event_id<>o.event_id'],['o.id'],None),
        'effort':(common,join,['NULL:e.amount'],['o.id'],None),
        'zero':(common,join,['o.count=INTEGER:0','o.detection=STRING:not-detected','NOT_NULL:e.amount'],['o.id'],None),
        'connected-measurements':(['o:observations','p:observed_properties','s:samples','e:sampling_events','site:monitoring_sites','r:reaches','m:methods'],['p.id=o.property_id','s.id=o.sample_id','e.id=s.event_id','site.id=e.site_id','r.id=site.reach_id','m.id=e.method_id'],['IN:p.name:temperature:dissolved-oxygen','m.fraction=STRING:in-situ'],['p.name','COUNT_DISTINCT:r.id'],('p.name',None)),
        'censor':(['o:observations'],[],['o.result_kind=STRING:censored','NULL:o.value'],['o.id','o.threshold','o.qualifier'],None),
        'match':(['s:site_matches'],[],['s.resolution=STRING:unresolved'],['s.id'],None),
        'network':(['n:network_links'],[],[],['n.upstream_id','n.downstream_id'],None),
        'comparability':(['o:occurrences','e:sampling_events','m:methods','i:identifications','t:taxa'],['e.id=o.event_id','m.id=e.method_id','i.id=o.identification_id','t.id=i.taxon_id'],[],['m.matrix','m.fraction','t.rank'],None),
        'fishing':(['f:fishing_events'],[],[],['f.effort_unit'],None)}
    raise ValueError('Closed original scenario pack required')


def entries(profile,accepted_bindings):
    if type(accepted_bindings)is not dict or type(accepted_bindings.get('entities'))is not list:raise ValueError('Explicit original accepted entity bindings required')
    names=list(profile['nodes'][0]);longs={f['signed64_column']for f in profile['fields']if f['signed64_column']}
    nodes=[]
    for row in profile['nodes']:
        if set(row)!=set(names):raise ValueError('Closed complete source-profile row required')
        for n,v in row.items():
            if n in longs:
                if v is not None and(type(v)is not int or not -(2**63)<=v<2**63):raise ValueError('Exact named LONG capacity required')
            elif v is not None and type(v)is not str:raise ValueError('Exact String/null source carrier required')
        nodes.append(dict(row,carrier_id=row['id'],carrier_key=row['graph_id']))
    source=json.loads(base64.b64decode(profile['source_admission']['original_graph_base64']));binding=accepted_bindings
    assignments={(e['type_id'],e['id']):e['originalKey']for e in binding['entities']if e['kind']=='edge'}
    lookup={e['key']:e for e in source['edges']};edges=[]
    for row in profile['edges']:
        if set(row)!=set(columns('edge'))or any(v is not None and type(v)is not str for v in row.values()):raise ValueError('Exact raw independent edge carrier required')
        key=assignments.get((row['rel_type_id'],row['id']))
        if key not in lookup:raise ValueError('Exact original independent edge binding required')
        edge=lookup[key];typ=json.dumps(edge['relationship'],sort_keys=True,separators=(',',':'))
        edges.append(dict(row,carrier_id=row['id'],carrier_key=row['graph_id'],original_key=key,original_type=typ))
    return nodes,edges,longs


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
        ordered=[[native_count(r[n])if language=='Gremlin'and name in ('media','connected-measurements')and i==1 else r[n]for i,n in enumerate(names)]for r in raw];types=['string']*len(names)
        if name in ('media','connected-measurements'):types[1]='bigint'
        actual=validate_case(profile,name,[{'name':n,'type':t}for n,t in zip(names,types)],ordered)
        reports.append({'original_scenario':case['original_scenario'],'query':script,'bindings':bindings,'original_native_rows':raw,'actual_ordered_cells':ordered,'validated':actual})
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
    # This one finite original case uses scan-local scalar filters, before the
    # cross-scan equality join. Source presence stays explicit; no null coercion.
    local_zero={}
    if profile['pack']=='ecology' and name=='zero':
        if list(predicates)!=['o.count=INTEGER:0','o.detection=STRING:not-detected','NOT_NULL:e.amount']:
            raise ValueError('Exact original zero predicates required')
        for token,family in [('o.count','integer'),('o.detection','string'),('e.amount','decimal')]:
            alias,field=token.split('.');metadata=fields[(records[alias],field)]
            if metadata['original_field']['scalarType']!=family or metadata['original_field']['cardinality']!='one':
                raise ValueError('Admitted original scalar filter meaning required')
        local_zero['o']=".has('"+column('o.count')[1]+"',eq(0)).has('"+column('o.detection')[1]+"',eq("+bound('not-detected')+"))"
        amount=fields[(records['e'],'amount')]
        local_zero['e']=".has('"+amount['presence_column']+"',eq("+bound('present')+"))"
    optional={j[9:].split('.')[0]for j in joins if j.startswith('OPTIONAL:')};text='g';available=set()
    pending=[j for j in joins if not j.startswith('OPTIONAL:')]+([] if local_zero else list(predicates))
    for s in scans:
        a=s.split(':')[0]
        if a not in optional:
            fragment=scan(s)
            if a in local_zero:
                marker=".as('"+a+"')"
                fragment=fragment[:-len(marker)]+local_zero[a]+marker
            text+=fragment;available.add(a)
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


def raw_projection(profile,kind,language,selected=None,*,accepted_bindings):
    nodes,edges,_=entries(profile,accepted_bindings);rows=nodes if kind=='node'else edges;names=list(rows[0])if selected is None else selected;label='Scenario'+profile['pack'].title()+kind.title()
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


def carrier_chunks(profile,kind,language,*,accepted_bindings):
    nodes,edges,_=entries(profile,accepted_bindings);rows=nodes if kind=='node'else edges;names=list(rows[0])
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


def carrier_chunk_check(profile,kind,language,query,selected,native_map_profile=MAP_STANDARD,*,accepted_bindings):
    script,expected,names,label=raw_projection(profile,kind,language,selected,accepted_bindings=accepted_bindings);raw=staged_query(query,{'pack':profile['pack'],'stage':'carrier','kind':kind,'columns':names},script,{})
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


def carrier_check(profile,kind,language,query,native_map_profile=MAP_STANDARD,*,accepted_bindings):
    chunks=carrier_chunks(profile,kind,language,accepted_bindings=accepted_bindings)
    if language=='Cypher':return carrier_chunk_check(profile,kind,language,query,chunks[0],native_map_profile,accepted_bindings=accepted_bindings)
    return {'chunks':[dict(columns=names,**carrier_chunk_check(profile,kind,language,query,names,native_map_profile,accepted_bindings=accepted_bindings))for names in chunks]}

