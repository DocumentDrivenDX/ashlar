import json, hashlib, sys
from pathlib import Path
from collections import Counter, defaultdict

ROOT=Path('/Users/erik/Projects/ashlar')
def enc(v): return json.dumps(v,sort_keys=True,ensure_ascii=False,separators=(',',':'))
def bag(v): return Counter(map(enc,v))
def native(v):
    if type(v) is dict and set(v)=={'@type','@value'} and v['@type']=='g:String':v=v['@value']
    assert type(v) is str
    return v

path=Path(sys.argv[1]); raw=path.read_bytes(); report=json.loads(raw)
candidates=json.loads((ROOT/'docs/helix/04-build/evidence/original-pack-puppy-preparation-20261009/candidates.json').read_bytes())
assert report['opening']==report['closing']
assert report['common_cases_passed']==21 and report['original_authored_scenarios_qualified'] is False
assert [r['pack'] for r in report['reports']]==[c['pack'] for c in candidates]
findings=[]
for candidate,observed in zip(candidates,report['reports']):
    pack=candidate['pack']; release_bytes=Path(candidate['release']).read_bytes()
    assert hashlib.sha256(release_bytes).hexdigest()==candidate['release_sha256']
    release=json.loads(release_bytes)
    graph=json.loads((Path(candidate['publication'])/'original-graph.json').read_bytes())
    objects={r['key']:r for r in graph['objects']}; edges={r['key']:r for r in graph['edges']}
    assert len(objects)==len(graph['objects']) and len(edges)==len(graph['edges'])
    maps={}; carriers={}
    for role,original in [('nodes',objects),('edges',edges)]:
        maps[role]={};carriers[role]={}
        for row in release[role]:
            src=json.loads(row['retained_json'])['original']; key=src['key']
            assert src==original[key]
            maps[role][row['graph_id']]=key;carriers[role][key]=row
        assert set(carriers[role])==set(original)
    records={r['case']:r for r in observed['records']}
    assert len(records)==9
    for role,case in [('nodes','all-objects'),('edges','all-edges')]:
        assert bag(records[case]['canonical_rows'])==bag(release[role])
    by_type=defaultdict(list); groups=defaultdict(list)
    for key,row in objects.items():by_type[enc(row['type'])].append(key)
    for key,row in edges.items():groups[(row['source'],enc(row['relationship']))].append(row['target'])
    typ=min(by_type,key=lambda k:(-len(by_type[k]),k));chosen=sorted(by_type[typ])
    endpoints={r[x] for r in edges.values() for x in ('source','target')}
    expected={
      'one-hop': [[e['source'],k,e['target']] for k,e in edges.items()],
      'two-hop': [[e['source'],k,e['target'],fkey,f['target']] for k,e in edges.items() for fkey,f in edges.items() if e['target']==f['source']],
      'grouped-count': [[s,t,len(dst),len(set(dst))] for (s,t),dst in groups.items()],
      'filtered-total':[len(chosen)],
      'singleton':[min(objects)],'filtered-limit':chosen[:1],
      'isolates':sorted(set(objects)-endpoints)}
    for case in ('one-hop','two-hop','grouped-count','filtered-total'):
        assert bag(records[case]['canonical_rows'])==bag(expected[case]),(pack,case)
        names={'one-hop':['source','edge','target'],'two-hop':['source','edge1','middle','edge2','target'],'grouped-count':['source','relationship','occurrences','destinations']}.get(case)
        actual=records[case]['native_rows']
        actual=[[r[n] for n in names] for r in actual] if names else [r['total'] if isinstance(r,dict) else r for r in actual]
        assert bag(actual)==bag(expected[case]),(pack,case,'raw native')
    for case in ('singleton','filtered-limit','isolates'):
        assert bag(records[case]['canonical_rows'])==bag(carriers['nodes'][k] for k in expected[case]),(pack,case)
    for case in ('all-objects','all-edges','singleton','filtered-limit','isolates'):
        role='edges' if case=='all-edges' else 'nodes'; label=pack.title()+('Edge' if role=='edges' else 'Node')
        source=edges if role=='edges' else objects
        actual=[]
        for row in records[case]['native_rows']:
            key=row['original_key'];want=carriers[role][key]
            assert set(row)==set(want)|{'original_key','original_type','native_id'}|({'native_src','native_dst'}if role=='edges'else set())
            assert {k:row[k] for k in want}==want
            assert row['original_type']==enc(source[key]['relationship' if role=='edges' else 'type'])
            assert native(row['native_id'])==label+'['+want['graph_id']+']'
            if role=='edges':
                assert native(row['native_src'])==pack.title()+'Node['+want['src']+']'
                assert native(row['native_dst'])==pack.title()+'Node['+want['dst']+']'
                assert maps['nodes'][want['src']]==source[key]['source']
                assert maps['nodes'][want['dst']]==source[key]['target']
            actual.append(want)
        assert bag(actual)==bag(records[case]['canonical_rows'])
    singleton_bindings=records['singleton']['bindings']
    singleton=singleton_bindings.get('ashlarSingletonKey',singleton_bindings.get('key'))
    assert maps['nodes'][singleton]==min(objects)
    for case in ('filtered-total','filtered-limit'):
        binding=records[case]['bindings']; selected=[r for r in release['nodes'] if r['source_system']==binding['source'] and r['type_id']==binding['type']]
        assert {maps['nodes'][r['graph_id']] for r in selected}==set(chosen)
    findings.append({'pack':pack,'objects':len(objects),'edges':len(edges),'two_hop':len(expected['two-hop']),'filtered_total':len(chosen),'isolates':len(expected['isolates'])})
print(json.dumps({'report':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'language':report['language'],'verified':findings,'complete_custody_equal':True},indent=2))
