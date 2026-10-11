import collections,hashlib,json,pathlib,socket,subprocess,urllib.error,urllib.request
ROOT=pathlib.Path('/Users/erik/Projects/ashlar/docs/helix/04-build/evidence/puppygraph-releases-20261009')
OUT=pathlib.Path('/private/tmp/ashlar-fabric-releases-20261011-a/fabric-corpus-c.json')
assert not OUT.exists()
WORKSPACE='0a4dbd9c-e1a7-4ec8-8ce9-aa51991ff793'
RELEASES=[('r1','8947b7a0-130b-4a9a-9597-1d8a326a6f23','ca4b40f78b1a27e3d6df50eb17816ec49e30f0f70d6ff7dabc195307c07642e7'),('r2','fe883e8f-7f7d-42d0-a58d-299eafbb4e36','a6a4cb268f6bb3f8d193b35289d5b593ad08e33d8fc561e29487919fd1502e3a')]
token=json.loads(subprocess.run(['/opt/homebrew/bin/az','account','get-access-token','--resource','https://api.fabric.microsoft.com','--output','json'],check=True,capture_output=True).stdout)['accessToken']
report={'profile':'ashlar-fabric-live-corpus/0.1','observations':[],'scope':'Actual synthetic frozen release graph GQL corpus; public-reader integration remains pending.'}
def bag(rows):return collections.Counter(json.dumps(r,sort_keys=True,ensure_ascii=False,separators=(',',':')) for r in rows)
def run(release,graph,digest,name,query,expected):
 entry={'release':release,'graphId':graph,'releaseSha256':digest,'case':name,'query':query,'expected':expected};report['observations'].append(entry)
 try:
  req=urllib.request.Request('https://api.fabric.microsoft.com/v1/workspaces/'+WORKSPACE+'/graphModels/'+graph+'/executeQuery?beta=true',data=json.dumps({'query':query}).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
  with urllib.request.urlopen(req,timeout=60) as r:entry.update(httpStatus=r.status,response=json.loads(r.read(1048577)))
  body=entry['response'];assert body['status']['code']=='00000';assert body['result']['kind']=='TABLE';entry['fullBagParity']=bag(body['result']['data'])==bag(expected);assert entry['fullBagParity']
 except urllib.error.HTTPError as e:entry.update(httpStatus=e.code,errorBody=e.read(16384).decode())
 except Exception as e:entry.update(error=type(e).__name__,message=str(e))
 OUT.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n');print(release,name,entry.get('httpStatus'),entry.get('fullBagParity'),entry.get('error'),flush=True)
 return entry.get('fullBagParity') is True
for release,graph,digest in RELEASES:
 raw=(ROOT/(digest+'.graph.json')).read_bytes();assert hashlib.sha256(raw).hexdigest()==digest;orig=json.loads(raw)
 for kind,pattern in [('nodes','(n:AshlarNode)'),('edges','()-[n:AshlarEdge]->()')]:
  cols=sorted(orig[kind][0]);assert run(release,graph,digest,kind,'MATCH '+pattern+' RETURN '+', '.join('n.'+c+' AS '+c for c in cols),orig[kind])
 edges=orig['edges'];nodes=orig['nodes']
 endpoints=[{'source':e['src'],'edge':e['graph_id'],'destination':e['dst']} for e in edges]
 assert run(release,graph,digest,'endpoints','MATCH (s:AshlarNode)-[e:AshlarEdge]->(d:AshlarNode) RETURN s.graph_id AS source, e.graph_id AS edge, d.graph_id AS destination',endpoints)
 hops=[{'source':a['src'],'first_edge':a['graph_id'],'middle':a['dst'],'second_edge':b['graph_id'],'destination':b['dst']} for a in edges for b in edges if a['dst']==b['src'] and a['graph_id']!=b['graph_id']]
 assert run(release,graph,digest,'two-hop-distinct-edges','MATCH (s:AshlarNode)-[a:AshlarEdge]->(m:AshlarNode)-[b:AshlarEdge]->(d:AshlarNode) WHERE a.graph_id <> b.graph_id RETURN s.graph_id AS source, a.graph_id AS first_edge, m.graph_id AS middle, b.graph_id AS second_edge, d.graph_id AS destination',hops)
 assert run(release,graph,digest,'node-count','MATCH (n:AshlarNode) RETURN count(n) AS node_count',[{'node_count':len(nodes)}])
 assert run(release,graph,digest,'edge-count','MATCH ()-[e:AshlarEdge]->() RETURN count(e) AS edge_count',[{'edge_count':len(edges)}])
release,graph,digest=RELEASES[0];orig=json.loads((ROOT/(digest+'.graph.json')).read_bytes())
for kind,pattern in [('nodes','(n:AshlarNode)'),('edges','()-[n:AshlarEdge]->()')]:
 cols=sorted(orig[kind][0]);assert run(release,graph,digest,'after-r2-'+kind,'MATCH '+pattern+' RETURN '+', '.join('n.'+c+' AS '+c for c in cols),orig[kind])
print('all 14 live corpus checks passed',flush=True)
