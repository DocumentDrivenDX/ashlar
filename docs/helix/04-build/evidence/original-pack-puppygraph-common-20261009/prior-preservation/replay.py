"""Bounded preservation replay of already qualified labels, no activation."""
import hashlib,json,os
from pathlib import Path
from check_pack_puppygraph import observe_native
from check_puppygraph_releases import check as releases
from check_commerce_puppygraph import check as commerce
from check_commerce_gremlin import check as commerce_gremlin
from check_graph_augmentations_cypher import check as authored_cypher
from check_graph_augmentations_gremlin import check as authored_gremlin
from check_commerce_exact_puppygraph import admitted,cypher as exact_cypher,gremlin as exact_gremlin
p=Path('/private/tmp/ashlar-pack-puppy-activation-inputs-20261009-a')
model=json.loads((p/'model.json').read_bytes());databases=json.loads((p/'databases.json').read_bytes())
u=os.environ['ASHLAR_PUPPY_USER'];pw=os.environ['ASHLAR_PUPPY_PASSWORD'];bolt='bolt://127.0.0.1:17887';gremlin='ws://127.0.0.1:18182/gremlin'
observe=lambda:observe_native('Cypher',bolt,model,databases,u,pw)
opening=observe()
source=Path('/private/tmp/ashlar-local-graph-refresh-20261009-a/source')
inputs={n:(str(source/(h+'.graph.json')),h)for n,h in [('R1','ca4b40f78b1a27e3d6df50eb17816ec49e30f0f70d6ff7dabc195307c07642e7'),('R2','a6a4cb268f6bb3f8d193b35289d5b593ad08e33d8fc561e29487919fd1502e3a')]}
records={'releases':releases(inputs,bolt=bolt,gremlin=gremlin,user=u,password=pw)}
c=Path('/private/tmp/ashlar-commerce-graph-export-20261009-b');r=(c/'release.graph.json').read_bytes();s=(c/'custody.json').read_bytes();rs=hashlib.sha256(r).hexdigest();cs=hashlib.sha256(s).hexdigest()
records['commerce-cypher']=commerce(r,s,rs,cs,bolt=bolt,user=u,password=pw)
records['commerce-gremlin']=commerce_gremlin(r,s,rs,cs,endpoint=gremlin,user=u,password=pw)
args=(Path('examples/graph-augmentations/model.umf.json').read_bytes(),Path('examples/graph-augmentations/source.json').read_bytes(),Path('docs/helix/04-build/evidence/graph-augmentations-graphframes-20261009/public-receipt.json').read_bytes(),Path('/private/tmp/ashlar-umf-dataset-45473'))
records['authored-cypher']=authored_cypher(*args,bolt,u,pw)
records['authored-gremlin']=authored_gremlin(*args,gremlin,u,pw)
e=Path('/private/tmp/ashlar-commerce-exact-puppy-prepared-20261009-b/receipt.json').read_bytes();exact=admitted(r,s,rs,cs,e,hashlib.sha256(e).hexdigest())
records['exact-cypher']=exact_cypher(exact,bolt,u,pw);records['exact-gremlin']=exact_gremlin(exact,gremlin,u,pw)
closing=observe()
if closing!=opening:raise ValueError('Complete five database/model/container preservation changed')
out=Path('/private/tmp/ashlar-pack-puppy-prior-replay-20261009-a.json')
if out.exists():raise ValueError('Fresh report required')
out.write_text(json.dumps({'format':'ashlar-puppygraph-prior-label-preservation/0.1','records':records,'opening':opening,'closing':closing,'scope':'Read-only replay of existing qualified label query bodies under current full requested/observed model and five sealed DB custody; no atomic activation or new original17 qualification.'},sort_keys=True,indent=2)+'\n')
