"""Local columnar screening only; no Delta/graph-engine performance claim."""
import argparse, hashlib, json, math, platform, shutil, subprocess, tempfile
from pathlib import Path
BASE=Path(__file__).resolve().parent
parser=argparse.ArgumentParser();parser.add_argument('--sizes',default='10000,100000');parser.add_argument('--repeats',type=int,default=7);args=parser.parse_args()
exe=shutil.which('duckdb')
if not exe:raise SystemExit('duckdb CLI required')
OUT=BASE/'out';OUT.mkdir(exist_ok=True)
report={'scope':'local DuckDB screening; warm same-process queries; not Delta/Spark/PuppyGraph/Fabric evidence','environment':{'engine':subprocess.check_output([exe,'--version'],text=True).strip(),'platform':platform.platform(),'machine':platform.machine(),'threads':4,'memory_limit':'2GB'},'repeats':args.repeats,'runs':[]}
for n in map(int,args.sizes.split(',')):
 with tempfile.TemporaryDirectory(prefix='ashlar-layout-') as tmp:
  temp=Path(tmp); seed=f"""
SET threads=4; SET memory_limit='2GB';
CREATE TABLE objects AS SELECT t AS type_id, i AS id,
 json_object('101', 'g'||cast(i%100 as varchar), '102', i%1000, '103', repeat('x',512))::VARCHAR AS props,
 '{{"future":{{"preserve":true}}}}' AS retained
FROM range(1,4) tt(t) CROSS JOIN range(1,{n+1}) ii(i) ORDER BY type_id,id;
CREATE TABLE edges AS SELECT rel AS rel_type_id, (rel-1)*{n*4}+i*4+k AS id,
 rel AS source_type, i AS source_id, rel+1 AS target_type,
 CASE WHEN i%100=0 THEN 1 ELSE ((i+k-1)%{n})+1 END AS target_id,
 json_object('201', k/10.0)::VARCHAR AS props
FROM range(1,3) rr(rel) CROSS JOIN range(1,{n+1}) ii(i) CROSS JOIN range(0,4) kk(k)
ORDER BY rel_type_id,source_id,id;
-- Controlled hub: n additional AB edges from A1, with stable edge identities.
INSERT INTO edges SELECT 1, {n*20}+i, 1,1,2,i,'{{"201":0.9}}' FROM range(1,{n+1}) ii(i);
CREATE TABLE promoted_objects AS SELECT *, json_extract_string(props,'$.101') AS group_value,
 cast(json_extract_string(props,'$.102') AS BIGINT) AS rank_value FROM objects ORDER BY type_id,id;
CREATE TABLE promoted_edges AS SELECT *,cast(json_extract_string(props,'$.201') AS DOUBLE) AS score FROM edges ORDER BY rel_type_id,source_id,id;
CREATE TABLE node_a AS SELECT * FROM promoted_objects WHERE type_id=1 ORDER BY id;
CREATE TABLE node_b AS SELECT * FROM promoted_objects WHERE type_id=2 ORDER BY id;
CREATE TABLE node_c AS SELECT * FROM promoted_objects WHERE type_id=3 ORDER BY id;
CREATE TABLE edge_ab AS SELECT * FROM promoted_edges WHERE rel_type_id=1 ORDER BY source_id,id;
CREATE TABLE edge_bc AS SELECT * FROM promoted_edges WHERE rel_type_id=2 ORDER BY source_id,id;
CREATE TABLE edge_ab_reverse AS SELECT * FROM edge_ab ORDER BY target_id,id;
"""
  queries={}
  for layout in ('bag','promoted','typed'):
   nodes='objects' if layout=='bag' else 'promoted_objects' if layout=='promoted' else 'node_a'
   eb='edges' if layout=='bag' else 'promoted_edges' if layout=='promoted' else 'edge_ab'
   ec='edges' if layout=='bag' else 'promoted_edges' if layout=='promoted' else 'edge_bc'
   nb='objects' if layout=='bag' else 'promoted_objects' if layout=='promoted' else 'node_b'
   group="json_extract_string(props,'$.101')" if layout=='bag' else 'group_value'
   score=lambda alias: f"cast(json_extract_string({alias}.props,'$.201') AS DOUBLE)" if layout=='bag' else f'{alias}.score'
   queries[(layout,'lookup')]=f'SELECT id,props,retained FROM {nodes} WHERE type_id=1 AND id=123;'
   queries[(layout,'list')]=f"SELECT id FROM {nodes} WHERE type_id=1 AND {group}='g42' ORDER BY id LIMIT 100;"
   queries[(layout,'count')]=f'SELECT {group} AS grp,count(*) AS n FROM {nodes} WHERE type_id=1 GROUP BY grp ORDER BY grp;'
   queries[(layout,'one_hop')]=f'SELECT id,target_id FROM {eb} WHERE rel_type_id=1 AND source_type=1 AND source_id=123 ORDER BY id;'
   queries[(layout,'two_hop')]=f"SELECT e.id AS first_edge,f.id AS second_edge,f.target_id FROM {eb} e JOIN {ec} f ON e.target_id=f.source_id AND e.target_type=f.source_type JOIN {nb} b ON b.id=e.target_id AND b.type_id=e.target_type WHERE e.rel_type_id=1 AND e.source_id=123 AND e.source_type=1 AND f.rel_type_id=2 AND {score('e')}>=0.1 AND {score('f')}>=0.1 AND " + ("cast(json_extract_string(b.props,'$.102') AS BIGINT)" if layout=='bag' else 'b.rank_value') + '>100 ORDER BY first_edge,second_edge;'
   queries[(layout,'reverse')]=f'SELECT id,source_id FROM {eb} WHERE rel_type_id=1 AND target_type=2 AND target_id=1 ORDER BY id;'
   queries[(layout,'hub_bounded')]=f'SELECT id,target_id FROM {eb} WHERE rel_type_id=1 AND source_id=1 ORDER BY id LIMIT 100;'
   queries[(layout,'hub_count')]=f'SELECT count(*) AS n FROM {eb} e JOIN {ec} f ON e.target_id=f.source_id AND e.target_type=f.source_type WHERE e.rel_type_id=1 AND e.source_id=1 AND e.source_type=1 AND f.rel_type_id=2;'
  queries[('typed_reverse','reverse')]=f'SELECT id,source_id FROM edge_ab_reverse WHERE rel_type_id=1 AND target_type=2 AND target_id=1 ORDER BY id;'
  (BASE/'sql'/f'screening-{n}.sql').write_text(seed+'\n'+ '\n'.join('-- '+str(k)+'\n'+v for k,v in queries.items()))
  script=['.bail on','.mode json',seed];items=[]
  for (layout,query),sql in queries.items():
   for rep in range(args.repeats+1):
    tag=f'{layout}-{query}-{rep}';profile=temp/(tag+'-profile.json');result=temp/(tag+'-result.json')
    script.extend([f"PRAGMA enable_profiling='json';",f"PRAGMA profiling_output='{profile}';",f'.once {result}',sql])
    items.append((layout,query,rep,profile,result))
  # Export each alternative as independently compressed, sorted Parquet.
  exports={'bag':['objects','edges'],'promoted':['promoted_objects','promoted_edges'],'typed':['node_a','node_b','node_c','edge_ab','edge_bc'],'typed_reverse':['edge_ab_reverse']}
  for layout,tables in exports.items():
   for table in tables:script.append(f"COPY {table} TO '{temp/table}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);")
  completed=subprocess.run([exe,'-batch',str(temp/'bench.duckdb')],input='\n'.join(script),text=True,capture_output=True)
  if completed.returncode:raise RuntimeError(completed.stderr)
  run={'nodes':n*3,'edges':n*9,'rows_per_node_type':n,'measurements':[],'parquet_bytes':{layout:sum((temp/(table+'.parquet')).stat().st_size for table in tables) for layout,tables in exports.items()}}
  grouped={};fingerprints={}
  for layout,query,rep,profile,result in items:
   prof=json.loads(profile.read_text());rows=json.loads(result.read_text());digest=hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':')).encode()).hexdigest()
   prior=fingerprints.setdefault(query,digest)
   assert prior==digest,(layout,query,'differential result mismatch')
   if rep:grouped.setdefault((layout,query),[]).append(prof['latency']*1000)
   if rep==1:
    (OUT/f'plan-{n}-{layout}-{query}.json').write_text(json.dumps(prof,indent=2)+'\n')
  for (layout,query),ms in grouped.items():
   values=sorted(ms);run['measurements'].append(dict(layout=layout,query=query,median_ms=round(values[len(values)//2],3),p95_ms=round(values[math.ceil(.95*len(values))-1],3),samples_ms=[round(v,3) for v in ms],result_sha256=fingerprints[query]))
  report['runs'].append(run)
  print(f'{n*3} nodes / {n*9} edges: {len(grouped)} measurements, results agree',flush=True)
(OUT/'screening.json').write_text(json.dumps(report,indent=2)+'\n')
print(OUT/'screening.json')
