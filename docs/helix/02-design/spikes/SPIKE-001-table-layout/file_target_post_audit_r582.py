"""Audit native pinned singleton responses and small-cohort metrics."""
import hashlib,json,math,statistics
from pathlib import Path
from overlay_sql_r395 import FIELDS
from mixed_changes_r228 import Changes
B=Path(__file__).resolve().parent;O=B/'out/native/file_target_post_reads_r581';a=json.loads((O/'summary.json').read_text());assert a['state']=='All32 changed/deleted singleton results agree across matched64/256 fixtures'
P=B/'out/native/file_target_merge_resume_r578/summary.json';source=json.loads(P.read_text());assert hashlib.sha256(P.read_bytes()).hexdigest()==a['source_sha256'] ;assert hashlib.sha256((B/'file_target_post_reads_r581.py').read_bytes()).hexdigest()==a['code_sha256']
r=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];h={x['query_id']:x for x in json.loads((O/'shared-history.json').read_text())['queries']};records={x['statement_id']:x for x in r};assert len(r)==len(h)==len(records)==43
for x in r:
 q=h[x['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and x['response']['status']['state']=='SUCCEEDED' and q['query_text']=='/* ashlar '+O.name+' '+x['label']+' */ '+x['sql']
assert a['custody_before']==a['custody_after']
for family,f in source['fixtures'].items():assert a['custody_before'][family]['detail']['id']==f['id'] and int(a['custody_before'][family]['head'][0])==f['after_version']
c=Changes(8000000,40000000,400000);inverse=pow(104729,-1,40000000);expected={}
for j,k in enumerate(a['keys']):
 ordinal=int(k[3])-8000001;index=ordinal*inverse%40000000
 assert 300000<=index<400000;x=c.change(index);row=x['after']
 assert k[:4]==[x['before']['lookup_hash'],x['before']['source_system'],x['before']['rel_type_id'],x['before']['id']] and (k[4]=='true')==(row is None)
 if row is None:expected[j]=[]
 else:
  row['apply_batch_id']='mixed-change/4';expected[j]=[[row[f].replace('T',' ').removesuffix('Z') if f=='published_at' else row[f] for f in FIELDS]]
for x in a['reads']:
 rec=records[x['statement_id']];assert rec['sql']==a['queries'][x['family']] and rec['parameters']==dict(zip(['hash','source','type','id'],a['keys'][x['key']][:4])) and rec['response']['result']['data_array']==x['result']==expected[x['key']] and not h[x['statement_id']]['metrics'].get('result_from_cache')
p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1];io={}
for family in ['file64','file256']:
 rr=[x for x in a['reads'] if x['family']==family];assert len(rr)==16
 io[family]={'read_bytes':sum(h[x['statement_id']]['metrics']['read_bytes'] for x in rr),'remote_bytes':sum(h[x['statement_id']]['metrics']['read_remote_bytes'] for x in rr),'median_files':statistics.median(h[x['statement_id']]['metrics']['read_files_count'] for x in rr)}
 for phase in [0,1]:
  group=[x for x in rr if x['phase']==phase];assert len(group)==8 and {x['key'] for x in group}==set(range(8))
  want={'queries':8,'caller_p95_ms':p95([records[x['statement_id']]['wall_ms'] for x in group]),'engine_p95_ms':p95([h[x['statement_id']]['metrics']['execution_time_ms'] for x in group]),'compile_p95_ms':p95([h[x['statement_id']]['metrics']['compilation_time_ms'] for x in group]),'read_bytes':sum(h[x['statement_id']]['metrics']['read_bytes'] for x in group),'remote_queries':sum(h[x['statement_id']]['metrics']['read_remote_bytes']>0 for x in group)};assert want==a['groups'][family+'-'+str(phase)]
cost={k:sum(x['metrics'].get(k,0) or 0 for x in h.values()) for k in a['costs']};assert cost==a['costs'] and all(v<=a['bounds'][k] for k,v in cost.items()) and a['wall_s']<180
result={'state':'All43 exact native statements and32 changed/deleted responses match independent fourth-batch carriers','groups':a['groups'],'io':io,'costs':cost,'wall_s':a['wall_s'],'qualification':a['qualification']+' Nearest-rank p95 over eight samples is the maximum; not a production p95.'};(B/'out/file-target-post-audit-r582.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
