"""Audit native pinned singleton responses and small-cohort metrics."""
import hashlib,json,math,statistics
from pathlib import Path
from overlay_sql_r395 import FIELDS
from mixed_changes_r228 import Changes
B=Path(__file__).resolve().parent;O=B/'out/native/file256_reads_r574';a=json.loads((O/'summary.json').read_text());assert a['state']=='All32 complete singleton responses agree across file targets with unchanged custody'
P=B/'out/native/file256_recover_r571/summary.json';source=json.loads(P.read_text());assert hashlib.sha256(P.read_bytes()).hexdigest()==a['source_sha256'] and a['table']==source['table'] and a['id']==source['id'];assert hashlib.sha256((B/'file256_reads_r574.py').read_bytes()).hexdigest()==a['code_sha256']
r=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];h={x['query_id']:x for x in json.loads((O/'shared-history.json').read_text())['queries']};records={x['statement_id']:x for x in r};assert len(r)==len(h)==len(records)==39
for x in r:
 q=h[x['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and x['response']['status']['state']=='SUCCEEDED' and q['query_text']=='/* ashlar '+O.name+' '+x['label']+' */ '+x['sql']
assert a['custody_before']==a['custody_after'] and a['custody_before']['detail']['id']==source['id']
c=Changes(8000000,40000000,300000);inverse=pow(104729,-1,40000000);expected={}
for j,k in enumerate(a['keys']):
 ordinal=int(k[3])-8000001;index=ordinal*inverse%40000000
 if index<300000:
  x=c.change(index);row=x['after'];assert row is not None;row['apply_batch_id']='mixed-change/'+str(index//100000+1)
 else:row=c.w.carrier('edge',ordinal)
 assert k==[row['lookup_hash'],row['source_system'],row['rel_type_id'],row['id']]
 expected[j]=[[row[f].replace('T',' ').removesuffix('Z') if f=='published_at' else row[f] for f in FIELDS]]
for x in a['reads']:
 rec=records[x['statement_id']];assert rec['sql']==a['queries'][x['family']] and rec['parameters']==dict(zip(['hash','source','type','id'],a['keys'][x['key']])) and rec['response']['result']['data_array']==x['result']==expected[x['key']] and not h[x['statement_id']]['metrics'].get('result_from_cache')
p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1];io={}
for family in ['before64','after256']:
 rr=[x for x in a['reads'] if x['family']==family];assert len(rr)==16
 io[family]={'read_bytes':sum(h[x['statement_id']]['metrics']['read_bytes'] for x in rr),'remote_bytes':sum(h[x['statement_id']]['metrics']['read_remote_bytes'] for x in rr),'median_files':statistics.median(h[x['statement_id']]['metrics']['read_files_count'] for x in rr)}
 for phase in [0,1]:
  group=[x for x in rr if x['phase']==phase];assert len(group)==8 and {x['key'] for x in group}==set(range(8))
  want={'queries':8,'caller_p95_ms':p95([records[x['statement_id']]['wall_ms'] for x in group]),'engine_p95_ms':p95([h[x['statement_id']]['metrics']['execution_time_ms'] for x in group]),'compile_p95_ms':p95([h[x['statement_id']]['metrics']['compilation_time_ms'] for x in group]),'read_bytes':sum(h[x['statement_id']]['metrics']['read_bytes'] for x in group),'remote_queries':sum(h[x['statement_id']]['metrics']['read_remote_bytes']>0 for x in group)};assert want==a['groups'][family+'-'+str(phase)]
cost={k:sum(x['metrics'].get(k,0) or 0 for x in h.values()) for k in a['costs']};assert cost==a['costs'] and all(v<=a['bounds'][k] for k,v in cost.items()) and a['wall_s']<180
result={'state':'All39 exact native statements and32 full singleton responses match independent synthetic carriers','groups':a['groups'],'io':io,'costs':cost,'wall_s':a['wall_s'],'qualification':a['qualification']+' Nearest-rank p95 over eight samples is the maximum; not a production p95.'};(B/'out/file256-reads-audit-r575.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
