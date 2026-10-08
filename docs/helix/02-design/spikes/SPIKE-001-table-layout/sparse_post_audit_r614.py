"""Audit exact current/prior responses and native I/O/latency for seventh keys."""
import json,hashlib,math,statistics
from pathlib import Path
from seventh_changes_r589 import SeventhChanges
from overlay_sql_r395 import FIELDS
B=Path(__file__).resolve().parent
def main():
 p=B/'out/native/ashlar_sparse_post_reads_r613';a=json.loads((p/'summary.json').read_text());assert a['state']=='All32matched newly changed full-carrier direct reads pass';source=B/'out/native/ashlar_sparse_maintain_r610/audited-summary.json';assert hashlib.sha256(source.read_bytes()).hexdigest()==a['source_sha256'];pub=json.loads(source.read_text());assert a['old']==pub['table'] and a['new']=={**pub['table'],'version':pub['after_version']} and a['old']['version']==10 and a['new']['version']==12
 for n,h in a['code_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==h
 rs=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());qs={q['query_id']:q for q in h['queries']};records={r['statement_id']:r for r in rs};assert len(rs)==len(qs)==len(records)==37 and h['require_final'] and not h['missing_ids']
 for r in rs:
  q=qs[r['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and r['response']['status']['state']=='SUCCEEDED' and q['query_text']=='/* ashlar '+p.name+' '+r['label']+' */ '+r['sql']
 c=SeventhChanges();assert len(a['reads'])==32;io={}
 for read in a['reads']:
  x=c.change(read['index']);row=x['after'];expected=[] if row is None else [[row[f].replace('T',' ').removesuffix('Z') if f=='published_at' else row[f] for f in FIELDS]];r=records[read['statement_id']];assert r['response'].get('result',{}).get('data_array',[])==expected and r['sql']==a['queries'][read['family']] and r['parameters']=={'hash':x['before']['lookup_hash'],'source':x['before']['source_system'],'type':x['before']['rel_type_id'],'id':x['before']['id']} and not qs[read['statement_id']]['metrics'].get('result_from_cache')
 p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
 for family in ['before','after']:
  rr=[r for r in a['reads'] if r['family']==family];assert len(rr)==16;io[family]={'read_bytes':sum(qs[r['statement_id']]['metrics']['read_bytes'] for r in rr),'median_files':statistics.median(qs[r['statement_id']]['metrics']['read_files_count'] for r in rr),'read_remote_bytes':sum(qs[r['statement_id']]['metrics']['read_remote_bytes'] for r in rr)}
  for phase in [None,0,1]:
   selected=rr if phase is None else [r for r in rr if r['phase']==phase];e=a['per_family'][family] if phase is None else a['per_family_phase'][family+'-'+str(phase)];assert len(selected)==e['queries'] and {r['index'] for r in selected}==set(a['indices'])
   want={'queries':len(selected),'caller_p95_ms':p95([records[r['statement_id']]['wall_ms'] for r in selected]),'engine_p95_ms':p95([qs[r['statement_id']]['metrics']['execution_time_ms'] for r in selected]),'compile_p95_ms':p95([qs[r['statement_id']]['metrics']['compilation_time_ms'] for r in selected]),'remote_queries':sum(qs[r['statement_id']]['metrics']['read_remote_bytes']>0 for r in selected)};assert e==want
 costs={k:sum(q['metrics'].get(k,0) for q in qs.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items()) and a['wall_s']<180
 request=json.loads((p/'inflight-request.json').read_text());assert request['state']=='returned' and request['query_id'] in qs;(p/'inflight-request.json').rename(p/'completed-last-request.json');a['audit']={'native_final_statements':37,'matched_full_carriers':32,'point_io':io,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({'io':io,'per_family_phase':a['per_family_phase']},indent=2))
if __name__=='__main__':main()
