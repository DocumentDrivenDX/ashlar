"""Finalize native terminal costs for canceled maintenance attempts; no SQL writes."""
import json,hashlib
from pathlib import Path
from persistent_sql import Client
from mixed_change_queries_r230 import row_hash_sql
B=Path(__file__).resolve().parent

def main():
 totals={k:0 for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
 for name in ['ashlar_lc_incremental_maintain_r341','ashlar_lc_incremental_maintain_r343','ashlar_lc_incremental_maintain_r347']:
  p=B/'out/native'/name;c=Client(p);c.records=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=c.history();native={q['query_id']:q for q in h};assert len(h)==len(c.records)==len({r['statement_id'] for r in c.records})
  for r in c.records:
   q=native[r['statement_id']];assert q['is_final'] and q['query_text']==r['sql'];assert (r['response']['status']['state'],q['status']) in [('SUCCEEDED','FINISHED'),('CANCELED','CANCELED')]
  a=json.loads((p/'cancel-audit.json').read_text());final=c.w.api_client.do('GET','/api/2.0/sql/statements/'+a['same_sid']);assert final['status']['state']=='CANCELED' and a['get_status']['state']=='CANCELED' and a['native']['query_id']==a['same_sid'];assert int(a['head_history'][0]['version'])==3 and not any(x['operation']=='OPTIMIZE' for x in a['head_history'])
  a['all_attempt_costs']={k:sum(q['metrics'].get(k,0) for q in h) for k in totals}
  for k,v in a['all_attempt_costs'].items():totals[k]+=v
  a['audit']={'final_statements':len(h),'same_sid_get_canceled':True,'includes_terminal_inspection_sql':True,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','query-history.json']}};(p/'audited-cancel.json').write_text(json.dumps(a,indent=2)+'\n')
 p=B/'out/native/ashlar_lc_incremental_maintain_r343';a=json.loads((p/'summary.json').read_text());records=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h={q['query_id']:q for q in json.loads((p/'query-history.json').read_text())};groups=[];t=a['table'];fields=json.loads((B/'out/second-cdf-image-oracle-r327.json').read_text())['roles']['edge_current']['fields'];hs=row_hash_sql(fields)
 for first in range(0,400,100):
  r=next(r for r in records if r['label']=='before-all-'+str(first));lo=8000001+first*100000;hi=lo+10000000;expected=f"SELECT CAST(floor((id-8000001)/100000) AS BIGINT) group_id,count(*),sha2(concat_ws('',sort_array(collect_list({hs}))),256) FROM {t} VERSION AS OF 3 WHERE id>={lo} AND id<{hi} GROUP BY group_id ORDER BY group_id";assert r['sql']==expected and not h[r['statement_id']]['metrics'].get('result_from_cache');rows=r['response']['result']['data_array'];assert len(rows)==100 and [int(x[0]) for x in rows]==list(range(first,first+100));groups.extend(rows)
 assert groups==a['checks']['before'] and sum(int(x[1]) for x in groups)==39980000
 report={'state':'Canceled whole maintenance attempts are terminal and head remains3; complete baseline verified','complete_baseline_rows':39980000,'complete_baseline_fields':20,'complete_baseline_groups':400,'all_attempt_costs':totals,'bounds':{'combined_read_bytes':500000000000,'combined_write_bytes':80000000000,'combined_spill_bytes':5000000000},'qualification':'Cancellation plus actual no-OPTIMIZE head history, not successful maintenance or physical orphan inventory.25.25GB reported attempted writes are charged even though no Delta commit. No cleanup/retry, graph/production pointer/ACK, singleton improvement or billion admission.'};assert totals['read_bytes']<=500000000000 and totals['write_remote_bytes']<=80000000000 and totals['spill_to_disk_bytes']<=5000000000;(B/'out/lc-maintenance-canceled-disposition-r349.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
if __name__=='__main__':main()
