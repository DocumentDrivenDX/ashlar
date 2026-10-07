"""Shared-lane bounded native history scan; keep completion and final costs distinct."""
import json
from pathlib import Path
class HistoryPending(ValueError):
 pass
def collect_history(workspace,records,path,*,require_final=True):
 if type(require_final) is not bool:raise ValueError('Explicit finality mode required')
 ids=[]
 for r in records:
  qid=r.get('statement_id')
  if not isinstance(qid,str) or not qid:raise ValueError('Resolve unknown query ID first')
  if r.get('response',{}).get('status',{}).get('state')!='SUCCEEDED':raise ValueError('Successful client result required')
  ids.append(qid)
 if len(ids)!=len(set(ids)):raise ValueError('Duplicate native IDs')
 selected={};token=None;pages=0
 for _ in range(8):
  params={'max_results':1000,'include_metrics':'true'}
  if token:params['page_token']=token
  response=workspace.api_client.do('GET','/api/2.0/sql/history/queries',query=params);pages+=1
  for q in response.get('res',[]):
   qid=q.get('query_id')
   if qid in ids:
    if qid in selected and selected[qid]!=q:raise ValueError('Conflicting native history')
    selected[qid]=q
  if set(ids).issubset(selected) or not response.get('has_next_page'):break
  token=response.get('next_page_token')
  if not isinstance(token,str) or not token:raise ValueError('Missing pagination token')
 report={'pages':pages,'require_final':require_final,'queries':list(selected.values()),'missing_ids':sorted(set(ids)-set(selected)),'qualification':'FINISHED plus successful client result qualifies completion; nonfinal metrics are not complete cost.'}
 Path(path).write_text(json.dumps(report,indent=2)+'\n')
 if report['missing_ids']:raise HistoryPending('Missing native history; inspect same IDs')
 for q in selected.values():
  if q.get('status') in ('RUNNING','QUEUED'):raise HistoryPending('Native history still running; inspect same IDs')
  if q.get('status')!='FINISHED':raise ValueError('Native query not successfully finished')
  if require_final and q.get('is_final') is not True:raise HistoryPending('Metrics not final')
 return selected
