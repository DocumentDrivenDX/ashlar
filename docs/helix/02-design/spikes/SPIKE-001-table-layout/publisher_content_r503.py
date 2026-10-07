"""Fail-closed private full-content cohort; trusted generated SELECTs only.
Caller owns clients/timeouts, immutable table/version/UUID custody, terminal history
and publication fencing. This collector is not a SQL security parser or publisher.
"""
import collections,concurrent.futures,dataclasses,re

@dataclasses.dataclass(frozen=True)
class Check:
 label:str
 sql:str
 expected:tuple

class ContentFailure(ValueError):pass

def rows(value):
 if not isinstance(value,(list,tuple)) or any(not isinstance(r,(list,tuple)) for r in value):raise ContentFailure('Scalar result rows required')
 result=[]
 for row in value:
  if any(v is not None and not isinstance(v,str) for v in row):raise ContentFailure('Exact string/null transport scalars required')
  result.append(tuple(row))
 return collections.Counter(result)

def collect(clients,checks):
 if not 1<=len(checks)<=4 or len(clients)!=len(checks):raise ContentFailure('Complete one-to-one cohort of1..4 workers required')
 if len({id(c) for c in clients})!=len(clients):raise ContentFailure('A distinct owned client per check required')
 if len({str(c.out) for c in clients})!=len(clients):raise ContentFailure('Distinct durable worker directories required')
 if any(not isinstance(c,Check) or not re.fullmatch('[a-z][a-z0-9_-]*',c.label) or not isinstance(c.sql,str) or not c.sql.lstrip().upper().startswith('SELECT ') for c in checks):raise ContentFailure('Named trusted internal SELECT checks required')
 if len({c.label for c in checks})!=len(checks):raise ContentFailure('Distinct labels required')
 for check in checks:rows(check.expected)
 def run(i):
  c=clients[i];check=checks[i];before=len(c.records)
  try:
   result=c.sql(check.label,check.sql)
   if len(c.records)!=before+1:raise ContentFailure('One new durable statement record required')
   record=c.records[-1]
   if record.get('label')!=check.label or record.get('sql')!=check.sql or record.get('response',{}).get('status',{}).get('state')!='SUCCEEDED' or not isinstance(record.get('statement_id'),str) or not record['statement_id']:raise ContentFailure('Successful exact-query record with native handle required')
   if rows(result)!=rows(check.expected):raise ContentFailure('Complete exact result multiset mismatch')
   return check.label,{'statement_id':record['statement_id'],'result':result,'caller_ms':record['wall_ms']},None
  except Exception as e:return check.label,None,type(e).__name__+': '+str(e)
 # Wait for every owned worker even on failure; retain all native handles before
 # returning an error. A partial successful set is never an accepted cohort.
 with concurrent.futures.ThreadPoolExecutor(max_workers=len(checks)) as pool:result=list(pool.map(run,range(len(checks))))
 errors={label:error for label,value,error in result if error is not None}
 if errors:raise ContentFailure('No accepted cohort; inspect durable worker handles: '+repr(errors))
 accepted={label:value for label,value,error in result}
 if len(accepted)!=len(checks) or len({v['statement_id'] for v in accepted.values()})!=len(checks):raise ContentFailure('Complete distinct native handles required')
 return accepted
