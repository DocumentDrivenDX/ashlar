"""Fail-closed metadata checks; observations are not a writer fence."""
import concurrent.futures,json,re
DETAIL_FIELDS=('properties','partitionColumns','clusteringColumns','tableFeatures','minReaderVersion','minWriterVersion')
STRUCTURED=set(DETAIL_FIELDS[:4])
class CustodyMismatch(ValueError):pass
def objects(c,label,q):
 rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
 if len(cols)!=len(set(cols)) or any(len(r)!=len(cols) for r in rows):raise CustodyMismatch('Malformed metadata shape')
 return [dict(zip(cols,r)) for r in rows]
def capture(c,key,table):
 if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){2}',table):raise CustodyMismatch('Invalid qualified table')
 d=objects(c,'detail-'+key,'DESCRIBE DETAIL '+table);h=objects(c,'head-'+key,'DESCRIBE HISTORY '+table+' LIMIT 1');s=objects(c,'schema-'+key,'DESCRIBE TABLE '+table)
 if len(d)!=1 or len(h)!=1 or not s:raise CustodyMismatch('Incomplete metadata '+key)
 return {'key':key,'table':table,'detail':d[0],'head':h[0],'schema':s}
def structured(v):return json.loads(v) if isinstance(v,str) else v
def verify(actual,expected):
 try:
  if actual['key']!=expected['key'] or actual['table']!=expected['table']:raise CustodyMismatch('Table binding changed')
  if actual['detail']['id']!=expected['detail']['id'] or actual['detail']['format']!='delta':raise CustodyMismatch('UUID/format changed')
  if int(actual['head']['version'])!=int(expected['head']['version']):raise CustodyMismatch('Head changed')
  if actual['schema']!=expected['schema']:raise CustodyMismatch('Schema changed')
  for f in DETAIL_FIELDS:
   av=actual['detail'][f];ev=expected['detail'][f]
   if (structured(av)!=structured(ev) if f in STRUCTURED else str(av)!=str(ev)):raise CustodyMismatch('Metadata changed: '+f)
 except (KeyError,TypeError,json.JSONDecodeError) as e:raise CustodyMismatch('Incomplete/invalid custody metadata') from e
 return actual
def collect(clients,expected):
 if not clients or not expected or len({x['key'] for x in expected})!=len(expected):raise CustodyMismatch('Invalid custody cohort')
 def worker(j):return [verify(capture(clients[j],e['key'],e['table']),e) for e in expected[j::len(clients)]]
 with concurrent.futures.ThreadPoolExecutor(max_workers=len(clients)) as pool:results=[x for group in pool.map(worker,range(len(clients))) for x in group]
 if sorted(x['key'] for x in results)!=sorted(x['key'] for x in expected):raise CustodyMismatch('Incomplete custody cohort')
 return sorted(results,key=lambda x:x['key'])
