import pathlib,json,sqlite3,hashlib,urllib.parse
import duckdb
assert duckdb.__version__=='1.4.1'
def enc(x):return json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
def digest(x):return hashlib.sha256(enc(x).encode()).hexdigest()
base=pathlib.Path('/private/tmp/ashlar-medical-initialization-installed-prep-c');installation=json.loads((base/'installation.json').read_bytes());c=sqlite3.connect('file:'+installation['journal']+'?mode=ro',uri=True);c.execute('pragma query_only=on');sizes=c.execute('select length(receipt) from local_operation').fetchall();assert len(sizes)==5 and sum(n[0] for n in sizes)<16*1024*1024
receipts=[json.loads(r) for r, in c.execute('select receipt from local_operation')];c.close();db=duckdb.connect(':memory:');db.execute("SET memory_limit='128MB'");db.execute('SET threads=1');total=0;proof=[];logpins=[]
def observe(target,version):
 global total
 p=pathlib.Path(target['path']);logs=sorted((p/'_delta_log').glob('*.json'));assert [int(x.stem) for x in logs]==list(range(len(logs)));assert version<len(logs)
 active={};meta=protocol=None;commit=None
 for log in logs[:version+1]:
  raw=log.read_bytes();assert len(raw)<=1024*1024;logpins.append({'path':str(log),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
  for line in raw.splitlines():
   action=json.loads(line)
   if 'metaData' in action:meta=action['metaData']
   if 'protocol' in action:protocol=action['protocol']
   if 'add' in action:
    a=action['add'];assert 'deletionVector' not in a and not a.get('partitionValues');name=a['path'];assert not pathlib.PurePosixPath(name).is_absolute() and '..' not in pathlib.PurePosixPath(name).parts;active[name]=a
   if 'remove' in action:active.pop(action['remove']['path'],None)
   if int(log.stem)==version and 'commitInfo' in action:commit=action['commitInfo']
 role=target['table'].split('.')[-1];expected=installation['native'][role]['registration'];assert meta['id']==target['uuid']==expected['uuid'];schema=json.loads(meta['schemaString']);assert schema==expected['schema'];assert protocol=={'minReaderVersion':1,'minWriterVersion':2};assert meta['configuration']==expected['properties'];assert all(f['type'] in ('string','long','timestamp') for f in schema['fields'])
 names=[f['name'] for f in schema['fields']];rows=[];pins=[]
 for name,a in active.items():
  file=(p/urllib.parse.unquote(name)).resolve();assert file.is_relative_to(p.resolve()) and file.is_file() and file.stat().st_size==a['size'];pins.append({'path':str(file),'bytes':file.stat().st_size,'sha256':hashlib.sha256(file.read_bytes()).hexdigest()})
  projection=','.join(('CAST(epoch_us("'+f['name']+'") AS VARCHAR)' if f['type']=='timestamp' else 'CAST("'+f['name']+'" AS VARCHAR)')+' AS "'+f['name']+'"' for f in schema['fields']);cursor=db.execute('SELECT '+projection+' FROM read_parquet(?) LIMIT 10001',[str(file)]);assert [d[0] for d in cursor.description]==names
  values=cursor.fetchall();assert len(values)+len(rows)<=10000
  for vals in values:
   assert all(v is None or type(v)is str for v in vals)
   for v in vals:
    n=0 if v is None else len(v.encode());assert n<=1024*1024;total+=n;assert total<=16*1024*1024
   rows.append(dict(zip(names,vals)))
 rows.sort(key=enc);return {'schema':[[f['name'],{'long':'bigint','timestamp':'timestamp','string':'string'}[f['type']]] for f in schema['fields']],'rows':rows,'row_sha256':digest(rows)},commit,pins,len(logs)-1
for receipt in receipts:
 target=next(t for t in installation['targets'] if t['table']==receipt['table']);snapshot,commit,pins,latest=observe(target,receipt['version']);assert snapshot==receipt['snapshot'];assert commit['userMetadata']==receipt['commit_metadata'] and commit['operation']==receipt['native_operation'];assert receipt['uuid']==target['uuid']
 proof.append({'operation':receipt['operation'],'table':target['table'],'version':receipt['version'],'rows':len(snapshot['rows']),'row_sha256':snapshot['row_sha256'],'receipt_sha256':digest(receipt),'parquet':pins,'metadata_and_snapshot_exact':True})
current=[]
for target in installation['targets']:
 versions=list((pathlib.Path(target['path'])/'_delta_log').glob('*.json'));snapshot,commit,pins,latest=observe(target,len(versions)-1);current.append({'table':target['table'],'uuid':target['uuid'],'version':latest,'rows':len(snapshot['rows']),'row_sha256':snapshot['row_sha256'],'parquet':pins})
db.close();report={'profile':'ashlar-medical-partial-native-receipt-correspondence/0.1','qualification':'Actual read-only DuckDB1.4.1 Parquet/Delta JSON correspondence to five ORIGINAL SQLite committed receipts and current six targets only. No independent original-pack semantic pass, publication authority, native recovery reconciliation, success publication, durable ACK or primary failure cause claim. No Spark, writer, effects, repair, reset or retry. SQLite mode=ro/query_only closed; in-memory DuckDB closed.','bounds':{'rows':10000,'cell_bytes':1048576,'total_bytes':16777216,'actual_cumulative_cell_bytes':total},'receipts':proof,'current_targets':current,'log_pins':list({x['path']:x for x in logpins}.values())};out=pathlib.Path('/private/tmp/ashlar-medical-native-receipt-audit-e.json');out.write_text(json.dumps(report,sort_keys=True,indent=2)+'\n');print(hashlib.sha256(out.read_bytes()).hexdigest());print([(x['table'],x['version'],x['rows']) for x in current])
