import json,hashlib,sqlite3,time
from pathlib import Path
import sys
from contextlib import closing
sys.path.insert(0,'/Users/erik/Projects/ashlar/src')
from ashlar_host.medical_installation import OriginalMedicalInstallationSelection,OriginalMedicalInstallationLease
from ashlar_host.delta_custody import DeltaTarget,encoded
r=Path(__file__).parent;config=Path('/private/tmp/ashlar-medical-recovery-installed-prep-h/configuration.json');raw=config.read_bytes();q=json.loads(raw)
handoff_raw=Path(q['installation']['path']).read_bytes()
if hashlib.sha256(handoff_raw).hexdigest()!=q['installation']['sha256'] or len(handoff_raw)!=q['installation']['bytes']:raise ValueError('Original installation handoff differs')
handoff=json.loads(handoff_raw)
if handoff['installation_id']!=q['installation_id'] or handoff['journal']!=q['journal'] or handoff['capacity']!=q['capacity']:raise ValueError('Original installation selection differs')
operator=OriginalMedicalInstallationSelection(handoff['installation_id'],Path(handoff['journal']),tuple(DeltaTarget(x['table'],Path(x['path']),x['uuid'])for x in handoff['targets']),handoff['capacity']);context=object();t=time.monotonic();answer=None
with OriginalMedicalInstallationLease.open(operator,context,journal_bytes=q['bounds']['journal_bytes'])as owner:
 with closing(sqlite3.connect(operator.journal.as_uri()+'?mode=ro',uri=True,timeout=1))as db:
  db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
  operations=[]
  for operation,intent,digest,before,state,receipt in db.execute('SELECT operation,intent,intent_sha,before_version,state,receipt FROM local_operation ORDER BY operation'):
   if hashlib.sha256(intent.encode()).hexdigest()!=digest:raise ValueError('Original operation intent differs')
   operations.append({'operation':operation,'intent_sha256':digest,'before_version':before,'state':state,'receipt':None if receipt is None else json.loads(receipt),'receipt_original_sha256':None if receipt is None else hashlib.sha256(receipt.encode()).hexdigest()})
  reconciliation=[]
  targets={target.table:target for target in operator.targets}
  for record in operations:
   receipt=record['receipt'];target=targets[receipt['table']]
   log=target.path/'_delta_log'/(str(receipt['version']).zfill(20)+'.json');lograw=log.read_bytes();entries=[json.loads(line)for line in lograw.splitlines()]
   commits=[e['commitInfo']for e in entries if 'commitInfo'in e]
   if len(commits)!=1 or commits[0].get('userMetadata')!=receipt['commit_metadata'] or commits[0].get('operation')!=receipt['native_operation']:raise ValueError('Original native commit correspondence differs')
   metadata=json.loads(receipt['commit_metadata'])
   if metadata['operation']!=record['operation'] or metadata['operation_sha256']!=record['intent_sha256'] or metadata['installation_id']!=operator.installation_id or receipt['uuid']!=target.uuid:raise ValueError('Original operation/installation correspondence differs')
   if hashlib.sha256(encoded(receipt['snapshot']['rows']).encode()).hexdigest()!=receipt['snapshot']['row_sha256']:raise ValueError('Original receipt row digest differs')
   native_metadata=[]
   for version in range(receipt['version']+1):
    native_metadata += [e['metaData']for e in map(json.loads,(target.path/'_delta_log'/(str(version).zfill(20)+'.json')).read_bytes().splitlines())if 'metaData'in e]
   if not native_metadata or native_metadata[-1]['id']!=target.uuid:raise ValueError('Original native UUID correspondence differs')
   reconciliation.append({'operation':record['operation'],'table':target.table,'version':receipt['version'],'log_sha256':hashlib.sha256(lograw).hexdigest(),'receipt_rows':len(receipt['snapshot']['rows']),'native_commit_metadata_equal':True,'original_receipt_row_digest_valid':True,'native_uuid_equal':True})
  tables={name:db.execute('SELECT count(*) FROM '+name).fetchone()[0]for name in ('local_installation','local_operation','local_plan','local_source_plan','local_publication_artifact')}
  originals={name:[hashlib.sha256(json.dumps(row,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()for row in db.execute('SELECT * FROM '+name+' ORDER BY 1')]for name in tables}
  answer={'configuration_sha256':hashlib.sha256(raw).hexdigest(),'installation_id':operator.installation_id,'journal':str(operator.journal),'journal_mode':db.execute('PRAGMA journal_mode').fetchone()[0],'table_counts':tables,'reconciliation':reconciliation,'operations':operations,'row_envelope_sha256':originals,'scope':'Read-only WAL-aware logical SQLite snapshot under actual existing installation lease/registry. No source/native authority admission, Spark, recovery, mutations, checkpoint or ACK.'}
  db.rollback()
 owner.renew(context)
answer['duration_seconds']=time.monotonic()-t
(r/'result.json').write_text(json.dumps(answer,indent=2)+'\n')
print(json.dumps({'table_counts':answer['table_counts'],'states':[(o['operation'],o['state'])for o in answer['operations']],'duration_seconds':answer['duration_seconds']},indent=2))
