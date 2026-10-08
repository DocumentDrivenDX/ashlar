"""Offline verification of retained native configured-source publication receipts.

No warehouse queries or authority renewal. This proves recorded complete parity,
not present-day publication readability or production source authorization.
"""
import argparse,hashlib,json,sqlite3,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from jsonl_source_configuration import load_jsonl_configuration
from native_csv_configuration import installation_namespace
from fixture_oracle import fixture_columns,fixture_inventory
from ashlar.source import jsonl_batches
from ashlar.whole_entity import changes_from_batch
from ashlar.publication import Descriptor
from ashlar.source_checkpoint import bind_source_descriptor
from ashlar.attempt_store import _request_digest
from databricks_transport import sql_result


def verify(config_path,installation_path,intake_path,journal_path,receipts_path):
    config=load_jsonl_configuration(config_path)
    installation=json.loads(Path(installation_path).read_bytes());proof=json.loads(Path(intake_path).read_bytes())
    namespace=installation_namespace(installation,ROOT)
    if proof['source_sha256']!=config.intake.source_sha256 or proof['artifact_sha256']!=config.intake.artifact_sha256:
        raise ValueError('Original configured native intake differs')
    out=Path(receipts_path);summary=json.loads((out/'summary.json').read_bytes())
    if summary['state']!='published' or summary['source_kind']!='configured-jsonl' or summary['query_only'] or summary['namespace']!=namespace:
        raise ValueError('Completed original configured publication summary required')
    raw_source=config.source.read_bytes();source_sha=hashlib.sha256(raw_source).hexdigest()
    if summary['source_sha256']!=source_sha:raise ValueError('Original source digest differs')
    batches=tuple(jsonl_batches(raw_source.splitlines(keepends=True),feed=config.feed,epoch=config.epoch))
    through=summary['local_completed_batches']
    if type(through) is not int or not 1<=through<=len(batches) or len(summary['published'])!=through:
        raise ValueError('Complete bounded publication inventory required')
    for batch in batches:
        for change in changes_from_batch(batch):config.policy(change)
    db=sqlite3.connect(Path(journal_path).resolve().as_uri()+'?mode=ro',uri=True)
    try:
        workload={'namespace':namespace,'source_sha256':source_sha,'uuids':{name:entry['uuid'] for name,entry in installation['tables'].items()},
            'intake_source_sha256':config.intake.source_sha256,'intake_table_uuid':proof['table_uuid'],
            'source_configuration_sha256':hashlib.sha256(config.original).hexdigest()}
        encode=lambda v:json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False)
        if db.execute('SELECT workload FROM stream_scope WHERE id=1').fetchone()!=(hashlib.sha256(encode(workload).encode()).hexdigest(),):
            raise ValueError('Original configuration/model/installation journal scope differs')
        stream='native-configured-jsonl-stream:'+namespace
        progress=db.execute('SELECT position,original_json,digest FROM jsonl_consumer_progress WHERE stream=? AND feed=? AND epoch=? ORDER BY position',(stream,config.feed,config.epoch)).fetchall()
        if len(progress)!=through or [r[0] for r in progress]!=[int(b.cursor_after) for b in batches[:through]] or summary['local_consumer_position']!=batches[through-1].cursor_after:
            raise ValueError('Complete original byte progress differs')
        reads=[json.loads(line) for line in (out/'statements.jsonl').read_text().splitlines()]
        checks=[];sha=lambda s:hashlib.sha256(s.encode()).hexdigest()
        sorted_rows=lambda rows:sorted(rows,key=encode)
        for ordinal,(position,text,digest) in enumerate(progress,1):
            if sha(text)!=digest:raise ValueError('Original checkpoint digest differs')
            cp=json.loads(text);request=cp['request'];row=cp['descriptor'];batch=batches[ordinal-1]
            if _request_digest(request)!=request['request_digest'] or row!=summary['published'][ordinal-1]['descriptor']:
                raise ValueError('Original request/publication differs')
            value=Descriptor(row['publication_id'],row['profile_version'],json.loads(row['table_versions_json']),json.loads(row['schema_revisions_json']),json.loads(row['source_progress_json']),json.loads(row['validation_report_json']),row)
            if dict(value.revisions)!={config.schema_alias:config.intake.document_revision}:raise ValueError('Original selected schema alias/revision differs')
            bind_source_descriptor(request,value,expected_publication_id=row['publication_id'])
            original=batch.begin+b''.join(r.raw for r in batch.records)+batch.commit
            if original!=raw_source[int(batch.cursor_before):position]:raise ValueError('Original complete source boundary differs')
            expected,_=fixture_inventory(batches[:ordinal],fixture_columns(ROOT),materialized_at=summary['materialized_at'])
            matches=[(t,a) for r,t,a in db.execute('SELECT request_json,targets_json,artifact_text FROM snapshot_artifact') if json.loads(r)==request]
            if len(matches)!=1:raise ValueError('One original snapshot artifact required')
            targets=json.loads(matches[0][0]);artifact=json.loads(matches[0][1])
            if artifact['manifest']!=row or set(targets)!=set(value.versions):raise ValueError('Complete original manifest target vector required')
            for table,target in targets.items():
                role=table.rsplit('.',1)[1]
                if target['uuid']!=installation['tables'][table]['uuid'] or target['rows']!=expected[role] or target['version']!=value.versions[table]:
                    raise ValueError('Independent complete snapshot expectation differs')
                clause='FROM '+'.'.join('`'+part+'`' for part in table.split('.'))+' VERSION AS OF '+str(target['version'])+' LIMIT 1001'
                handles=[]
                for read in reads:
                    if clause in read['sql']:
                        result=sql_result(read['response'])
                        if result.columns!=tuple((name,'STRING') for name,_ in fixture_columns(ROOT)[role]) or sorted_rows(result.rows)!=sorted_rows(expected[role]):
                            raise ValueError('Recorded complete native inventory differs')
                        handles.append(read['statement_id'])
                if not handles:raise ValueError('Original complete native inventory receipt missing')
                checks.append({'ordinal':ordinal,'table':table,'version':target['version'],'rows':len(expected[role]),'original_handles':handles})
        mutations=[]
        for operation,request,digest,handle,response in db.execute('SELECT * FROM submission ORDER BY rowid'):
            if sha(request)!=digest:raise ValueError('Original native request digest differs')
            if json.loads(request)['body']['statement'].split()[0].upper() in ('SELECT','DESCRIBE','SHOW'):continue
            if not handle or not response or json.loads(response)['status']['state']!='SUCCEEDED':raise ValueError('Original native mutation not terminal successful')
            mutations.append({'operation':operation,'original_handle':handle,'request_sha256':digest,'response_sha256':sha(response),'state':'SUCCEEDED'})
        config.verify()
        final,_=fixture_inventory(batches[:through],fixture_columns(ROOT),materialized_at=summary['materialized_at'])
        return {'state':'passed-recorded-native-configured-publication','summary':summary,'inventory_checks':checks,'original_mutations':mutations,
            'checkpoints':[{'position':p,'original':json.loads(t),'digest':d} for p,t,d in progress],
            'final_independent_inventory':final,'configuration_sha256':sha(config.original.decode('utf-8')),
            'receipt_sha256':{str(p.relative_to(out)):hashlib.sha256(p.read_bytes()).hexdigest() for p in out.rglob('*') if p.is_file()},
            'qualification':'Offline recorded original configured JSONL publication verification: pinned configuration/model/installation journal scope, exact complete transaction progress, all published native version inventories and terminal original mutations. No fresh authority/readability renewal, accepted Truss IDs, remote fencing/ACK or complete toolkit claim.'}
    finally:db.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('source-config','installation','intake-proof','journal','receipts','output'):parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists():parser.error('Fresh evidence file required')
    value=verify(args.source_config,args.installation,args.intake_proof,args.journal,args.receipts)
    args.output.write_text(json.dumps(value,indent=2)+'\n')
    print('Verified '+str(len(value['checkpoints']))+' original publication checkpoints and '+str(len(value['inventory_checks']))+' complete native version inventories.')

if __name__=='__main__':main()
