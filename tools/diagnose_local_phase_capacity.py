"""Read-only retained local apply artifact → complete phase/framing byte budgets.

No publication, native snapshot admission, ACK or recovery is performed.
"""
import argparse,hashlib,json,sqlite3
from pathlib import Path
from local_delta_custody import child_operation,encoded

def diagnose(journal):
    journal=Path(journal).resolve();connection=sqlite3.connect('file:'+str(journal)+'?mode=ro',uri=True)
    try:
        rows=connection.execute('SELECT request,artifact FROM local_publication_artifact').fetchall()
        if len(rows)!=1:raise ValueError('One retained original apply artifact required')
        request_text,artifact=rows[0];request=json.loads(request_text);manifest=json.loads(artifact)['manifest'];descriptor=json.dumps(manifest,sort_keys=True,separators=(',',':'),ensure_ascii=False)
        originals=[json.loads(raw)for(raw,)in connection.execute('SELECT intent FROM local_operation')];attempts=[value for value in originals if value['table'].endswith('.attempts')]
        if not attempts:raise ValueError('Original phase statement custody required')
        phases=[json.loads(value['parameters']['payload'])['phase']for value in attempts]
        template=attempts[0];reports=[]
        for phase,proposed_descriptor in [('applied',None),('committing',None),('committed',descriptor)]:
            text=json.dumps({'request':request,'result_json':artifact,'descriptor_json':proposed_descriptor},sort_keys=True,separators=(',',':'))
            row={'stream':request['stream'],'batch_id':request['batch_id'],'phase':phase,'request_digest':request['request_digest'],'payload_json':text,'payload_digest':hashlib.sha256(text.encode()).hexdigest()}
            parameters={'payload':json.dumps(row,separators=(',',':'))}
            intent={**template,'operation':child_operation('attempt:'+request['request_digest'],template['statement'],parameters),'parameters':parameters};raw=encoded(intent)
            reports.append({'phase':phase,'phasePayloadBytes':len(text.encode()),'rowParameterBytes':len(parameters['payload'].encode()),'operationIntentBytes':len(raw.encode()),'operationIntentSha256':hashlib.sha256(raw.encode()).hexdigest(),'withinUnchangedCorePhase4MiB':len(text.encode())<=4194304,'withinProposedOuter8MiB':len(raw.encode())<=8388608})
        return {'format':'ashlar-local-phase-capacity-diagnosis/0.1','retainedPhases':phases,'nativeOperationCounts':{role:sum(v['table'].endswith('.'+role)for v in originals)for role in ('attempts','object_current','edge_current','whole_source_history','manifest')},'originalArtifactBytes':len(artifact.encode()),'originalArtifactSha256':hashlib.sha256(artifact.encode()).hexdigest(),'originalRequestBytes':len(request_text.encode()),'originalRequestSha256':hashlib.sha256(request_text.encode()).hexdigest(),'phaseProjections':reports,'qualification':'Read-only retained originalSQLite request/apply artifact and statement. Reconstructs exact unsubmitted phase framing; actual failed installation remains provisional nativeeffects, not committed publication/ACK/nativeclosure. No originalmutation or recovery performed.'}
    finally:connection.close()
if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--journal',required=True,type=Path);parser.add_argument('--output',required=True,type=Path);args=parser.parse_args()
    if args.output.exists():raise ValueError('Fresh diagnostic output required')
    result=diagnose(args.journal);args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
