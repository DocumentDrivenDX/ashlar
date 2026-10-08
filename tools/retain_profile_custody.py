"""Retain original candidate profile definitions in isolated PostgreSQL custody.

The source bundle is deliberately scoped, not complete implementation recognition.
No executable profile is registered/admitted and the Truss head does not change.
"""
import argparse,base64,hashlib,json,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from ashlar.truss_input import artifact,exact_artifact
from ashlar.schema import _json
from ashlar.profile_custody import verify_profile_custody
CONTAINER='ashlar-e2e-truss-pg17'
PROJECTION="identity,version,encode(definition_bytes,'hex') AS definition_hex,encode(definition_sha256,'hex') AS definition_sha256,encode(source_bundle_bytes,'hex') AS bundle_hex,encode(source_bundle_sha256,'hex') AS bundle_sha256,original_database_role,original_session_role,original_xid::text AS original_xid,original_at::text AS original_at"

def text_sql(value):
    if type(value) is not str or not value or '\x00' in value:raise ValueError('Exact native identity required')
    return "convert_from(decode('"+value.encode('utf-8').hex()+"','hex'),'UTF8')"


def bounded_file(path):
    with Path(path).open('rb') as source:raw=source.read(1048577)
    if len(raw)>1048576:raise ValueError('Original file resource bound exceeded')
    return raw


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['profile-archives','producer-source','output']:p.add_argument('--'+name,required=True)
    p.add_argument('--docker',default='docker');a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
    raw=bounded_file(a.profile_archives);profiles=_json(raw)
    if type(profiles) is not dict or not 1<=len(profiles)<=32:raise ValueError('Bounded original profile set required')
    producer=bounded_file(a.producer_source)
    if hashlib.sha256(producer).hexdigest()!=json.loads(exact_artifact(profiles['acceptance']))['body_sha256']:
        raise ValueError('Original producer source does not match archive')
    bundle=json.dumps({'format':'ashlar-profile-source-bundle/0.1','qualification':'Selected original archive and producer artifacts only; not complete implementation/dependency recognition or profile admission',
        'archives':artifact('original-profile-archives',raw),'producer':artifact('original-producer-source',producer)},separators=(',',':')).encode()
    (out/'source-bundle.json').write_bytes(bundle)
    label=subprocess.run([a.docker,'inspect',CONTAINER,'--format','{{index .Config.Labels "ashlar.purpose"}}'],capture_output=True,text=True,check=True,timeout=10)
    if label.stdout.strip()!='end-to-end-development':raise RuntimeError('Refusing unrelated container')
    statements=["BEGIN; SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='10s'; SET LOCAL TimeZone='UTC'; SET LOCAL DateStyle='ISO,YMD'; SET LOCAL ROLE ashlar_profile_custody_writer;"]
    expected=[];charged=0
    for key,entry in sorted(profiles.items()):
        definition=exact_artifact(entry);identity=entry['identity'];version='0.1'
        # Version is explicit in the selected original candidate descriptor protocol.
        charged+=len(definition)*2+len(bundle)*2+4096
        if charged>8388608:raise ValueError('Complete native submission bound exceeded; nothing submitted')
        expected.append((identity,version,definition))
        call='ashlar_profile_custody.retain('+','.join([text_sql(identity),text_sql(version),"decode('"+definition.hex()+"','hex')","decode('"+bundle.hex()+"','hex')"])+')'
        statements.append('SELECT row_to_json(r) FROM (SELECT '+PROJECTION+' FROM '+call+') r;')
    statements+=['RESET ROLE; COMMIT;',"SELECT json_build_object('phase','committed','truss_head',(SELECT rev FROM truss.schema_head WHERE id=1),'profiles',(SELECT count(*) FROM ashlar_profile_custody.original));"]
    statement='\n'.join(statements)+'\n';(out/'retain.sql').write_text(statement)
    base=[a.docker,'exec','-i',CONTAINER,'psql','-X','-qAt','-U','postgres','-d','truss_e2e','-v','ON_ERROR_STOP=1']
    result=subprocess.run(base,input=statement,capture_output=True,text=True,timeout=30)
    (out/'native-output.txt').write_text(result.stdout);(out/'native-errors.txt').write_text(result.stderr)
    if result.returncode:raise RuntimeError('Profile custody refused; inspect original terminal result and native state')
    rows=[json.loads(line) for line in result.stdout.splitlines() if line]
    if len(rows)!=len(expected)+1 or rows[-1]['phase']!='committed' or rows[-1]['truss_head']!=0:raise RuntimeError('Unexpected original custody settlement/head')
    for row,(identity,version,definition) in zip(rows,expected):
        receipt=verify_profile_custody(row,identity=identity,version=version,definition=definition,source_bundle=bundle)
        if receipt.original_database_role!='ashlar_profile_custody_writer' or receipt.original_session_role!='postgres':raise RuntimeError('Unexpected original native role capture')
    (out/'summary.json').write_text(json.dumps({'state':'committed_original_byte_custody','profiles':len(expected),'receipts':rows[:-1],'head':0,'qualification':'Immutable original profile definition/source bytes and native original context only. Not profile registration/admission, complete implementation recognition, Truss readiness or accepted revision.'},indent=2)+'\n')
    print(str(len(expected))+' original profiles retained; Truss accepted head remains 0')

if __name__=='__main__':main()
