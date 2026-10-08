"""Rollback-only initial catalog probe against the isolated Truss declaration.

This exercises native candidate effects, not acceptance or runtime readiness.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from ashlar.binding import plan_string_record_binding
from ashlar.catalog import Identity, plan_catalog_ids
from ashlar.lineage import lineage_bytes, TYPE
from ashlar.schema import SchemaIntake
from ashlar.semantic_policy import StringRecordPolicy
from ashlar.truss_input import artifact, verify_acceptance_input_custody

CONTAINER = 'ashlar-e2e-truss-pg17'


def text_sql(value):
    if not isinstance(value, str) or '\x00' in value:
        raise ValueError('Exact PostgreSQL UTF8 text required')
    return "pg_catalog.convert_from(pg_catalog.decode('" + value.encode('utf-8').hex() + "','hex'),'UTF8')"


def candidate_sql(intake, interpretation, request=None):
    plan = plan_string_record_binding(interpretation, trusted_validator_revision=intake.validator_revision)
    desired = [Identity('type', tuple(t['identity'])) for t in plan['types']]
    desired += [Identity('property', tuple(p['identity'])) for p in plan['properties']]
    ids = plan_catalog_ids([], desired, highwater={'type': 0, 'property': 0, 'relationship': 0}, expected_head='0')
    # Refuses unsupported assertions and source/interpretation drift before SQL.
    StringRecordPolicy.from_intake(intake, interpretation, ids.entries, source_system='truss-native-candidate')
    if request is None or request.document_sources!=(intake.source,):
        raise ValueError('Complete original acceptance-input custody required')
    origin={'probe':'rollback-only','acceptanceInputSha256':request.sha256,
        'acceptanceInputPreimageBase64':__import__('base64').b64encode(request.canonical_preimage).decode('ascii'),
        'originalInputBase64':__import__('base64').b64encode(request.original).decode('ascii')}
    mapping = {e.identity: e.catalog_id for e in ids.entries}
    sql = ["BEGIN; SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='10s';",
        """DO $guard$ DECLARE head integer; BEGIN
        SELECT rev INTO head FROM truss.schema_head WHERE id=1 FOR UPDATE;
        IF NOT FOUND OR head <> 0 THEN RAISE EXCEPTION 'Initial installed catalog required'; END IF;
        IF (SELECT count(*) FROM truss.schema_rev) <> 1
          OR EXISTS(SELECT FROM truss.schema_doc) OR EXISTS(SELECT FROM truss.schema_change)
          OR EXISTS(SELECT FROM truss.type_def) OR EXISTS(SELECT FROM truss.prop_def)
          OR EXISTS(SELECT FROM truss.key_def) OR EXISTS(SELECT FROM truss.rel_def)
          OR EXISTS(SELECT FROM truss.object) OR EXISTS(SELECT FROM truss.edge)
          OR EXISTS(SELECT FROM truss.journal) OR EXISTS(SELECT FROM truss.catalog_acceptance_report)
        THEN RAISE EXCEPTION 'Probe refuses nonempty catalog/graph/history'; END IF;
        END $guard$;
        CREATE TEMP TABLE ashlar_probe_high_water ON COMMIT DROP AS SELECT * FROM truss.catalog_global_high_water_v01();
        DO $water$ BEGIN
        IF (SELECT count(*) FROM pg_temp.ashlar_probe_high_water) <> 3
          OR (SELECT count(DISTINCT allocation_domain) FROM pg_temp.ashlar_probe_high_water) <> 3
          OR EXISTS(SELECT FROM pg_temp.ashlar_probe_high_water WHERE allocation_domain NOT IN ('type_id','prop_id','rel_type_id') OR maximum_id IS DISTINCT FROM '0')
        THEN RAISE EXCEPTION 'Unexpected initial allocation inventory'; END IF;
        END $water$;""",
        "INSERT INTO truss.schema_rev(rev,origin) VALUES(1,"+text_sql(json.dumps(origin,separators=(',',':')))+"::jsonb);",
        "INSERT INTO truss.schema_doc(rev,ord,doc_id,doc_revision,umf_version,content_sha256,document,validation) VALUES(1,0," +
        ','.join(text_sql(v) for v in [intake.document_id,intake.document_revision,'0.7.0',intake.source_sha256,intake.source.decode('utf-8')]) +
        ',' + text_sql(json.dumps(json.loads(intake.artifact)['validation'],ensure_ascii=False,separators=(',',':'))) + '::jsonb);']
    expected_types=[];expected_props=[]
    for t in plan['types']:
        doc,module,element=t['identity']; tid=mapping[Identity('type',tuple(t['identity']))]
        raw=lineage_bytes(TYPE,list(t['identity']))
        sql.append("INSERT INTO truss.type_def(document_id,type_id,module,element,kind,since_rev,doc_ord,lineage_profile,lineage_bytes,definition_source_kind,definition_rev,definition_doc_ord,definition_document_id) VALUES("+
            ','.join([text_sql(doc),"(SELECT maximum_id::bigint+"+str(tid)+" FROM pg_temp.ashlar_probe_high_water WHERE allocation_domain='type_id')",text_sql(module),text_sql(element),"'record'","1","0",text_sql(TYPE),"pg_catalog.decode('"+raw.hex()+"','hex')","'accepted_document'","1","0",text_sql(doc)])+');')
        expected_types.append({'document_id':doc,'type_id':tid,'module':module,'element':element,'kind':'record','since_rev':1,'doc_ord':0,'retired_rev':None,'provisional':False,'lineage_profile':TYPE,'lineage_hex':raw.hex(),'lineage_sha256':hashlib.sha256(raw).hexdigest(),'definition_source_kind':'accepted_document','definition_rev':1,'definition_doc_ord':0,'definition_document_id':doc,'binding_source_rev':None,'binding_source_pointer':None,'binding_source_bytes':None})
    for prop in plan['properties']:
        identity=Identity('property',tuple(prop['identity'])); pid=mapping[identity]; tid=mapping[Identity('type',identity.parts[:3])]
        element=identity.parts[4]; name=prop['displayName'] if prop['displayName'] is not None else element
        availability=prop['nullability']['nullability']
        sql.append("INSERT INTO truss.prop_def(prop_id,type_id,element,name,scalar_type,nullability,cardinality,home,since_rev,doc_ord,definition_source_kind,definition_rev,definition_doc_ord,definition_document_id) VALUES("+
            ','.join(["(SELECT maximum_id::bigint+"+str(pid)+" FROM pg_temp.ashlar_probe_high_water WHERE allocation_domain='prop_id')",str(tid),text_sql(element),text_sql(name),"'string'",text_sql(availability),"'one'","'json'","1","0","'accepted_document'","1","0",text_sql(identity.parts[0])])+');')
        expected_props.append({'prop_id':pid,'type_id':tid,'element':element,'name':name,'scalar_type':'string','nullability':availability,'cardinality':'one','home':'json','since_rev':1,'doc_ord':0,'retired_rev':None,'facets':None,'item':None,'definition_source_kind':'accepted_document','definition_rev':1,'definition_doc_ord':0,'definition_document_id':identity.parts[0],'binding_source_rev':None,'binding_source_pointer':None,'binding_source_bytes':None})
    sql.append("""SELECT json_build_object('phase','candidate','head',(SELECT rev FROM truss.schema_head WHERE id=1),
      'original_request',(SELECT origin FROM truss.schema_rev WHERE rev=1),
      'document',(SELECT json_build_object('doc_id',doc_id,'doc_revision',doc_revision,'umf_version',umf_version,'content_sha256',content_sha256,'document',document,'validation',validation) FROM truss.schema_doc WHERE rev=1),
      'types',(SELECT coalesce(json_agg(row_to_json(t) ORDER BY type_id),'[]'::json) FROM
        (SELECT document_id,type_id,module,element,kind,since_rev,doc_ord,retired_rev,provisional,lineage_profile,encode(lineage_bytes,'hex') lineage_hex,encode(lineage_sha256,'hex') lineage_sha256,definition_source_kind,definition_rev,definition_doc_ord,definition_document_id,binding_source_rev,binding_source_pointer,binding_source_bytes FROM truss.type_def) t),
      'properties',(SELECT coalesce(json_agg(row_to_json(p) ORDER BY prop_id),'[]'::json) FROM
        (SELECT prop_id,type_id,element,name,scalar_type,nullability,cardinality,home,since_rev,doc_ord,retired_rev,facets,item,definition_source_kind,definition_rev,definition_doc_ord,definition_document_id,binding_source_rev,binding_source_pointer,binding_source_bytes FROM truss.prop_def) p),
      'report_count',(SELECT count(*) FROM truss.catalog_acceptance_report));
      ROLLBACK;
      SELECT json_build_object('phase','after_rollback','head',(SELECT rev FROM truss.schema_head WHERE id=1),
        'revision_count',(SELECT count(*) FROM truss.schema_rev),'document_count',(SELECT count(*) FROM truss.schema_doc),
        'type_count',(SELECT count(*) FROM truss.type_def),'property_count',(SELECT count(*) FROM truss.prop_def),
        'report_count',(SELECT count(*) FROM truss.catalog_acceptance_report));""")
    expected={'phase':'candidate','head':0,'original_request':origin,'document':{'doc_id':intake.document_id,'doc_revision':intake.document_revision,'umf_version':'0.7.0','content_sha256':intake.source_sha256,'document':intake.source.decode('utf-8'),'validation':json.loads(intake.artifact)['validation']},'types':sorted(expected_types,key=lambda x:x['type_id']),'properties':sorted(expected_props,key=lambda x:x['prop_id']),'report_count':0}
    return '\n'.join(sql)+'\n',expected



def retain_candidate_input(intake, out):
    """Retain unregistered profile descriptors and complete compared request."""
    descriptors = {
        'layout': {'layout': 'truss-weft-review-0.11', 'original_sql_sha256': '1ac7cc82405ff581072d45ad586f54d8c48343eaecc7c011195535ed359e37f9'},
        'acceptance': {'body_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'mode': 'initial-catalog-rollback-only'},
        'validator': {'umf_git_revision': intake.validator_revision, 'inspection_sha256': hashlib.sha256((ROOT/'tools/inspect_umf.ts').read_bytes()).hexdigest()},
        'support': {'body_sha256': hashlib.sha256((ROOT/'src/ashlar/semantic_policy.py').read_bytes()).hexdigest(), 'scope': 'one-document local Record/singleton string Field required or absent-allowed; no keys/facets/relationships'},
        'policy': {'unknownEndpoint': 'reject', 'loss': 'strict'},
        'umf': {'core_version': '0.7.0', 'umf_git_revision': intake.validator_revision},
    }
    archives={};pins={};retained={}
    for name,descriptor in descriptors.items():
        raw=json.dumps({'status':'unregistered candidate; not acceptance authority', **descriptor},sort_keys=True,separators=(',',':')).encode()
        identity='ashlar-truss-catalog-probe:'+name
        pins[name]={'identity':identity,'version':'0.1','sha256':hashlib.sha256(raw).hexdigest()}
        archives[(identity,'0.1')]=raw;retained[name]=artifact(identity,raw)
    value={'interfaceVersion':'truss-acceptance-input/0.1.0',
        'layoutProfile':pins['layout'],'acceptanceProfile':pins['acceptance'],
        'validatorProfile':pins['validator'],'supportProfile':pins['support'],
        'documents':[{'documentId':intake.document_id,'documentRevision':intake.document_revision,
            'artifact':artifact('original-source:'+intake.source_sha256,intake.source),
            'umfProfile':pins['umf'],'ingress':{'kind':'native'}}],
        'binding':{'state':'absent'},'policy':{'unknownEndpoint':'reject','loss':'strict','profile':pins['policy']},'transforms':[]}
    raw=json.dumps(value,ensure_ascii=False,indent=2).encode()+b'\n'
    custody=verify_acceptance_input_custody(raw,profile_archives=archives,intakes=[intake],
        trusted_validator_revision=intake.validator_revision,document_order=(intake.document_id,))
    (out/'acceptance-input-candidate.json').write_bytes(raw)
    (out/'profile-archives.json').write_text(json.dumps(retained,indent=2)+'\n')
    (out/'acceptance-input-preimage.bin').write_bytes(custody.canonical_preimage)
    return custody


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['umf-source','validator-revision','document','revision','output']:
        p.add_argument('--'+name,required=True)
    p.add_argument('--bun',default='bun');p.add_argument('--docker',default='docker')
    a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
    artifacts=[]
    for tool,name in [('inspect_umf.ts','intake.json'),('inspect_schema_semantics.ts','interpretation.json')]:
        result=subprocess.run([a.bun,str(ROOT/'tools'/tool),a.umf_source,a.validator_revision,a.document],capture_output=True,timeout=60)
        if result.returncode:raise RuntimeError('Pinned UMF inspection refused: '+result.stderr.decode('utf-8','replace'))
        artifacts.append(result.stdout);(out/name).write_bytes(result.stdout)
    intake=SchemaIntake.read(artifacts[0],a.revision,trusted_validator_revision=a.validator_revision)
    custody=retain_candidate_input(intake,out)
    statement,expected=candidate_sql(intake,artifacts[1],custody)
    (out/'candidate.sql').write_text(statement)
    label=subprocess.run([a.docker,'inspect',CONTAINER,'--format','{{index .Config.Labels "ashlar.purpose"}}'],capture_output=True,text=True,check=True,timeout=10)
    if label.stdout.strip()!='end-to-end-development':raise RuntimeError('Refusing unrelated container')
    result=subprocess.run([a.docker,'exec','-i',CONTAINER,'psql','-X','-qAt','-U','postgres','-d','truss_e2e','-v','ON_ERROR_STOP=1'],input=statement,capture_output=True,text=True,timeout=30)
    (out/'native-output.txt').write_text(result.stdout);(out/'native-errors.txt').write_text(result.stderr)
    if result.returncode:raise RuntimeError('Native candidate transaction refused; connection closed, no COMMIT issued')
    rows=[json.loads(line) for line in result.stdout.splitlines() if line]
    after={'phase':'after_rollback','head':0,'revision_count':1,'document_count':0,'type_count':0,'property_count':0,'report_count':0}
    if rows!=[expected,after]:raise RuntimeError('Native candidate or rollback parity failed')
    summary={'state':'verified_rollback_only','source_sha256':intake.source_sha256,'validator_revision':a.validator_revision,'acceptance_input_sha256':custody.sha256,'input_profiles_registered':False,'sql_sha256':hashlib.sha256(statement.encode()).hexdigest(),'observed':rows,'qualification':'Actual selected UMF string-record candidate effects and exact native lineage/property/source readback under initial head lock, rolled back. No accepted revision/report, committed IDs, protected producer, ordinary-role closure, mutation/feed or full runtime readiness.'}
    (out/'summary.json').write_text(json.dumps(summary,indent=2,ensure_ascii=False)+'\n')
    print('Verified native candidate definitions and complete rollback; accepted head remains 0')

if __name__=='__main__':main()
