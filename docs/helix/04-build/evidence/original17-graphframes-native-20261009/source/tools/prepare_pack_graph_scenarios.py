"""Named finite vertex projection for the seventeen original relational scenarios.

Genuine public UMF admission precedes representation. Full canonical raw carriers
and original tokens remain authoritative. Signed64 analytic columns are an
explicit finite engine capability, never a narrowing of original UMF types.
Authored equality joins use native property equality; edge traversals require a
separate multiplicity witness and cannot silently replace relational bags.
"""
import base64,hashlib,importlib,json,re,subprocess,tempfile
from pathlib import Path
from prepare_pack_query_cases import prepare as original_cases
from run_pack_release_graphframes import original
from run_graph_release_graphframes import columns,load_release
from private_graph_custody import PROFILE
PROFILE_SCENARIO='ashlar-original-pack-relational-vertex-profile/0.1'
INTEGER_FIELDS={'archaeology':{('interpretations','earliest'),('interpretations','latest')},'ecology':{('occurrences','count')}}


def signed64_token(token,field):
    if field.get('scalarType')!='integer' or type(token)is not str or not re.fullmatch(r'-?(0|[1-9][0-9]*)',token):
        raise ValueError('Named finite Integer token representation required')
    value=int(token)
    if not -(2**63)<=value<2**63:raise ValueError('Named signed64 engine capacity exceeded; original Integer unchanged')
    return value


def project(pack,value,graph,nodes,admission):
    """Representation only, called after independently recomputed public admission."""
    candidate=original_cases(pack);model=json.loads(base64.b64decode(admission['original_model_base64']))
    if hashlib.sha256(base64.b64decode(admission['original_graph_base64'])).hexdigest()!=candidate['original_inputs'][2]['sha256']:
        raise ValueError('Exact original scenario graph required')
    if graph!=json.loads(base64.b64decode(admission['original_graph_base64'])) or hashlib.sha256(base64.b64decode(admission['original_model_base64'])).hexdigest()!=candidate['original_inputs'][1]['sha256']:
        raise ValueError('Original graph/model bytes and selected objects must agree')
    fields=[];by_record={}
    for table in candidate['tables']:
        record=table['identity'][2];entries=[]
        for f in table['fields']:
            definition=f['original_field'];name=definition['name'];identity=f['identity'];property_id=f['development_property_id']
            if not re.fullmatch(r'[1-9][0-9]*',property_id):raise ValueError('Explicit canonical development Field ID required')
            entry={'identity':identity,'record_identity':table['identity'],'original_field':definition,
                   'development_property_id':property_id,'lexical_column':'field_'+property_id,
                   'presence_column':'presence_'+property_id,'signed64_column':'integer_'+property_id if (record,name)in INTEGER_FIELDS[pack]else None}
            fields.append(entry);entries.append(entry)
        by_record[record]=entries
    if len({f['development_property_id']for f in fields})!=len(fields):raise ValueError('Injective original Field column binding required')
    objects={o['key']:o for o in graph['objects']};rows=[];integer_guards=[]
    for raw in value['nodes']:
        source=objects[nodes[raw['graph_id']]];record=source['type']['element']
        if source['type']!={'document':model['id'],'module':'domain','element':record}or record not in by_record:raise ValueError('Closed original Record identity required')
        row=dict(raw);row['original_key']=source['key'];row['original_type']=json.dumps(source['type'],sort_keys=True,separators=(',',':'))
        member_ids={f['identity'][2]for f in by_record[record]}
        if set(source['values'])!=member_ids:raise ValueError('Original finite profile requires every original member presence; no missing/null equivalence')
        for f in fields:
            selected=f['record_identity'][2]==record
            token=source['values'][f['identity'][2]]if selected else None
            if token is not None and type(token)is not str:raise ValueError('Original lexical String/null representation required')
            row[f['lexical_column']]=token
            row[f['presence_column']]='nonmember'if not selected else'present-null'if token is None else'present'
            if f['signed64_column']:
                row[f['signed64_column']]=None if token is None else signed64_token(token,f['original_field'])
                if selected:integer_guards.append({'original_key':source['key'],'field_identity':f['identity'],'raw_token':token,'native_signed64':row[f['signed64_column']]})
        rows.append(row)
    return {'format':PROFILE_SCENARIO,'pack':pack,'source_admission':admission,'fields':fields,'nodes':rows,'edges':value['edges'],
            'integer_capacity_guards':integer_guards,'cases':candidate['cases'],'original_inputs':candidate['original_inputs'],
            'query_semantics':'Original authored property-equality relational bags; optional unmatched state distinct from source presence. Independent edges retained, never silently substituted or deduplicated.',
            'engine_executed':False,'qualification':__doc__}


def prepare(candidate,umf):
    required={'pack','release','release_sha256','custody','custody_sha256','publication'}
    if type(candidate)is not dict or set(candidate)!=required:raise ValueError('Closed original paired candidate required')
    pack=candidate['pack']
    if pack not in INTEGER_FIELDS:raise ValueError('Explicit archaeology/ecology scenario pack required')
    publication=Path(candidate['publication']);module=importlib.import_module('run_'+pack+'_outbox_publication')
    receipt=(publication/'public-dataset.json').read_bytes()
    with tempfile.TemporaryDirectory(prefix='ashlar-original-scenario-public-')as temporary:
        output=Path(temporary)/'public.json'
        process=subprocess.run(['bun',str(Path(__file__).resolve().parent/('check_'+pack+'_dataset_compact.ts')),str(umf),str(output)],capture_output=True,text=True,timeout=60)
        if process.returncode or not output.exists()or output.read_bytes()!=receipt:raise PermissionError('Fresh complete original public receipt recomputation required')
    value=load_release(Path(candidate['release']).read_bytes(),candidate['release_sha256'],custody_profile=PROFILE,custody_payload=Path(candidate['custody']).read_bytes(),trusted_custody_sha256=candidate['custody_sha256'])
    graph,nodes,edges,admission=original(pack,value,publication)
    if admission['umf_revision']!=module.UMF_PIN:raise PermissionError('Original public semantic producer pin required')
    result=project(pack,value,graph,nodes,admission)
    result['publication']=value['publication'];result['snapshots']=value['snapshots'];result['roles']=value['roles']
    result['original_release_sha256']=candidate['release_sha256'];result['original_custody_sha256']=candidate['custody_sha256']
    result['recomputed_public_receipt_sha256']=hashlib.sha256(receipt).hexdigest()
    return result

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--candidate',type=Path,required=True);p.add_argument('--umf',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    if a.output.exists():raise ValueError('Fresh profile artifact required')
    value=prepare(json.loads(a.candidate.read_bytes()),a.umf)
    a.output.write_text(json.dumps(value,sort_keys=True,ensure_ascii=False,indent=2)+'\n')
