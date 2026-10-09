"""Independent medical bytes/reference/type oracle; no clinical/UMF/native claim."""
import csv,hashlib,io,json
from decimal import Decimal
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/medical'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def pointer(document,path):
    if not path.startswith('/'):raise ValueError('Original absolute JSON pointer required')
    value=document
    for token in path.split('/')[1:]:
        token=token.replace('~1','/').replace('~0','~')
        value=value[int(token)]if type(value)is list else value[token]
    return value

def original_oracle(root=ROOT):
    root=Path(root);custody=json.loads((root/'source-custody.json').read_bytes())
    for entry in custody['files']:
        raw=(root/entry['path']).read_bytes()
        if sha(raw)!=entry['sha256']or len(raw)!=entry['bytes']:raise ValueError('Original medical custody changed')
    raw_pack=(root/'upstream/pack.json').read_bytes();pack=json.loads(raw_pack);raw_model=(root/'upstream/ontology.json').read_bytes();model=json.loads(raw_model)
    if pack['version']!='1.1.0'or model['umf']!='0.8.0'or sha(raw_pack)!=custody['currentPackSha256']:raise ValueError('Exact current original profiles required')
    fields={e['id']:e for module in model['modules']for e in module['elements']if e['kind']=='field'}
    tables={};lexical=[]
    for binding in pack['source_bindings']:
        if binding['role']!='rows':continue
        table=binding['schema_id'];descriptor=pack['sources'][binding['source_id']];raw=(root/'upstream'/descriptor['reference']).read_bytes()
        if sha(raw)!=descriptor['checksum']['value']:raise ValueError('Original CSV source checksum differs')
        rows=list(csv.DictReader(io.StringIO(raw.decode('utf-8'))));tables[table]=rows
        if len(rows)!=pack['fixture_counts'][table]:raise ValueError('Original fixed row count differs')
        for index,row in enumerate(rows):
            for column,token in row.items():
                if token is None:raise ValueError('Malformed source CSV inventory')
                field=fields[table+'.'+column]
                lexical.append({'table':table,'row':index+1,'column':column,'field':field['id'],'originalToken':token,'originalScalarType':field['scalarType'],'nativeNull':token==pack['csv_conventions']['null_value']})
    resources={};resource_receipts=[]
    for row in tables['resources']:
        raw=(root/'upstream'/row['source_file']).read_bytes()
        if row['resource_key']in resources or sha(raw)!=row['source_sha256']or row['resource_json'].encode()!=raw:raise ValueError('Original complete resource JSON bytes changed/duplicated')
        document=json.loads(raw,parse_float=Decimal)
        if document['resourceType']+'/'+document['id']!=row['resource_key']:raise ValueError('Original native resource identity differs')
        resources[row['resource_key']]=document
        resource_receipts.append({'resourceKey':row['resource_key'],'sourceFile':row['source_file'],'sourceSha256':sha(raw),'originalResourceJson':row['resource_json'],'exactBytes':True})
    references=[]
    for row in tables['resource_references']:
        source=resources[row['source_resource_key']]
        if pointer(source,row['json_pointer'])!=row['native_reference']:raise ValueError('Original native reference/pointer differs')
        if row['resolution']!='local'or row['target_resource_key']!=row['native_reference']or row['target_resource_key']not in resources:raise ValueError('Unqualified or invented original local reference')
        references.append(dict(row))
    boolean=[]
    for table in ['patients','practitioners']:
        for row in tables[table]:
            raw=resources[row['resource_key']];token=row['active'];present='active'in raw
            if present:
                if type(raw['active'])is not bool or token not in ['true','false']or (token=='true')is not raw['active']:raise ValueError('Original boolean carrier differs')
            elif token!=pack['csv_conventions']['null_value']:raise ValueError('Absent original active projection differs')
            boolean.append({'resourceKey':row['resource_key'],'originalResourceState':'present'if present else'absent','originalNativeValue':raw.get('active'),'csvToken':token,'declaredType':fields[table+'.active']['scalarType']})
    string_fields=[x for x in lexical if x['field']in ['patients.birth_date','observations.effective_start','observations.effective_end','observations.value_decimal']]
    if any(x['originalScalarType']!='string'for x in string_fields):raise ValueError('Original medical string declarations changed')
    if len(resources)!=17 or len(references)!=17 or sum(map(len,tables.values()))!=51:raise ValueError('Original finite current medical inventory differs')
    return {'format':'ashlar-medical-current-source-oracle/0.1','currentPackSha256':sha(raw_pack),'modelSha256':sha(raw_model),'tables':tables,'lexicalCells':lexical,'resourceBytes':resource_receipts,'references':references,'booleanCarriers':boolean,'authoredStringFields':string_fields,'qualification':'Direct original current1.1 CSV/resource bytes and native JSON pointers only. CSV null and original-resource absent states are retained separately. No clinical validation, FHIR conformance, terminology equivalence, public UMF admission, graph publication or query claim.'}
if __name__=='__main__':print(json.dumps(original_oracle(),ensure_ascii=False,indent=2))

def current_dataset_input(root=ROOT):
    root=Path(root);oracle=original_oracle(root);model=json.loads((root/'upstream/ontology.json').read_bytes());pack=json.loads((root/'upstream/pack.json').read_bytes())
    module=next(m for m in model['modules']if m['id']=='domain');elements={e['id']:e for e in module['elements']};records=[];ids={};relationships=[];coordinates=[]
    def literal(table,column,token):
        if token==pack['csv_conventions']['null_value']:return None
        family=elements[table+'.'+column]['scalarType']
        if family=='string':return {'string':token}
        if family=='boolean':
            if token not in ('true','false'):raise ValueError('Explicit current CSV boolean grammar refused')
            return {'boolean':token=='true'}
        if family=='integer':return {'integerToken':token}
        raise ValueError('Unqualified original CSV scalar carrier')
    for table,rows in oracle['tables'].items():
        record=elements[table];key=next(k for k in record['keys']if k.get('primary')is True)
        for position,row in enumerate(rows):
            identity=[row[ref['element'].removeprefix(table+'.')]for ref in key['fields']];instance=json.dumps([model['id'],'domain',table,identity],ensure_ascii=False,separators=(',',':'))
            if (table,tuple(identity))in ids:raise ValueError('Original duplicate instance coordinate')
            ids[table,tuple(identity)]=instance
            values=[{'field':ref,'state':'present','value':literal(table,ref['element'].removeprefix(table+'.'),row[ref['element'].removeprefix(table+'.')])}for ref in record['members']]
            records.append({'instanceId':instance,'identity':{'module':'domain','element':table},'values':values});coordinates.append({'instanceId':instance,'table':table,'row':position+1,'originalCells':row})
    for table,rows in oracle['tables'].items():
        schema=json.loads((root/'upstream/umf'/(table+'.json')).read_bytes());key=next(k for k in elements[table]['keys']if k.get('primary')is True)
        for row in rows:
            source=ids[table,tuple(row[ref['element'].removeprefix(table+'.')]for ref in key['fields'])]
            for fk in schema.get('relationships',{}).get('foreign_keys',[]):
                token=row[fk['column']]
                if token==pack['csv_conventions']['null_value']:continue
                relationship=next(r for r in module['relationships']if r['id']==table+'.'+fk['column'])
                if relationship['source']!=[{'module':'domain','element':table}]or relationship['target']!=[{'module':'domain','element':fk['references_table'],'key':'identity'}]:raise ValueError('Original tabular FK/UMF relationship correspondence differs')
                target_record=elements[fk['references_table']];target_key=next(k for k in target_record['keys']if k['id']=='identity')
                if target_key['fields']!=[{'module':'domain','element':fk['references_table']+'.'+fk['references_column']}]:raise ValueError('Unsupported original FK target key')
                target=ids[fk['references_table'],(token,)];instance=json.dumps([model['id'],'domain',relationship['id'],source,target],ensure_ascii=False,separators=(',',':'))
                relationships.append({'instanceId':instance,'identity':{'module':'domain','id':relationship['id']},'sourceInstanceId':source,'target':{'identity':{'module':'domain','element':fk['references_table'],'key':'identity'},'values':[literal(fk['references_table'],fk['references_column'],token)]}})
    if len(records)!=51 or len(relationships)!=62:raise ValueError('Original current medical finite dataset counts differ')
    return {'scope':{'id':'ashlar-original-current-medical-csv','closure':'supplied-dataset-only'},'context':{'packVersion':'1.1.0','modelSha256':oracle['modelSha256'],'sourceProfile':'ashlar-medical-current-csv/0.1','csvConventions':pack['csv_conventions'],'qualification':'Original UTF8 CSV headers/cells. Canonical true/false boolean tokens map to explicit public boolean literal; native null marker maps present null, retaining original resource absence separately. Original STRING dates/decimal/JSON remain exact strings.'},'records':records,'relationships':relationships},coordinates
