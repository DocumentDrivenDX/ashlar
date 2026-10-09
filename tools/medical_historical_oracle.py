"""Independent original archive CSV/key/FK oracle; never converts Boolean tokens."""
import csv,hashlib,json
from medical_source_oracle import ROOT,original_oracle

def historical_oracle():
    original_oracle() # Recheck every retained current/archive original byte.
    custody=json.loads((ROOT/'source-custody.json').read_bytes())
    archive=ROOT/'historical/archive';model_bytes=(archive/'schemas/ontology.json').read_bytes();model=json.loads(model_bytes)
    pack_bytes=(ROOT/'historical/original-pack.json').read_bytes();pack=json.loads(pack_bytes)
    if hashlib.sha256(pack_bytes).hexdigest()!=custody['historicalPackSha256'] or pack['version']!='1.0.0':raise ValueError('Original historical pack differs')
    if hashlib.sha256((ROOT/'historical/original-1.0.0.zip').read_bytes()).hexdigest()!=custody['historicalArchiveSha256']:raise ValueError('Original historical archive differs')
    graph_bytes=(ROOT/'upstream/graph/fixture.json').read_bytes();graph=json.loads(graph_bytes)
    if graph['pack']['sha256']!=custody['historicalPackSha256'] or graph['pack']['version']!='1.0.0' or graph['schema']!={'id':model['id'],'revision':pack['version'],'sha256':hashlib.sha256(model_bytes).hexdigest()}:raise ValueError('Historical graph pairing differs')
    manifest=json.loads((archive/'manifest.json').read_bytes());keys={};tables={};coordinates=[];expected_objects={};expected_edges=[]
    compact=lambda value:json.dumps(value,separators=(',',':'),ensure_ascii=False)
    for table,entry in manifest['tables'].items():
        schema=json.loads((archive/'schemas'/f'{table}.json').read_bytes())
        with (archive/entry['file']).open(newline='',encoding='utf-8')as handle:rows=list(csv.DictReader(handle))
        if len(rows)!=entry['rows']:raise ValueError('Original archive count differs')
        tables[table]={'schema':schema,'rows':rows,'file':entry['file']}
        for ordinal,row in enumerate(rows,1):
            key=compact([model['id'],'domain',table,[row[name]for name in schema['primary_key']]])
            if key in expected_objects:raise ValueError('Duplicate archive key')
            values={table+'.'+column['name']:None if row[column['name']]==pack['csv_conventions']['null_value']else row[column['name']]for column in schema['columns']}
            expected_objects[key]={'key':key,'type':{'document':model['id'],'module':'domain','element':table},'values':values}
            coordinates.append({'objectKey':key,'table':table,'row':ordinal,'archiveFile':entry['file'],'archiveFileSha256':hashlib.sha256((archive/entry['file']).read_bytes()).hexdigest(),'cells':dict(row)})
            for column in schema['primary_key']:keys[(table,column,row[column])]=key
    for table,data in tables.items():
        schema=data['schema']
        for row in data['rows']:
            source=compact([model['id'],'domain',table,[row[name]for name in schema['primary_key']]])
            for fk in schema.get('relationships',{}).get('foreign_keys',[]):
                token=row[fk['column']]
                if token==pack['csv_conventions']['null_value']:continue
                target=keys[(fk['references_table'],fk['references_column'],token)];identity=table+'.'+fk['column']
                expected_edges.append({'key':compact([model['id'],'domain',identity,source,target]),'relationship':{'document':model['id'],'module':'domain','id':identity},'source':source,'target':target})
    actual_objects={o['key']:o for o in graph['objects']}
    if len(actual_objects)!=len(graph['objects'])or actual_objects!=expected_objects:raise ValueError('Graph full original values/key correspondence differs')
    if sorted(map(compact,expected_edges))!=sorted(map(compact,graph['edges'])):raise ValueError('Graph original FK occurrence bag differs')
    return {'profile':'ashlar-medical-historical-raw-oracle/0.1','packVersion':pack['version'],'packSha256':custody['historicalPackSha256'],'archiveSha256':custody['historicalArchiveSha256'],'modelSha256':hashlib.sha256(model_bytes).hexdigest(),'graphSha256':hashlib.sha256(graph_bytes).hexdigest(),'schemaRevisionQualification':'Original graph producer uses owning pack version as graph schema revision; original core document has no authored revision and remains unchanged.','coordinates':coordinates,'objects':graph['objects'],'edges':graph['edges'],'qualification':'Original own historical1.0 archive CSV literals, complete primary key/FK occurrence correspondence. No typed Boolean conversion, canonical UMF/native IDs, publication, clinical or FHIR equivalence.'}

if __name__=='__main__':print(json.dumps(historical_oracle(),ensure_ascii=False)+'\n')
