"""Original representative pack bindings/read ports; private local publication only.

Exact original UMF bytes and development property IDs remain distinct from
Truss accepted catalogs. The caller must hold the complete publication/ACK
interval before any compiler checks or user SQL. No query success is inferred
from constructing these ports.
"""
import hashlib,importlib,json,uuid
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from local_delta_custody import encoded,DeltaTarget
from fixture_oracle import fixture_columns
from protected_outbox_ack import AckScope
from run_local_outbox_publication import NativeDriver,PrivatePolicy,ROOT
from run_commerce_publication_weft import ReadOnlyTransport,PublicationProvider,LAYOUT_SHA
from run_commerce_arithmetic_weft import BACKEND

PACKS=('archaeology','ecology')
COMPILER_SOURCE='faa33133382cff1af3b54b1b924183dfb25e4d49'
WEFT_REVISION='86588a265f3cc473675cafe6f7b7f5dbde3e6a1f'
COMPILER_SHA='beab6ff8782dda2c212af501023e03056377c2f997ec9e160738d51b2e6d77bd'


def original_ports(pack):
    if pack not in PACKS:raise ValueError('Explicit supported original pack required')
    converter=importlib.import_module(pack+'_source_transaction')
    native=importlib.import_module('run_'+pack+'_outbox_publication')
    admission=getattr(native,pack.title()+'Admission')
    oracle=getattr(native,'original_'+pack+'_oracle')
    return converter,native,admission,oracle


def compiler_request(pack,sql,model,graph,bindings,manifest,registry,aliases):
    converter,native,_,_=original_ports(pack)
    batch,expected=converter.build_transaction(model,graph,source_system=native.SOURCE_SYSTEM)
    if bindings!=expected:raise ValueError('Exact original development catalog bindings required')
    versions=json.loads(manifest['table_versions_json']);rows={row['table']:row for row in registry}
    required={'object_current','edge_current','tombstone','whole_source_history','attempts','manifest'}
    if len(rows)!=6 or len(rows)!=len(registry) or {table.split('.')[-1] for table in rows}!=required or {table.split('.')[-1] for table in versions}!=required-{'attempts','manifest'} or not set(versions)<=set(rows):raise ValueError('Complete original six-role native publication required')
    if any(type(version)is not int or version<0 for version in versions.values()) or len({row['uuid'] for row in registry})!=6 or any(str(uuid.UUID(row['uuid']))!=row['uuid'] for row in registry):raise ValueError('Distinct canonical original UUIDs/nonnegative version pins required')
    if set(json.loads(manifest['source_progress_json']))!={native.SOURCE_SYSTEM} or set(aliases)!=set(versions) or len(set(aliases.values()))!=len(aliases):raise ValueError('Complete original source/vector/alias custody required')
    document=json.loads(model);pin={'documentId':document['id'],'revision':converter.SOURCE_SHA,'sha256':converter.SOURCE_SHA,'umfVersion':'0.8.0'}
    table_vector=[{'name':aliases[table].split('.'),'uuid':rows[table]['uuid'],'version':version} for table,version in sorted(versions.items())]
    object_table=next(table for table in versions if table.endswith('.object_current'));index=sorted(versions).index(object_table)
    elements={(module['id'],element['id']):element for module in document['modules'] for element in module['elements']}
    properties={tuple(item['identity']):item['property_id'] for item in bindings['properties']}
    records=[]
    for entry in bindings['types']:
        kind,module,element=entry['identity']
        if kind!='object':continue
        record=elements[(module,element)];homes=[]
        for member in record['members']:
            identity=(document['id'],member['module'],member['element'])
            homes.append({'logical':{'documentId':identity[0],'module':identity[1],'element':identity[2],'revision':converter.SOURCE_SHA},'home':{'kind':'props','propertyId':properties[identity]}})
        records.append({'kind':'object','logical':{'documentId':document['id'],'module':module,'element':element,'revision':converter.SOURCE_SHA},'sourceSystem':batch.feed,'typeId':entry['type_id'],'table':index,'schemaRevision':converter.SOURCE_SHA,'properties':homes})
    binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3','layoutSha256':LAYOUT_SHA,'modelPins':[pin],
        'publication':{'id':manifest['publication_id'],'manifestUuid':rows[next(table for table in rows if table.endswith('.manifest'))]['uuid'],'tables':table_vector},'records':records}
    raw=encoded(binding)
    return {'interfaceVersion':'weft-compile/0.3.0','dialect':'weft-sql/0.3.0','sql':sql,
        'modules':[{'documentJson':model.decode(),'pin':pin,'selectedModuleIds':sorted({record['logical']['module'] for record in records})}],
        'target':{**BACKEND,'bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()},'options':{'allowCandidate':True}}


@contextmanager
def open_pack_reader(spark,publication,pack):
    converter,native,admission_type,oracle=original_ports(pack)
    publication=Path(publication)
    original_report=json.loads((publication/'report.json').read_bytes())
    if original_report['format']!='ashlar-original-'+pack+'-publication/0.1':raise ValueError('Exact original pack publication report required')
    model=(publication/'original-ontology.json').read_bytes();graph=(publication/'original-graph.json').read_bytes();bindings=json.loads((publication/'development-bindings.json').read_bytes())
    batch,expected_bindings=converter.build_transaction(model,graph,source_system=native.SOURCE_SYSTEM)
    if bindings!=expected_bindings or batch.begin+b''.join(row.raw for row in batch.records)+batch.commit!=(publication/'source.jsonl').read_bytes():raise ValueError('Original source/binding bytes differ')
    admission=admission_type(batch,bindings,publication/'original-ontology.json',publication/'original-graph.json',publication/'development-bindings.json',publication/'public-dataset.json')
    expected=oracle(model,graph,bindings,batch,fixture_columns(ROOT))
    targets=[DeltaTarget(row['table'],Path(row['path']),row['uuid']) for row in original_report['table_registry']]
    def native_files():return {str(path):hashlib.sha256(path.read_bytes()).hexdigest() for target in targets for path in target.path.rglob('*') if path.is_file()}
    original_native=native_files();transport=None;provider=None
    try:
        context=object();policy=PrivatePolicy(context,targets);policy.initializing=False
        transport=ReadOnlyTransport.open(spark,publication/'operations.sqlite','private-original-'+pack,targets,policy)
        artifacts=transport.db.execute('SELECT request,artifact FROM local_publication_artifact').fetchall()
        if len(artifacts)!=1:raise ValueError('Exact original single publication artifact required')
        request_raw,artifact_raw=artifacts[0];request=json.loads(request_raw);manifest=json.loads(artifact_raw)['manifest']
        if manifest!=original_report['native_manifest']:raise ValueError('Original immutable artifact/report manifest differs')
        scope=AckScope(**original_report['protected_ack_scope']);source=original_report['source_schema'];role=scope.service_schema.replace('pipeline_','operator_')
        port={'scope':scope,'source':source,'reader':source+'_reader','role':role,'signature':original_report['source_signature_sha256']}
        tables={target.table.split('.')[-1]:target.table for target in targets}
        driver=NativeDriver(transport,policy,context,tables,{batch.feed:port},admission.changes,fixture_columns(ROOT),source_admission=admission)
        plans=transport.db.execute('SELECT original FROM local_source_plan WHERE request_digest=?',(request['request_digest'],)).fetchall()
        if len(plans)!=1:raise ValueError('Exact original immutable source plan required')
        plan=json.loads(plans[0][0]);policy.active={'request':request,'steps':plan['selected_steps'],'expected':expected,'manifest':manifest,'publication_id':manifest['publication_id'],'previous_progress':{},'progress':json.loads(manifest['source_progress_json']),'previous_expected':plan['complete_prior_oracle'],'generated_steps':plan['generated_steps'],'elisions':plan['zero_match_elisions']}
        aliases={table:table.replace('local.','spark_catalog.',1) for table in json.loads(manifest['table_versions_json'])}
        first=next(entry['identity'][2] for entry in bindings['types'] if entry['identity'][0]=='object')
        request_template=compiler_request(pack,'SELECT * FROM '+first,model,graph,bindings,manifest,original_report['table_registry'],aliases)
        provider=PublicationProvider(driver,aliases,port,request_raw.encode(),encoded(manifest).encode(),json.loads(request_template['target']['bindingJson']),context=context)
        yield SimpleNamespace(provider=provider,driver=driver,admission=admission,independent_expected=expected,context=context,original_native_files=original_native,native_files=native_files,aliases=aliases,original_report=original_report,model=model,graph=graph,bindings=bindings,manifest=manifest,request_bytes=request_raw.encode(),manifest_bytes=encoded(manifest).encode())
        if native_files()!=original_native:raise ValueError('Read-only consumer changed original native bytes')
    finally:
        if provider is not None:provider._reader_closed=True
        if transport is not None:transport.close()


def compile_cases(executable,publications,output):
    """Retain every actual compiler result; this is not native query acceptance."""
    import subprocess
    from prepare_pack_query_cases import prepare
    executable=Path(executable);output=Path(output)
    if hashlib.sha256(executable.read_bytes()).hexdigest()!=COMPILER_SHA:raise ValueError('Exact reviewed compiler executable required')
    if set(publications)!=set(PACKS):raise ValueError('Both original representative publications required')
    output.mkdir(parents=True,exist_ok=False)
    records=[]
    for pack in PACKS:
        directory=Path(publications[pack]);report=json.loads((directory/'report.json').read_bytes())
        model=(directory/'original-ontology.json').read_bytes();graph=(directory/'original-graph.json').read_bytes();bindings=json.loads((directory/'development-bindings.json').read_bytes())
        manifest=report['native_manifest'];aliases={table:table.replace('local.','spark_catalog.',1) for table in json.loads(manifest['table_versions_json'])}
        for case in prepare(pack)['cases']:
            original=case['original_scenario'];request=compiler_request(pack,original['sql'],model,graph,bindings,manifest,report['table_registry'],aliases)
            prefix=pack+'-'+original['id'];request_path=output/(prefix+'-request.json');request_path.write_text(encoded(request)+'\n')
            process=subprocess.run([str(executable)],input=encoded(request),capture_output=True,text=True,timeout=30)
            (output/(prefix+'-stdout.json')).write_text(process.stdout);(output/(prefix+'-stderr.txt')).write_text(process.stderr)
            try:artifact=json.loads(process.stdout)
            except json.JSONDecodeError:artifact=None
            compiled=process.returncode==0 and isinstance(artifact,dict) and artifact.get('status')=='compiled'
            if compiled and (artifact.get('bindingSha256')!=request['target']['bindingSha256'] or artifact.get('modelPins')!=[request['modules'][0]['pin']]):raise ValueError('Original compiler request/model custody differs')
            records.append({'pack':pack,'case':original['id'],'original_scenario':original,'original_graph_expected':case['original_graph_expected'],'exit':process.returncode,'compiled':compiled,'request_path':str(request_path),'artifact_path':str(output/(prefix+'-stdout.json')),'request_sha256':hashlib.sha256(request_path.read_bytes()).hexdigest(),'artifact_sha256':hashlib.sha256(process.stdout.encode()).hexdigest(),'original_status':artifact.get('status') if isinstance(artifact,dict) else None})
    result={'format':'ashlar-original-pack-compiler-trial/0.1','weft_revision':WEFT_REVISION,'compiler_build_source':COMPILER_SOURCE,'compiler_path':str(executable),'compiler_sha256':COMPILER_SHA,'cases':records,'qualification':'All17 authored SQL cases retained unchanged. Actual compilation only against candidate bindings naming existing local publication pins; no publication resolver/ACK interval opened, no native checks/userSQL/results or native-query pass. Compiler refusals remain unmet required cases, not successful query acceptance.'}
    (output/'report.json').write_text(encoded(result)+'\n');return result
