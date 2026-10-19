# SPDX-License-Identifier: MIT OR Apache-2.0
"""Four authored indexed queries in a genuine Truss publication hold."""
import json,hashlib
from collections import Counter
from ashlar.manifest import manifest_pin_vector
from ashlar.publication import resolve_publication
from ashlar.weft_implicit_arithmetic_distribution import (ImplicitArithmeticDistributionPaths,compile_implicit_arithmetic_distribution,installed_implicit_arithmetic_schema_bundle)
from .driver import NativeDriver
from .truss_feed_sessions import TrussFeedSessions
from .lifecycle import owned_context
from .delta_custody import encoded
from .indexed_query import decode_compile_artifact
from .truss_weft_query import truss_query_aliases
from .truss_indexed_commerce_request import truss_indexed_commerce_request
from .truss_indexed_commerce_oracle import (AcceptedCommerceHome,CurrentCommerceObject,IndexedCommercePopulation,current_truss_commerce_oracle)
from .truss_indexed_commerce_guard import admit_truss_indexed_commerce_guard
from .count_star_admission import admit_implicit_arithmetic_artifact
from .count_star_schema import make_offline_implicit_arithmetic_schema_validation
from .implicit_arithmetic_profile import ImplicitArithmeticAdmissionConfig,PROFILE
from .count_star_execution import decode_indexed_scalar
from .path_capture import PathCaptureConfig,capture_string_frame


def population_for_binding(admission,binding,expected,*,revision,document_id):
    """Pure snapshot of complete published rows and actual accepted property homes."""
    model,_=admission.revisions[revision]['models'][document_id]
    declarations={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    homes=[];selected={}
    for record in binding['records']:
        physical=record['schemaRevision'];actual=admission.revisions[physical]['types'][record['typeId']]
        if actual['owner']!=(document_id,record['logical']['module'],record['logical']['element']):raise ValueError('Actual original record owner required')
        marker=(record['sourceSystem'],record['typeId']);selected[marker]=physical
        for property in record['properties']:
            identity=property['logical'];field=declarations[identity['module'],identity['element']];pid=property['home']['propertyId']
            if field['scalarType']not in('string','integer','decimal'):raise ValueError('Original commerce scalar home required')
            homes.append(AcceptedCommerceHome(record['sourceSystem'],record['typeId'],physical,record['logical']['element'],field['name'],pid,field['scalarType'],tuple(sorted(actual['facets'][pid].items()))))
    objects=[]
    for row in expected['object_current']:
        marker=(row['source_system'],row['type_id'])
        if marker not in selected:continue
        if row['schema_revision']!=selected[marker]:raise ValueError('Original homogeneous physical population required')
        objects.append(CurrentCommerceObject(row['source_system'],row['type_id'],row['id'],row['schema_revision'],row['props_json']))
    return IndexedCommercePopulation(tuple(objects),tuple(homes),16*1048576,10000)


def execute_indexed_commerce_artifact(spark,artifact,guard,population,*,authored_sql,maximum_rows):
    """No authority: caller must supply the genuine held original compiler artifact."""
    from .truss_indexed_commerce_guard import TrussIndexedCommerceGuard
    if type(guard)is not TrussIndexedCommerceGuard or type(population)is not IndexedCommercePopulation or type(maximum_rows)is not int or not 1<=maximum_rows<=10000:raise ValueError('Nominal bounded Truss execution inputs required')
    slots={'p'+str(p['position']):p['value']for p in artifact['parameters']};evidence=[];remaining=1048576
    # Source projections are independent of the user output budget. Each complete
    # projection fits the existing 1 MiB capture bound; never run a user LIMIT here.
    def capture(check,rows):
        nonlocal remaining
        if remaining<=0:raise ValueError('Complete execution byte budget exhausted')
        frame=capture_string_frame(spark.sql(check['sql'],args=slots),config=PathCaptureConfig(rows,min(65536,remaining),remaining))
        remaining-=sum(len(cell.encode())for row in frame['rows']for cell in row if cell is not None)
        evidence.append({'check':check,'native':frame});return frame
    def zero(frame):return frame['rows']==[['0']]and len(frame['schema'])==1 and frame['schema'][0][1]=='STRING'
    obligations=artifact['obligations'];scalar=next(o for o in obligations if o['id']=='ashlar.candidate.scalarIntegrity')
    checks=scalar['parameters']['checks']
    for check in checks:
        if check.get('publicSourceOnly')is True:guard.require_source(check,capture(check,guard.source_budget(check)))
    # Preserve the original subsequence within each semantic phase. The complete
    # public-source population is validated before any representability check.
    for check in checks:
        if check.get('publicSourceOnly')is True or check.get('representabilityOnly')is True:continue
        if not zero(capture(check,1)):raise ValueError('Original scalar integrity guard refused')
    for check in checks:
        if check.get('representabilityOnly')is True:
            guard.entry(check)
            if not zero(capture(check,1)):raise ValueError('Original representability guard refused')
    for obligation in obligations:
        if obligation['id']!='ashlar.arithmetic.exact':continue
        for check in obligation['parameters']['checks']:
            if not zero(capture(check,1)):raise ValueError('Original arithmetic capacity guard refused')
    # The independent finite calculation follows source/capacity discharge, so it
    # does not misclassify a valid unbounded source as an invalid logical domain.
    expected=current_truss_commerce_oracle(population,authored_sql,maximum_rows=maximum_rows,maximum_numeric_digits=38)
    if remaining<=0:raise ValueError('Complete execution byte budget exhausted')
    result=capture_string_frame(spark.sql(artifact['sql'],args=slots),config=PathCaptureConfig(maximum_rows,min(65536,remaining),remaining))
    names=[c.get('carrierName',c['outputName'])for c in artifact['columns']]
    if [entry[0]for entry in result['schema']]!=names or any(len(row)!=len(names)for row in result['rows']):raise ValueError('Original positional output columns required')
    decoded=[]
    for row in result['rows']:
        decoded.append([decode_indexed_scalar(column,output['expression'],cell,numeric_capacity_admitted=True,type_graph=artifact['logicalPlan']['typeGraph'])for column,output,cell in zip(artifact['columns'],artifact['logicalPlan']['outputs'],row)])
    if artifact['logicalPlan']['order']:
        equal=result['rows']==expected['rows']
    else:equal=Counter(encoded(row)for row in result['rows'])==Counter(encoded(row)for row in expected['rows'])
    if not equal:raise ValueError('Complete current Truss commerce occurrence bag differs')
    return {'rows':result['rows'],'columns':artifact['columns'],'native_result':result,'oracle':expected,'guards':evidence}


def execute_truss_indexed_commerce_query(driver,paths,*,case_id,pack_bytes,revision,document_id,maximum_rows=1000,empty_record_revisions=()):
    """Original indexed protocol, native pins, complete source and outer cleanup."""
    if type(driver)is not NativeDriver or type(paths)is not ImplicitArithmeticDistributionPaths or type(driver.source_sessions)is not TrussFeedSessions or driver.ack_sessions is not driver.source_sessions:raise PermissionError('Actual Truss publication and indexed compiler owners required')
    if type(maximum_rows)is not int or not 1<=maximum_rows<=10000:raise ValueError('Bounded original result budget required')
    bundle=dict(installed_implicit_arithmetic_schema_bundle(paths));config=ImplicitArithmeticAdmissionConfig(16*1048576,make_offline_implicit_arithmetic_schema_validation({name:bundle[name]for name,_ in PROFILE.schemas}))
    with owned_context(driver.writer(driver.policy.active['request']['stream'],driver.context)):
        manifest=dict(driver.policy.active['manifest']);uuids={table:driver.transport.targets[table].uuid for table in json.loads(manifest['table_versions_json'])}
        with owned_context(driver.hold(manifest_pin_vector(manifest,uuids,authority='private-local-process'),context=driver.context)):
            resolved=resolve_publication(driver,manifest['publication_id'],uuids,context=driver.context,supported_profiles=['ashlar-delta/0.3'],supported_revisions={key:[value]for key,value in json.loads(manifest['schema_revisions_json']).items()})
            if dict(resolved.descriptor.raw)!=manifest:raise PermissionError('Original held Truss publication differs')
            with owned_context(truss_query_aliases(driver,manifest))as aliases:
                request=truss_indexed_commerce_request(driver.source_admission,manifest,uuids,driver.transport.targets[driver.tables['manifest']].uuid,case_id=case_id,pack_bytes=pack_bytes,expected=driver.policy.active['expected'],revision=revision,document_id=document_id,empty_record_revisions=empty_record_revisions,aliases=aliases)
                request_bytes=(encoded(request)+'\n').encode();response=compile_implicit_arithmetic_distribution(paths,request_bytes);recompiled=compile_implicit_arithmetic_distribution(paths,request_bytes)
                artifact=decode_compile_artifact(response);admit_implicit_arithmetic_artifact(request,artifact,decode_compile_artifact(recompiled),config=config)
                # Preserve the public artifact unchanged. The oracle's SQL selector
                # comes from the original request rather than an invented alias.
                wanted={'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity','ashlar.arithmetic.exact'}|({'weft.output.positioned'}if case_id=='partial-return'else set())
                if len(artifact['obligations'])!=len(wanted)or {o['id']for o in artifact['obligations']}!=wanted:raise ValueError('Exact original case obligation inventory required')
                binding=json.loads(request['target']['bindingJson']);population=population_for_binding(driver.source_admission,binding,driver.policy.active['expected'],revision=revision,document_id=document_id)
                guard=admit_truss_indexed_commerce_guard(driver.source_admission,request,artifact,driver.policy.active['expected'],revision=revision,document_id=document_id,case_id=case_id)
                driver.validate_manifest(manifest,driver.context)
                # Check actual native source column types on the pinned version.
                objects=next(t for t in binding['publication']['tables']if any(r['kind']=='object'and r['table']==binding['publication']['tables'].index(t)for r in binding['records']))
                table='`'+'`.`'.join(objects['name'])+'`';schema=driver.transport.spark.sql('SELECT * FROM '+table+' VERSION AS OF '+str(objects['version'])).schema
                required={'id':'bigint','type_id':'bigint','source_system':'string','schema_revision':'string','props_json':'string'}
                if any(dict((f.name,f.dataType.simpleString())for f in schema.fields).get(name)!=kind for name,kind in required.items()):raise ValueError('Original pinned native object schema required')
                result=execute_indexed_commerce_artifact(driver.transport.spark,artifact,guard,population,authored_sql=request['sql'],maximum_rows=maximum_rows)
                driver.validate_manifest(manifest,driver.context)
    return {'request_bytes':request_bytes,'response_bytes':response,'recompiled_bytes':recompiled,'request_sha256':hashlib.sha256(request_bytes).hexdigest(),'response_sha256':hashlib.sha256(response).hexdigest(),'recompiled_sha256':hashlib.sha256(recompiled).hexdigest(),'publication_id':manifest['publication_id'],'case':case_id,**result,'qualification':'Original indexed0.4.2 commerce case on a held Truss publication, truthful per-record homogeneous physical revisions, full original source/capacity guards and independent current positional occurrence bag. Outer source/Spark cleanup remains required; no production qualification.'}
