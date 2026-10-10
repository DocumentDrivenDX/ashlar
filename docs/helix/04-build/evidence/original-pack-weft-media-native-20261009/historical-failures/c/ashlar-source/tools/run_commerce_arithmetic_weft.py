"""Original commerce arithmetic on a held canonical publication; experimental local only.

This host submits compiler SQL/checks unchanged. It grants no production/UC
qualification, and releases no evidence before all native/ACK reader closes.
"""
import hashlib,json,subprocess,re,tempfile,copy
from decimal import Decimal
from run_commerce_publication_weft import compiler_request as old_request,encoded,SOURCE_SHA
from check_commerce_graphframes_scenarios import scenario_oracle

HAVING_BACKEND={'backendId':'ashlar.databricks.count-having','backendVersion':'0.3.0-count-having-candidate','targetProfile':'spark4-delta4-count-having-candidate'}
COUNT_BACKEND={'backendId':'ashlar.databricks.count-distinct','backendVersion':'0.3.0-count-distinct-candidate','targetProfile':'spark4-delta4-count-distinct-candidate'}
BACKEND={'backendId':'ashlar.databricks','backendVersion':'0.3.0-arithmetic-candidate','targetProfile':'spark4-delta4-arithmetic-candidate'}
OBLIGATIONS={'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity','ashlar.arithmetic.exact'}

def compiler_request(sql,model,bindings,manifest,registry,aliases):
    request=old_request(sql,model,bindings,manifest,registry,aliases,fields=True)
    if bindings['profile']!='ashlar-commerce-development-bindings/0.2':raise ValueError('Original canonical property bindings required')
    binding=json.loads(request['target']['bindingJson']);pin=binding['modelPins'][0]
    document=json.loads(model);properties={tuple(p['identity']):p['property_id'] for p in bindings['properties']}
    if len(properties)!=34 or len(properties)!=len(bindings['properties']) or len(set(properties.values()))!=34:raise ValueError('Complete injective original 34 Field inventory required')
    records=[];object_index=binding['records'][0]['table']
    for entry in bindings['types']:
        if entry['identity'][0]!='object':continue
        _,module,element=entry['identity'];record=next(e for m in document['modules'] if m['id']==module for e in m['elements'] if e['id']==element)
        homes=[]
        for member in record['members']:
            identity=(pin['documentId'],member['module'],member['element'])
            if identity not in properties:raise ValueError('Missing original qualified property')
            homes.append({'logical':{'documentId':identity[0],'module':identity[1],'element':identity[2],'revision':SOURCE_SHA},'home':{'kind':'props','propertyId':properties[identity]}})
        records.append({'kind':'object','logical':{'documentId':pin['documentId'],'module':module,'element':element,'revision':SOURCE_SHA},'sourceSystem':'private-original-commerce-fixture','typeId':entry['type_id'],'table':object_index,'schemaRevision':SOURCE_SHA,'properties':homes})
    if len(records)!=10 or sum(len(r['properties']) for r in records)!=34:raise ValueError('All original ten Record homes required')
    binding['records']=records
    raw=encoded(binding);request.update(interfaceVersion='weft-compile/0.3.0',dialect='weft-sql/0.3.0')
    request['target']={**BACKEND,'bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()}
    return request

def scenarios():
    expected,sources,pack=scenario_oracle();cases=[]
    for case in pack['scenario_checks']:
        sql=case['sql'];qualification=None
        if case['id']=='partial-return':
            sql='SELECT f.quantity AS fulfilled_quantity,r.quantity AS returned_quantity,l.quantity-f.quantity+r.quantity AS remaining_quantity FROM order_lines l JOIN fulfillments f ON f.line_id=l.id JOIN returns r ON r.line_id=l.id WHERE l.quantity>f.quantity'
            qualification={'originalSql':case['sql'],'equivalentQuery':'Explicit output aliases and reversed operands with equivalent > predicate; original source and bag unchanged.'}
        cases.append({'id':case['id'],'sql':sql,'expected':expected[case['id']],'equivalentQueryQualification':qualification})
    return cases,sources

def original_graph_expected(case,graph):
    expected=[[str(cell)for cell in row]for row in case['expected']]
    if case['id']=='partial-return':return expected,[]
    records={'fulfillment':'fulfillments','settlement':'payments','refund':'refunds'}
    record=records[case['id']];witnesses=[];mapped=[]
    matches=[b for b in graph['source_bindings']if b.get('role')=='rows' and b.get('schema_id')==record and b.get('source_id')=='scenario_replay_output']
    if len(matches)!=1:raise ValueError('Unique original replay source binding required')
    if graph['run']['components']!=1 or type(graph['run']['seed'])is not int:raise ValueError('Exact original single-component replay qualification required')
    for row in expected:
        if len(row)!=1:raise ValueError('Original identifier scenario shape required')
        matches=[]
        for obj in graph['objects']:
            if obj['type']['element']!=record:continue
            original=obj['values'][record+'.id']
            try:replay=json.loads(original)
            except (ValueError,TypeError):continue
            if replay==[graph['run']['seed'],0,row[0]]:matches.append((obj,original))
        if len(matches)!=1:raise ValueError('Unique original replay identity witness required')
        obj,original=matches[0];mapped.append([original]);witnesses.append({'sourceRecord':record,'originalGraphKey':obj['key'],'originalGraphField':record+'.id','originalGraphValue':original,'originalCsvValue':row[0],'replaySeed':graph['run']['seed'],'component':0,'originalSourceBinding':next(b for b in graph['source_bindings']if b.get('schema_id')==record and b.get('role')=='rows' and b.get('source_id')=='scenario_replay_output'),'qualification':'Original TableSpec scenario-replay identity encoding; native text remains unchanged.'})
    return mapped,witnesses

def guard_controls():
    maximum='9'*38;product='l.quantity*'+maximum
    return [
      ('projection-cancellation','SELECT '+product+' - '+product+' AS canceled FROM order_lines l',True),
      ('where-prefilter','SELECT l.id FROM order_lines l WHERE l.quantity=0 AND '+product+'>1',True),
      ('join-prepredicate','SELECT l.id FROM order_lines l JOIN returns r ON '+product+'=r.quantity AND r.line_id=l.id',True),
      ('projection-empty-survivors','SELECT '+product+' AS omitted FROM order_lines l WHERE l.quantity=0',False)]

def compile_original(executable,request,compiler_sha256):
    if hashlib.sha256(executable.read_bytes()).hexdigest()!=compiler_sha256:raise ValueError('Exact reviewed compiler required')
    process=subprocess.run([str(executable)],input=encoded(request),capture_output=True,text=True,timeout=30)
    if process.returncode:raise ValueError('Actual compiler refused')
    artifact=json.loads(process.stdout)
    if artifact.get('status')!='compiled' or artifact.get('bindingSha256')!=request['target']['bindingSha256'] or artifact.get('modelPins')!=[request['modules'][0]['pin']]:raise ValueError('Original compilation custody differs')
    return artifact

def admit_operator_free_plan(artifact,binding):
    plan=artifact.get('logicalPlan');keys={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
    if type(plan)is not dict or set(plan)!=keys or plan['irVersion']!='weft-ir/0.3.0' or plan['aggregate']is not False or any(plan[k]!=[] for k in ['filters','groups','joins','order']) or any(plan[k]is not None for k in ['limit','pageKey','readProfile']) or plan['modulePins']!=binding['modelPins']:raise ValueError('Empty arithmetic checks require an original operator-free field plan')
    allowed={'project','scan','type.integer','type.integer.unbounded','type.string','type.boolean','type.decimal'}
    if type(plan['requiredCapabilities'])is not list or not set(plan['requiredCapabilities'])<=allowed:raise ValueError('Unproved operator-free capabilities')
    source=plan['source']
    if set(source)!={'occurrence','pin','record'} or source['pin']not in binding['modelPins']:raise ValueError('Original operator-free source required')
    records=[r for r in binding['records']if r['logical']==source['record']]
    if len(records)!=1 or type(plan['outputs'])is not list or not plan['outputs'] or [o['name']for o in plan['outputs']]!=[c['outputName']for c in artifact['columns']]:raise ValueError('Original operator-free output/source correspondence required')
    identities=[p['logical']for p in records[0]['properties']]
    for output in plan['outputs']:
        expression=output['expression']
        if set(output)!={'name','expression'} or set(expression)!={'identity','op','scan'} or expression['op']!='field' or expression['scan']!=source['occurrence'] or expression['identity']not in identities:raise ValueError('Only original direct Field outputs admit empty arithmetic checks')

class NativeGuardRefusal(ValueError):
    def __init__(self,obligation,check,rows,prior_checks):
        super().__init__('Original native '+obligation+' refused before user query')
        self.evidence={'obligation':obligation,'check':check,'rows':rows,'priorChecks':prior_checks,'userSqlExecuted':False,'resultReleased':False}

def execute_guarded(provider,request,artifact,*,context,public_source=None,positioned_outputs=False,native_null=False,distinct=False,count_distinct=False,count_having=False):
    # Private snapshots precede every admission callback and native action.
    request,artifact=copy.deepcopy(request),copy.deepcopy(artifact)
    binding=json.loads(request['target']['bindingJson'])
    if type(count_distinct) is not bool or (count_distinct and (native_null or distinct or positioned_outputs)):
        raise ValueError('Explicit separate String set/count host opt-in required')
    if type(count_having) is not bool or (count_having and (count_distinct or native_null or distinct or positioned_outputs)):
        raise ValueError("Explicit separate optional-count/HAVING host opt-in required")
    count_route=count_distinct or count_having
    backend=HAVING_BACKEND if count_having else COUNT_BACKEND if count_distinct else BACKEND
    if request['target']!={**backend,'bindingJson':request['target']['bindingJson'],'bindingSha256':hashlib.sha256(request['target']['bindingJson'].encode()).hexdigest()} or artifact.get('status')!='compiled' or artifact.get('bindingSha256')!=request['target']['bindingSha256'] or artifact.get('modelPins')!=binding['modelPins']:raise ValueError('Exact compiler/binding custody required')
    plan = artifact.get('logicalPlan',{})
    if count_route:
        from weft_field_plan import admit_field_plan
        admit_field_plan(artifact,binding,request['modules'],count_distinct=count_distinct,count_having=count_having)
    uses_distinct = 'distinct' in plan or 'project.distinct' in plan.get('requiredCapabilities',[])
    if type(distinct) is not bool or (uses_distinct and not distinct) or (distinct and native_null):
        raise ValueError('Explicit separate DISTINCT host opt-in required before callbacks')
    if distinct:
        from weft_field_plan import admit_field_plan
        admit_field_plan(artifact,binding,request['modules'],distinct=True)
    null_caps = {'predicate.nativeNull','compare.nullAwareStringEqual','value.nativeNull'}
    uses_null = bool(null_caps & set(artifact.get('logicalPlan',{}).get('requiredCapabilities',[])))
    uses_null = uses_null or any(p.get('home',{}).get('encoding')=='ashlar-weft-json-native-null/0.1-candidate' for r in binding.get('records',[]) for p in r.get('properties',[]))
    if type(native_null) is not bool or (uses_null and not (native_null or count_having)):
        raise ValueError('Explicit native-null host opt-in required before callbacks')
    if native_null:
        from weft_field_plan import admit_field_plan
        admit_field_plan(artifact,binding,request['modules'],native_null=True)
    obligations=artifact.get('obligations',[])
    positioned = any(o.get('id')=='weft.output.positioned' for o in obligations)
    expected_obligations=OBLIGATIONS | ({'weft.output.positioned'} if positioned else set())
    if len(obligations)!=len(expected_obligations) or {o['id'] for o in obligations}!=expected_obligations:raise ValueError('All and only original required obligations must be fulfilled')
    from weft_field_plan import admit_positioned_outputs,admit_positioned_cells
    if type(positioned_outputs) is not bool:raise ValueError('Explicit positioned host opt-in must be Boolean')
    if positioned:
        if not positioned_outputs:raise ValueError('Positioned outputs require explicit host opt-in')
        admit_positioned_outputs(artifact)
        from weft_field_plan import admit_field_plan
        admit_field_plan(artifact,binding,request['modules'],positioned_output_only=True,distinct=distinct)
        if not callable(getattr(provider,'sql_ordered',None)):raise ValueError('Explicit native ordered transport required')
    elif any('carrierName' in c for c in artifact.get('columns',[])) or 'project.positionedOutputs' in artifact.get('logicalPlan',{}).get('requiredCapabilities',[]):
        raise ValueError('Positioned descriptor requires explicit obligation handler')
    params_by_id={o['id']:o['parameters'] for o in obligations};publication=params_by_id['ashlar.candidate.publication']
    if any(publication[k]!=binding[k] for k in ['publication','modelPins','layoutRevision','layoutSha256']):raise ValueError('Full publication obligation differs')
    for name in ['ashlar.candidate.scalarIntegrity','ashlar.arithmetic.exact']:
        p=params_by_id[name]
        if p['phase']!='before-user-query' or p['samePublicationRequired']is not True or p['noPartialPublication']is not True or type(p['checks'])is not list or (name=='ashlar.candidate.scalarIntegrity' and not p['checks']):raise ValueError('Mandatory complete before-query checks required')
    arithmetic=params_by_id['ashlar.arithmetic.exact']
    if not arithmetic['checks']:
        plan=artifact.get('logicalPlan',{})
        if native_null or distinct or count_route:
            pass  # complete explicit original optional/distinct field proof above
        elif any(plan.get(k) for k in ('filters','joins','order')):
            from weft_field_plan import admit_field_plan
            admit_field_plan(artifact,binding,request['modules'],distinct=distinct)
        else:admit_operator_free_plan(artifact,binding)
    if arithmetic.get('nativeRepresentation')!='DECIMAL(38,0) coefficients' or type(arithmetic.get('maxScale'))is not int or arithmetic['maxScale']!=18:raise ValueError('Exact native arithmetic profile required')
    phases=('aggregate-candidates',) if count_route else ('join-candidates','where-candidates','projection-survivors')
    count_outputs=[i for i,o in enumerate(plan.get('outputs',[])) if o['expression'].get('op')=='countDistinct'] if count_route else []
    if count_route and len(arithmetic['checks'])!=len(count_outputs):raise ValueError('Every distinct-count output requires its emitted capacity guard')
    if any(c.get('phase')not in phases for c in arithmetic['checks']):raise ValueError('Original arithmetic evaluation phases required')
    for check in params_by_id['ashlar.candidate.scalarIntegrity']['checks']:
        for flag in ['publicSourceOnly','representabilityOnly']:
            if flag in check and type(check[flag])is not bool:raise ValueError('Exact original guard kind required')
        if check.get('publicSourceOnly')is True and check.get('representabilityOnly')is True:raise ValueError('Distinct original source/capacity dispositions required')
    if count_having and not callable(getattr(provider,'sql_ordered',None)):raise ValueError('Optional-count host requires exact native schema/ordered-cell transport')
    params={'p'+str(p['position']):p['value'] for p in artifact['parameters']};checks=[];completed=False
    with provider.interval(context):
        opening=provider.resolve(context);provider.admit_binding(binding,opening,context);provider.runtime(context)
        for name in ['ashlar.candidate.scalarIntegrity','ashlar.arithmetic.exact']:
            for check in params_by_id[name]['checks']:
                rows=provider.sql(check['sql'],params)
                public_receipt=None
                if check.get('publicSourceOnly') is True:
                    if name!='ashlar.candidate.scalarIntegrity' or public_source is None:raise ValueError('Mandatory actual public source admission unavailable')
                    public_receipt=public_source(request,artifact,[{'check':check,'rows':rows}])
                elif rows!=[{'violations':'0'}]:
                    if len(rows)==1 and set(rows[0])=={'violations'} and type(rows[0]['violations'])is str and re.fullmatch('[0-9]+',rows[0]['violations']) and int(rows[0]['violations'])>0:raise NativeGuardRefusal(name,check,rows,checks)
                    raise ValueError('Malformed original native violation count')
                checks.append({'obligation':name,'check':check,'rows':rows,'publicSourceReceipt':public_receipt})
        ordered=None
        if positioned:
            ordered=admit_positioned_cells(artifact,provider.sql_ordered(artifact['sql'],params));rows=ordered['rows']
        elif count_having:
            ordered=admit_count_ordered_cells(artifact,provider.sql_ordered(artifact['sql'],params))
            rows=[dict(zip([c['outputName'] for c in artifact['columns']],row)) for row in ordered['rows']]
        else:rows=provider.sql(artifact['sql'],params)
        if count_route:
            admit_count_result_rows(artifact,rows,count_having=count_having)
        provider.runtime(context);closing=provider.resolve(context);provider.admit_binding(binding,closing,context)
        if dict(opening.descriptor.raw)!=dict(closing.descriptor.raw) or opening.snapshots!=closing.snapshots:raise ValueError('Whole publication changed before release')
        completed=True
    if not completed:raise ValueError('Suppressed interval failure')
    return {'rows':rows,'checks':checks,**({'positioned':ordered,'native_schema':ordered['schema'],'ordered_rows':ordered['rows']} if positioned or count_having else {})}

def admit_count_ordered_cells(artifact,observed):
    """Preserve actual unique native names, schema and ordered cells; no repair."""
    if type(observed) is not dict or set(observed)!={'schema','rows'} or type(observed['schema']) is not list or type(observed['rows']) is not list:
        raise ValueError('Exact native ordered result structure required')
    names=[c['outputName'] for c in artifact['columns']]
    if len(set(names))!=len(names) or observed['schema'] != [[name,'STRING']for name in names]:
        raise ValueError('Native unique output names/order/exact text schema required')
    for row in observed['rows']:
        if type(row) is not list or len(row)!=len(names) or any(type(cell)is not str for cell in row):
            raise ValueError('Complete nonnull exact text native count cells required')
    return observed

def admit_count_result_rows(artifact,rows,*,count_having=False):
    """Check exact native count carrier/cardinality; never repair a result bag."""
    plan=artifact['logicalPlan'];columns=artifact['columns']
    if type(rows) is not list:raise ValueError('Complete native result list required')
    if type(count_having) is not bool:raise ValueError('Explicit HAVING result route required')
    if plan.get('having') and not count_having:raise ValueError('HAVING requires separate result admission')
    if plan['aggregate'] and not plan['groups']:
        if count_having and plan.get('having'):
            if len(rows)>1:raise ValueError('Global HAVING requires zero or one native row')
        elif len(rows)!=1:raise ValueError('Global distinct count requires exactly one native row')
    count_names=[c['outputName'] for c,o in zip(columns,plan['outputs']) if o['expression'].get('op')=='countDistinct']
    group_names=[c['outputName'] for c,o in zip(columns,plan['outputs']) if o['expression'].get('op')!='countDistinct']
    groups=set()
    for row in rows:
        if type(row) is not dict or set(row)!={c['outputName'] for c in columns}:raise ValueError('Exact original native count output cells required')
        for name in count_names:
            value=row[name]
            if type(value) is not str or not re.fullmatch('0|[1-9][0-9]*',value) or int(value)>9223372036854775807:raise ValueError('Exact nonnegative signed64 native count text required')
        if plan['aggregate'] and plan['groups']:
            key=encoded([row[n] for n in group_names])
            if key in groups:raise ValueError('Native grouped result contains a duplicate complete group tuple')
            groups.add(key)
    return rows

def admit_public_source(script,umf,request,artifact,checks):
    from pathlib import Path
    expected='68f0ad41a4cd090281a0ce5e89d6ab8c93873d882a698a593e21d68c7dbf2577'
    if hashlib.sha256(script.read_bytes()).hexdigest()!=expected:raise ValueError('Exact reviewed public UMF source guard required')
    original={'sourceText':request['modules'][0]['documentJson'],'modelPins':artifact['modelPins'],'bindingSha256':artifact['bindingSha256'],'checks':checks}
    raw=encoded(original)+'\n'
    with tempfile.TemporaryDirectory(prefix='ashlar-public-source-') as temporary:
        path=Path(temporary);(path/'request.json').write_text(raw)
        subprocess.run(['bun',str(script),str(umf),str(path/'request.json'),str(path/'receipt.json')],check=True,capture_output=True,text=True,timeout=30)
        receipt_raw=(path/'receipt.json').read_text();receipt=json.loads(receipt_raw)
        if receipt['originalRequestText']!=raw or receipt['umfRevision']!='c7c95e1c4ea5b72541f47fa0350ca467ff02f395' or receipt['admitted']is not True:raise ValueError('Original source validity/token custody refused')
        return {'originalRequestText':raw,'originalReceiptText':receipt_raw,'receiptSha256':hashlib.sha256(receipt_raw.encode()).hexdigest()}

def run(publication,output,jars,compiler,compiler_sha256,source_guard,umf):
    from pathlib import Path
    import importlib.metadata
    from run_commerce_publication_weft import open_commerce_reader
    from run_local_weft_typed_spark4 import JAR_SHA
    from pyspark.sql import SparkSession
    if output.exists():raise ValueError('Fresh exclusive evidence directory required')
    if importlib.metadata.version('pyspark')!='4.0.1':raise ValueError('Explicit local Spark4.0.1 required')
    paths=[jars/n for n in ('delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar')]
    if any(not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest()!=JAR_SHA[p.name] for p in paths):raise ValueError('Exact existing Delta4 jars required')
    cases,sources=scenarios();output.mkdir(mode=0o700)
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar original publication arithmetic Weft')
      .config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
      .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC')
      .config('spark.sql.ansi.enabled','true').config('spark.sql.decimalOperations.allowPrecisionLoss','false')
      .config('spark.jars',','.join(str(p) for p in paths)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
      .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR');pending={};completed=False
    try:
        with open_commerce_reader(spark,publication) as opened:
            provider=opened.provider;context=opened.context
            original_graph=json.loads(opened.graph)
            from commerce_source_transaction import ROOT
            if hashlib.sha256((ROOT/'pack.json').read_bytes()).hexdigest()!=original_graph['pack']['sha256']:raise ValueError('Original scenario pack pin differs')
            for table,source in sources.items():
                if source['sha256']!=original_graph['source_metadata']['template_'+table]['checksum']['value']:raise ValueError('Original oracle CSV source pin differs')
            with provider.interval(context):
                spark.sql('CREATE DATABASE commerce').collect()
                for native,alias in opened.aliases.items():
                    target=opened.driver.transport.targets[native]
                    spark.sql('CREATE TABLE '+alias+' USING DELTA LOCATION '+"'"+str(target.path)+"'").collect()
                if opened.native_files()!=opened.original_native_files:raise ValueError('Alias registration mutated original bytes')
            probes=[]
            for case in cases:
                req=compiler_request(case['sql'],opened.model,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases)
                provider.expected_binding=encoded(json.loads(req['target']['bindingJson']))
                artifact=compile_original(compiler,req,compiler_sha256)
                result=execute_guarded(provider,req,artifact,context=context,public_source=lambda r,a,c:admit_public_source(source_guard,umf,r,a,c))
                # Exact public nativeScalar descriptor admission is completed only
                # with the frozen original compiler artifact, never inferred SQL.
                decoded=decode_rows(artifact,result['rows'])
                expected,witnesses=original_graph_expected(case,original_graph)
                if sorted(decoded,key=encoded)!=sorted(expected,key=encoded):raise ValueError('Independent original source scenario bag differs: '+encoded({'case':case['id'],'actual':decoded,'expected':expected}))
                probes.append({'case':case,'result':result,'exact_text_rows':decoded,'independent_expected_text':expected,'original_replay_identity_witnesses':witnesses})
                pending[case['id']+'-request.json']=encoded(req)+'\n';pending[case['id']+'-artifact.json']=encoded(artifact)+'\n'
            controls=[]
            for name,sql,must_refuse in guard_controls():
                req=compiler_request(sql,opened.model,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases)
                provider.expected_binding=encoded(json.loads(req['target']['bindingJson']));artifact=compile_original(compiler,req,compiler_sha256)
                try:
                    result=execute_guarded(provider,req,artifact,context=context,public_source=lambda r,a,c:admit_public_source(source_guard,umf,r,a,c))
                except NativeGuardRefusal as refusal:
                    if not must_refuse or refusal.evidence['obligation']!='ashlar.arithmetic.exact':raise
                    controls.append({'name':name,'expectedDisposition':'backend-capability','actual':refusal.evidence})
                else:
                    if must_refuse or result['rows']:raise ValueError('Required native overflow refusal or empty-survivor bag differs')
                    controls.append({'name':name,'expectedDisposition':'empty-survivor-pass','actual':result})
                pending[name+'-request.json']=encoded(req)+'\n';pending[name+'-artifact.json']=encoded(artifact)+'\n'
            req=compiler_request('SELECT l.quantity AS quantity FROM order_lines l',opened.model,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases)
            changed=json.loads(req['target']['bindingJson']);next(r for r in changed['records']if r['logical']['element']=='order_lines')['schemaRevision']='wrong-original-schema-revision'
            raw=encoded(changed);req['target']['bindingJson']=raw;req['target']['bindingSha256']=hashlib.sha256(raw.encode()).hexdigest();provider.expected_binding=raw
            artifact=compile_original(compiler,req,compiler_sha256)
            try:execute_guarded(provider,req,artifact,context=context,public_source=lambda r,a,c:admit_public_source(source_guard,umf,r,a,c))
            except NativeGuardRefusal as refusal:
                if refusal.evidence['obligation']!='ashlar.candidate.scalarIntegrity':raise
                controls.append({'name':'wrong-schema-revision','expectedDisposition':'source-integrity','actual':refusal.evidence})
            else:raise ValueError('Original schema revision guard failed to refuse')
            pending['wrong-schema-revision-request.json']=encoded(req)+'\n';pending['wrong-schema-revision-artifact.json']=encoded(artifact)+'\n'
            if opened.native_files()!=opened.original_native_files:raise ValueError('Read-only arithmetic mutated original native bytes')
            report={'format':'ashlar-local-publication-arithmetic-weft/0.1','qualification':__doc__,'compiler_sha256':compiler_sha256,'runtime':'experimental-spark4.0.1-delta4.0.0','qualified_databricks_profile':False,'publication':opened.manifest,'original_native_files':opened.original_native_files,'original_source_csv':sources,'queries':probes,'guardControls':controls}
        # Outer reader closes native holds, ACK session and final whole file hash
        # check before any result/request/artifact evidence becomes visible.
        pending['report.json']=encoded(report)+'\n'
        completed=True
    finally:
        if not completed:spark.stop()
    if not completed:raise ValueError('Suppressed reader failure')
    persist_after_stop(spark,pending,output)
    return report

def persist_after_stop(spark,pending,output):
    spark.stop()
    for name,raw in pending.items():(output/name).write_text(raw)

def decode_rows(artifact,rows,*,positioned=False,native_schema=None,ordered_rows=None,native_null=False):
    columns=artifact.get('columns')
    if type(columns)is not list or not columns:raise ValueError('Original output columns required')
    if type(native_null)is not bool:raise ValueError('Explicit native-null decoder opt-in required')
    if type(positioned)is not bool:raise ValueError('Explicit positioned decoder opt-in must be Boolean')
    names=[c['outputName'] for c in columns]
    if positioned:
        from weft_field_plan import admit_positioned_cells
        result=admit_positioned_cells(artifact,{'schema':native_schema,'rows':ordered_rows})
        if rows!=ordered_rows:raise ValueError('Original returned row arrays differ from captured native cells')
        cell_rows=result['rows']
    else:
        if native_schema is not None or ordered_rows is not None or any('carrierName'in c for c in columns) or any(o.get('id')=='weft.output.positioned'for o in artifact.get('obligations',[])):raise ValueError('Unknown positioned decoding requires explicit handler')
        if len(set(names))!=len(names) or [c['position']for c in columns]!=list(range(1,len(columns)+1)):raise ValueError('Original ordered injective outputs required')
        if type(rows)is not list or any(type(row)is not dict or set(row)!=set(names)for row in rows):raise ValueError('Exact original output cell inventory required')
        cell_rows=[[row[name]for name in names]for row in rows]
    decoded=[]
    for row in cell_rows:
        values=[]
        for column,value in zip(columns,row):
            rep=column['representation']
            if rep.get('kind') == 'value':
                if not native_null or set(rep) != {'kind','descriptor','nativeNull'} or rep['nativeNull'] is not True or column['nullable'] is not False or type(value) is not str:
                    raise ValueError('Explicit tagged native-null output required')
                descriptors=[d for d in artifact['logicalPlan']['typeGraph'] if d['identity']==rep['descriptor']]
                if len(descriptors)!=1 or descriptors[0]['availability']!='absent-allowed' or descriptors[0]['kind']!='scalar' or column['sourceIdentities']!=[rep['descriptor']]:
                    raise ValueError('Original optional scalar descriptor required')
                descriptor=descriptors[0]
                if set(descriptor)!={'identity','availability','kind','type'} or set(column)!={'position','outputName','sourceIdentities','nullable','representation'}:
                    raise ValueError('Closed original tagged descriptor required')
                logical=descriptor['type']
                if type(logical)is not dict or set(logical)!={'family','facets','nullable'} or logical['nullable']is not False or type(logical['facets'])is not dict:
                    raise ValueError('Original ideal scalar descriptor required')
                family=logical['family'];facets=logical['facets']
                if family not in ('string','boolean','integer','decimal') or (family in ('string','boolean') and facets):
                    raise ValueError('Unknown original ideal scalar meaning')
                # Validate the descriptor even when a row is null: null is not
                # evidence that unknown numeric facets are safe to ignore.
                probe={'string':'','boolean':'false','integer':'0','decimal':'0'}[family]
                scalar={'outputName':'cell','position':1,'nullable':False,'representation':{'kind':'scalar','carrier':'text','decoder':{'string':'text','boolean':'boolean','integer':'exact-integer','decimal':'exact-decimal'}[family],'logicalType':logical}}
                decode_rows({'columns':[scalar]},[{'cell':probe}])
                def unique(pairs):
                    result={}
                    for key,item in pairs:
                        if key in result:raise ValueError('Duplicate tagged Value member')
                        result[key]=item
                    return result
                cell=json.loads(value,object_pairs_hook=unique)
                if type(cell)is not dict or cell.get('state')not in ('null','value') or set(cell)!=({'state'}if cell.get('state')=='null'else{'state','value'}):
                    raise ValueError('Only exact null/value tagged states qualified')
                if cell['state']=='value':
                    logical=descriptors[0]['type'];family=logical['family']
                    inner=cell['value']
                    if family=='boolean':
                        if type(inner)is not bool:raise ValueError('Native Boolean tagged value required')
                        text='true'if inner else'false'
                    else:
                        if type(inner)is not str:raise ValueError('Original exact scalar text required')
                        text=inner
                    synthetic={'columns':[{'outputName':'cell','position':1,'nullable':False,'representation':{'kind':'scalar','carrier':'text','decoder':{'string':'text','boolean':'boolean','integer':'exact-integer','decimal':'exact-decimal'}[family],'logicalType':logical}}]}
                    decode_rows(synthetic,[{'cell':text}])
                values.append(cell)
                continue
            logical=rep['logicalType']
            if set(rep)!={'kind','logicalType','carrier','decoder'} or rep['kind']!='scalar' or rep['carrier']!='text' or set(logical)!={'family','facets','nullable'} or logical['nullable']is not False or column['nullable']is not False or type(value)is not str:raise ValueError('Exact nonnull public scalar text required')
            family=logical['family'];facets=logical['facets']
            if type(facets)is not dict:raise ValueError('Original facets required')
            expected={'string':'text','boolean':'boolean','integer':'exact-integer','decimal':'exact-decimal'}
            if family not in expected or rep['decoder']!=expected[family]:raise ValueError('Unknown original scalar decoder')
            if family=='string':
                if facets:raise ValueError('Unsupported string facets')
                value.encode('utf-8')
            elif family=='boolean':
                if facets or value not in ('true','false'):raise ValueError('Canonical boolean text required')
            elif family=='integer':
                if len(value)>128 or not re.fullmatch('-?[0-9]+',value) or set(facets) not in (set(),{'integerWidth'}):raise ValueError('Exact mathematical Integer text required')
                number=int(value)
                if facets:
                    width=facets['integerWidth']
                    if set(width)!={'bits','signed'} or type(width['bits'])is not int or not 1<=width['bits']<=64 or type(width['signed'])is not bool:raise ValueError('Unknown integer facets')
                    bits=width['bits'];low=-(2**(bits-1)) if width['signed'] else 0;high=2**(bits-int(width['signed']))-1
                    if not low<=number<=high:raise ValueError('Integer result outside original declared width')
            else:
                if len(value)>128 or set(facets) not in ({'scale'},{'precision','scale'}) or type(facets['scale'])is not int or not 0<=facets['scale']<=38 or not re.fullmatch('-?[0-9]+(?:\.[0-9]+)?',value):raise ValueError('Exact decimal text with explicit scale required')
                whole,_,fraction=value.lstrip('-').partition('.')
                if len(fraction.rstrip('0'))>facets['scale']:raise ValueError('Decimal result loses original scale')
                if 'precision'in facets:
                    precision=facets['precision']
                    if type(precision)is not int or not facets['scale']<=precision<=38:raise ValueError('Unknown decimal precision')
                    coefficient=whole+fraction[:facets['scale']]+'0'*max(0,facets['scale']-len(fraction))
                    if len(coefficient.lstrip('0')or'0')>precision:raise ValueError('Decimal result outside original precision')
            values.append(value)
        decoded.append(values)
    return decoded

if __name__=='__main__':
    import argparse
    from pathlib import Path
    p=argparse.ArgumentParser();p.add_argument('--publication',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--jars',type=Path,required=True);p.add_argument('--compiler',type=Path,required=True);p.add_argument('--compiler-sha256',required=True);p.add_argument('--source-guard',type=Path,required=True);p.add_argument('--umf',type=Path,required=True)
    args=p.parse_args();run(args.publication,args.output,args.jars,args.compiler,args.compiler_sha256,args.source_guard,args.umf)
