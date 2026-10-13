"""Original Truss STRING bindings and publication-pinned public Weft queries.

Compiler receipts supply no native/source authority; the live owner supplies it.
"""
import hashlib
import json
import re
import uuid
from contextlib import contextmanager
from ashlar.manifest import manifest_pin_vector,validate_manifest_row
from ashlar.publication import resolve_publication
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar.weft_distribution import DistributionPaths,compile_distribution
from .delta_custody import encoded
from .driver import NativeDriver
from .indexed_query import decode_compile_artifact
from .lifecycle import owned_context, finish
from .truss_feed_sessions import TrussFeedSessions
from .truss_graph_admission import TrussGraphAdmission

BACKEND={'backendId':'ashlar.databricks','backendVersion':'0.1.0-candidate',
    'interfaceVersion':'weft-backend/0.2.0','targetProfile':'dbsql-candidate'}


def truss_string_compile_request(admission,manifest,table_uuids,manifest_uuid,sql,*,revision,document_id,aliases=None):
    """Bind one selected original accepted document/revision without rewriting it."""
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:
        raise ValueError('Actual selected accepted Truss graph revision required')
    if type(sql)is not str or not sql or len(sql.encode())>65536:
        raise ValueError('Bounded authored Weft SQL required')
    validate_manifest_row(manifest)
    if json.loads(manifest['validation_report_json'])['source_admission']!=admission.metadata():
        raise ValueError('Original published Truss semantic admission differs')
    installation=json.loads(admission.batch.begin)['prior']['installationId']
    if json.loads(manifest['schema_revisions_json']).get(installation)!=revision:
        raise ValueError('Selected physical catalog revision differs from publication')
    selected=admission.revisions[revision]
    if document_id not in selected['models']:raise ValueError('Original accepted document required')
    model,original=selected['models'][document_id]
    if any(element.get('kind')=='field'and element.get('scalarType')!='string'for module in model['modules']for element in module['elements']):raise ValueError('Truss Weft profile requires STRING-only declarations')
    source_hash=hashlib.sha256(original).hexdigest()
    pin={'documentId':document_id,'revision':source_hash,'sha256':source_hash,'umfVersion':model['umf']}
    versions=json.loads(manifest['table_versions_json']);names=sorted(versions)
    if set(table_uuids)!=set(names)or len(names)!=4 or {name.split('.')[-1]for name in names}!={'object_current','edge_current','tombstone','whole_source_history'}:
        raise ValueError('Complete original four-role Truss publication required')
    uuid=lambda value:type(value)is str and re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',value)is not None
    if not uuid(manifest_uuid)or any(not uuid(value)for value in table_uuids.values())or len(set(table_uuids.values()))!=len(table_uuids):
        raise ValueError('Complete distinct canonical original native UUIDs required')
    if any(len(name.split('.'))!=3 or any(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',part)is None for part in name.split('.'))for name in names):
        raise ValueError('Canonical qualified original table names required')
    aliases={name:name for name in names}if aliases is None else dict(aliases)
    if set(aliases)!=set(names)or len(set(aliases.values()))!=len(names)or any(type(alias)is not str or len(alias.split('.'))!=3 or any(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',part)is None for part in alias.split('.'))for alias in aliases.values()):
        raise ValueError('Complete injective original table alias inventory required')
    tables=[{'name':aliases[name].split('.'),'uuid':table_uuids[name],'version':versions[name]}for name in names]
    objects=next(index for index,name in enumerate(names)if name.split('.')[-1]=='object_current')
    records=[]
    report=json.loads(selected['report'])
    logical=lambda module,element:{'documentId':document_id,'revision':source_hash,'module':module,'element':element}
    for type_id,record in sorted(selected['types'].items()):
        owner=record['owner']
        if owner[0]!=document_id:continue
        properties=[]
        for field in report['properties']:
            if field['typeId']!=type_id:continue
            if field['propertyId']not in record['fields']:raise ValueError('Actual accepted STRING property required')
            properties.append({'logical':logical(field['moduleId'],field['elementId']),
                'home':{'kind':'props','propertyId':field['propertyId']}})
        records.append({'logical':logical(owner[1],owner[2]),'kind':'object','table':objects,
            'sourceSystem':installation,'typeId':type_id,'schemaRevision':revision,'properties':properties})
    if not records:raise ValueError('Selected original STRING record bindings required')
    binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3',
        'layoutSha256':LAYOUT_SHA256,'modelPins':[pin],
        'publication':{'id':manifest['publication_id'],'manifestUuid':manifest_uuid,'tables':tables},'records':records}
    record_map={record['typeId']:record['logical']for record in records}
    edge_table=next(index for index,name in enumerate(names)if name.split('.')[-1]=='edge_current')
    relationships=[]
    for type_id,edge in sorted(selected['edges'].items()):
        if edge['owner'][0]!=document_id:continue
        source_type,target_type=edge['endpoints']
        if source_type not in record_map or target_type not in record_map:
            raise ValueError('Original accepted relationship endpoint records required')
        relationships.append({'logical':{'documentId':document_id,'revision':source_hash,
            'module':edge['owner'][1],'relationship':edge['owner'][2]},
            'acceptedDefinition':edge['declaration'],'table':edge_table,'kind':'edge',
            'sourceSystem':installation,'typeId':type_id,'schemaRevision':revision,
            'source':record_map[source_type],'target':record_map[target_type]})
    if relationships:binding['relationships']=relationships
    raw=encoded(binding)
    return {'interfaceVersion':'weft-compile/0.2.0','dialect':'weft-sql/0.2.0','sql':sql,
        'modules':[{'documentJson':original.decode(),'pin':pin,'selectedModuleIds':sorted({record['owner'][1]for record in selected['types'].values()if record['owner'][0]==document_id})}],
        'target':{**{key:BACKEND[key]for key in ('backendId','backendVersion','targetProfile')},
            'bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()},'options':{'allowCandidate':True}}


def _truss_string_query(driver,paths,sql,*,revision,document_id,maximum_rows=None):
    """Compile exact authored singleton/list SQL under the actual publication hold.

    Returns original request/response bytes after closing native pins and writer.
    No generated SQL is executed and outer resources remain caller owned.
    """
    if (type(driver)is not NativeDriver or type(paths)is not DistributionPaths
            or type(driver.source_sessions)is not TrussFeedSessions or driver.ack_sessions is not driver.source_sessions):
        raise PermissionError('Actual Truss publication/compiler owners required')
    if maximum_rows is not None and (type(maximum_rows)is not int or not 1<=maximum_rows<=10000):
        raise ValueError('Explicit bounded query row budget required')
    rows=None;checks=None
    with owned_context(driver.writer(driver.policy.active['request']['stream'],driver.context)):
        manifest=dict(driver.policy.active['manifest'])
        uuids={table:driver.transport.targets[table].uuid for table in json.loads(manifest['table_versions_json'])}
        vector=manifest_pin_vector(manifest,uuids,authority='private-local-process')
        with owned_context(driver.hold(vector,context=driver.context)):
            resolved=resolve_publication(driver,manifest['publication_id'],uuids,context=driver.context,
                supported_profiles=['ashlar-delta/0.3'],supported_revisions={key:[value]for key,value in json.loads(manifest['schema_revisions_json']).items()})
            if dict(resolved.descriptor.raw)!=manifest:raise PermissionError('Original pinned Truss manifest differs')
            with owned_context(_query_aliases(driver,manifest,enabled=maximum_rows is not None))as aliases:
                request=truss_string_compile_request(driver.source_admission,manifest,uuids,
                    driver.transport.targets[driver.tables['manifest']].uuid,sql,revision=revision,document_id=document_id,aliases=aliases)
                request_bytes=(encoded(request)+'\n').encode()
                response=compile_distribution(paths,request_bytes)
                artifact=decode_compile_artifact(response)
                if (artifact.get('status')!='compiled' or artifact.get('backend')!=BACKEND or artifact.get('bindingSha256')!=request['target']['bindingSha256']
                        or artifact.get('modelPins')!=json.loads(request['target']['bindingJson'])['modelPins']):
                    raise ValueError('Original public Weft Truss compiler correspondence differs')
                binding=json.loads(request['target']['bindingJson'])
                obligations=artifact.get('obligations')
                if (type(obligations)is not list or len(obligations)not in (2,3)
                        or {item.get('id')for item in obligations}not in ({'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity'},{'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity','ashlar.candidate.relationshipIntegrity'})):
                    raise ValueError('Exact candidate Truss compiler obligation inventory required')
                if any(type(item)is not dict or set(item)!={'id','owner','parameters','failureCode'} or item['owner']!='host' or type(item['failureCode'])is not str or not item['failureCode']or type(item['parameters'])is not dict for item in obligations):
                    raise ValueError('Exact immutable host compiler obligations required')
                publication=next(item['parameters']for item in obligations if item['id']=='ashlar.candidate.publication')
                if any(publication.get(key)!=binding[key]for key in ('publication','modelPins','layoutRevision','layoutSha256')):
                    raise ValueError('Original compiled Truss publication obligation differs')
                integrity=next(item['parameters']for item in obligations if item['id']=='ashlar.candidate.scalarIntegrity')
                if (integrity.get('phase')!='before-user-query' or integrity.get('samePublicationRequired')is not True
                        or integrity.get('noPartialPublication')is not True or type(integrity.get('checks'))is not list):
                    raise ValueError('Original candidate scalar integrity lifecycle differs')
                related=any(column.get('representation',{}).get('kind')=='relatedKeys'for column in artifact.get('columns',[]))
                filtered=any(item.get('op')=='hasRelated'for item in artifact.get('logicalPlan',{}).get('filters',[]))
                if (related or filtered) != any(item['id']=='ashlar.candidate.relationshipIntegrity'for item in obligations):
                    raise ValueError('Exact related-key guard obligation required')
                if related or filtered:
                    guard=next(item['parameters']for item in obligations if item['id']=='ashlar.candidate.relationshipIntegrity')
                    if guard.get('phase')!='before-user-query'or guard.get('samePublicationRequired')is not True or type(guard.get('checks'))is not list or not guard['checks']:
                        raise ValueError('Complete original before-query relationship guards required')
                    if filtered:
                        from .truss_relationship_filter import admit_truss_relationship_filter
                        filter_expected=admit_truss_relationship_filter(driver.source_admission,artifact,binding,driver.policy.active['expected'],revision=revision,document_id=document_id)
                    else:
                        related_expected=admit_truss_related_keys(driver.source_admission,artifact,binding,driver.policy.active['expected'],revision=revision,document_id=document_id)
                driver.validate_manifest(manifest,driver.context)
                if maximum_rows is not None:
                    count=False;value_projection=None;join_expected=None;join_projection=None
                    if any(column.get('representation',{}).get('kind')=='value'for column in artifact.get('columns',[])):
                        from .truss_value_projection import admit_truss_value_projection
                        value_projection=admit_truss_value_projection(driver.source_admission,artifact,request,manifest,driver.policy.active['expected'],revision=revision,document_id=document_id)
                    grouped=bool(artifact.get('logicalPlan',{}).get('groups'))
                    if grouped:
                        grouped_expected=admit_truss_grouped_count(driver.source_admission,artifact,binding,driver.policy.active['expected'],revision=revision,document_id=document_id)
                    elif artifact.get('logicalPlan',{}).get('joins'):
                        from .truss_join_count import admit_truss_join_count,admit_truss_join_projection
                        if artifact['logicalPlan']['aggregate']is True:
                            join_expected=admit_truss_join_count(driver.source_admission,artifact,binding,driver.policy.active['expected'],revision=revision,document_id=document_id)
                            count=True
                        else:
                            join_projection=admit_truss_join_projection(driver.source_admission,artifact,binding,driver.policy.active['expected'],revision=revision,document_id=document_id)
                    elif len(artifact.get('columns',[]))==1 and artifact['columns'][0].get('representation',{}).get('logicalType',{}).get('family')=='integer':
                        admit_truss_global_count(driver.source_admission,artifact,binding,revision=revision,document_id=document_id)
                        count=True
                    rows,checks=_execute_string_artifact(driver.transport.spark,artifact,maximum_rows,count=count,related=related,grouped=grouped,relationship_guards=filtered,value_projection=value_projection)
                    if value_projection is not None:value_projection.require_complete_bag(rows)
                    if join_projection is not None:
                        from collections import Counter
                        if Counter(encoded(row)for row in rows)!=Counter(encoded(row)for row in join_projection):raise ValueError("Independent original join projection bag differs")
                    if join_expected is not None and rows!=join_expected:raise ValueError("Independent original join count differs")
                    if filtered:
                        from collections import Counter
                        if Counter(encoded(row)for row in rows)!=Counter(encoded(row)for row in filter_expected):
                            raise ValueError('Independent complete original relationship filter bag differs')
                    if grouped:
                        from collections import Counter
                        if Counter(encoded(row)for row in rows)!=Counter(encoded(row)for row in grouped_expected):
                            raise ValueError('Independent complete original grouped count bag differs')
                    if related:
                        from collections import Counter
                        if Counter(encoded(row)for row in rows)!=Counter(encoded(row)for row in related_expected):
                            raise ValueError('Independent complete original related-key occurrence bag differs')
                    driver.validate_manifest(manifest,driver.context)
    result={'request_bytes':request_bytes,'response_bytes':response,
        'request_sha256':hashlib.sha256(request_bytes).hexdigest(),'response_sha256':hashlib.sha256(response).hexdigest(),
        'publication_id':manifest['publication_id'],'qualification':'Original accepted UMF0.7 STRING record mapping for the selected physical catalog revision; public candidate Weft compilation under native publication pins. No SQL execution or scalar-integrity discharge.'}
    if maximum_rows is not None:
        result.update(rows=rows,integrity=checks,qualification='Original accepted UMF0.7 STRING scalar/global or grouped COUNT, required STRING equality INNER JOIN COUNT/projections, authored forward/inverse HAS_RELATED or unordered authored forward/inverse RELATED_KEYS query through public candidate Weft compiler SQL after all exact integrity checks, under complete native publication pins; related cells preserve authored key order and edge occurrences. Outer source/Spark lifetime remains caller owned.')
    return result


@contextmanager
def _query_aliases(driver,manifest,*,enabled):
    if not enabled:
        yield None
        return
    spark=driver.transport.spark
    namespace='ashlar_truss_query_'+uuid.uuid4().hex
    schema='`spark_catalog`.`'+namespace+'`'
    created=[];owned=False;primary=None
    aliases={table:'spark_catalog.'+namespace+'.'+table.split('.')[-1]for table in json.loads(manifest['table_versions_json'])}
    try:
        spark.sql('CREATE DATABASE '+schema).collect();owned=True
        for table,alias in aliases.items():
            name='`'+'`.`'.join(alias.split('.'))+'`'
            target=driver.transport.targets[table]
            location=str(target.path).replace("'","''")
            spark.sql('CREATE TABLE '+name+" USING DELTA LOCATION '"+location+"'").collect();created.append(name)
            detail=spark.sql('DESCRIBE DETAIL '+name).first()
            if detail['id']!=target.uuid:raise PermissionError('Original external alias native UUID differs')
        yield aliases
    except BaseException as error:primary=error
    finish(primary,[*(lambda name=name:spark.sql('DROP TABLE '+name).collect()for name in reversed(created)),
        *([lambda:spark.sql('DROP DATABASE '+schema).collect()]if owned else [])])


def _execute_string_artifact(spark,artifact,maximum_rows,*,count=False,related=False,grouped=False,relationship_guards=False,value_projection=None):
    from .path_capture import PathCaptureConfig,capture_string_frame
    from ashlar.weft_decode import decode_exact_scalar
    columns=artifact.get('columns')
    if type(columns)is not list or not columns or len(columns)>64:
        raise ValueError('Bounded original compiler output inventory required')
    names=[]
    for position,column in enumerate(columns,1):
        representation=column.get('representation')
        integer=count or (grouped and representation.get('logicalType',{}).get('family')=='integer')
        expected={'kind':'scalar','logicalType':{'family':'integer'if integer else'string','facets':{},'nullable':False},'carrier':'text','decoder':'exact-integer'if integer else'text'}
        if related and representation.get('kind')=='relatedKeys':expected=representation
        if value_projection is not None:
            from .truss_value_projection import TrussValueProjection
            if type(value_projection)is not TrussValueProjection or encoded(column)not in [encoded(item)for item in json.loads(value_projection.columns_json)]:raise ValueError('Exact admitted value projection column required')
            expected=representation
        if column.get('position')!=position or column.get('nullable')is not False or representation!=expected or type(column.get('outputName'))is not str or not column['outputName']:
            raise ValueError('Only admitted non-null STRING scalar outputs supported')
        names.append(column['outputName'])
    if len(set(names))!=len(names):raise ValueError('Distinct original result output names required')
    params={}
    slots=artifact.get('parameters')
    if type(slots)is not list or len(slots)>256:raise ValueError('Bounded original parameter inventory required')
    for position,slot in enumerate(slots,1):
        if type(slot)is not dict or slot.get('position')!=position or type(slot.get('value'))is not str:
            raise ValueError('Exact emitted ordered STRING parameter slots required')
        params['p'+str(position)]=slot['value']
    integrity=next(item['parameters']for item in artifact['obligations']if item['id']=='ashlar.candidate.scalarIntegrity')
    if len(integrity['checks'])>128:raise ValueError('Bounded complete scalar integrity inventory required')
    checks=[]
    def capture(sql,budget):
        if type(sql)is not str or not sql or len(sql.encode())>1048576 or sql.lstrip().split(None,1)[0].upper()not in ('SELECT','WITH'):
            raise ValueError('Original bounded compiler SELECT or WITH query required')
        return capture_string_frame(spark.sql(sql,args=params),config=PathCaptureConfig(budget,65536,1048576))
    all_checks=list(integrity['checks'])
    if related or relationship_guards:
        guard=next(item['parameters']for item in artifact['obligations']if item['id']=='ashlar.candidate.relationshipIntegrity')
        if len(guard['checks'])>128:raise ValueError('Bounded relationship integrity inventory required')
        all_checks.extend(guard['checks'])
    for check in all_checks:
        if (type(check)is not dict or set(check)not in ({'sql','failureCode','record'},{'sql','failureCode','field','record'},{'sql','failureCode','relationship'})
                or check.get('failureCode')not in ('WFT-BINDING','WFT-NUMERIC-DOMAIN')):
            raise ValueError('Unknown compiler scalar integrity check meaning')
        observed=capture(check['sql'],1)
        if len(observed['schema'])!=1 or observed['rows']!=[['0']]:raise ValueError('Original scalar integrity check refused')
        checks.append({'check':check,'rows':observed['rows']})
    observed=capture(artifact['sql'],maximum_rows)
    if observed['schema']!=[[name,'STRING']for name in names]:raise ValueError('Original compiler result schema differs')
    if count and len(observed['rows'])!=1:raise ValueError('One complete global COUNT row required')
    def decode(column,value):
        if value_projection is not None:return value_projection.decode(column,value)
        if column['representation']['kind']=='relatedKeys':
            from ashlar.weft_related_keys_decode import decode_related_keys,RelatedKeysDecodeConfig
            decoded=decode_related_keys(column['representation'],value,model_pins=artifact['modelPins'],config=RelatedKeysDecodeConfig(65536))
            return {'items':[list(item)for item in decoded.items],'truncated':decoded.truncated}
        return decode_exact_scalar(column['representation'],value,count=count or (grouped and column['representation']['logicalType']['family']=='integer')).value
    rows=[{name:decode(columns[index],value)for index,(name,value)in enumerate(zip(names,row))}for row in observed['rows']]
    return rows,checks


def compile_truss_string_query(driver,paths,sql,*,revision,document_id):
    """Compile original SQL under publication pins without executing its SQL."""
    return _truss_string_query(driver,paths,sql,revision=revision,document_id=document_id)


def execute_truss_string_query(driver,paths,sql,*,revision,document_id,maximum_rows=1000):
    """Execute STRING projections, COUNT, HAS_RELATED or RELATED_KEYS while held.

    Original generated SQL is unchanged; owned external catalog aliases select
    original Delta paths, UUIDs and VERSION AS OF snapshots. Optional-state, numeric-field and other aggregate outputs refuse pending
    their separate semantic decoder admission.
    """
    return _truss_string_query(driver,paths,sql,revision=revision,document_id=document_id,maximum_rows=maximum_rows)


def admit_truss_global_count(admission,artifact,binding,*,revision,document_id):
    """Bind public global-count IR to actual accepted Truss record allocation."""
    from ashlar.weft_decode import admit_global_count_ir
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:
        raise ValueError('Actual accepted Truss count source required')
    selected=admission.revisions[revision]
    if document_id not in selected['models']:raise ValueError('Original accepted count document required')
    model,raw=selected['models'][document_id];digest=hashlib.sha256(raw).hexdigest()
    pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':model['umf']}
    identities=[{'documentId':owner[0],'revision':digest,'module':owner[1],'element':owner[2]}
        for item in selected['types'].values()for owner in (item['owner'],)if owner[0]==document_id]
    if binding['modelPins']!=[pin]:raise ValueError('Original accepted count model pin differs')
    identity=admit_global_count_ir(artifact['columns'][0],artifact,model_pin=pin,record_identities=identities)
    records=[record for record in binding['records']if record['logical']==identity]
    installation=json.loads(admission.batch.begin)['prior']['installationId']
    if len(records)!=1:raise ValueError('Exact original count physical record required')
    record=records[0];actual=selected['types'].get(record['typeId'])
    tables=binding['publication']['tables'];index=record.get('table')
    if type(index)is not int or not 0<=index<len(tables)or tables[index]['name'][-1]!='object_current':
        raise ValueError('Original global count object table required')
    if (actual is None or actual['owner']!=(document_id,identity['module'],identity['element'])
            or record['kind']!='object' or record['schemaRevision']!=revision or record['sourceSystem']!=installation):
        raise ValueError('Actual original count type/source/revision differs')
    return identity


def admit_truss_related_keys(admission,artifact,binding,expected,*,revision,document_id):
    """Independent edge-occurrence oracle for one unfiltered authored forward or inverse source scan.

    Each edge contributes its target's authored key tuple; equal tuples remain
    repeated. This does not reinterpret RELATED_KEYS as distinct target records.
    """
    selected=admission.revisions[revision];model,raw=selected['models'][document_id]
    digest=hashlib.sha256(raw).hexdigest();pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':model['umf']}
    if artifact['modelPins']!=[pin]or binding['modelPins']!=[pin]:raise ValueError('Original related-key model differs')
    plan=artifact.get('logicalPlan')
    plan_keys={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
    if (type(plan)is not dict or set(plan)!=plan_keys or plan.get('irVersion')!='weft-ir/0.2.0' or plan.get('aggregate')is not False
            or any(plan.get(key)!=[]for key in ('filters','groups','joins','order'))
            or any(plan.get(key)is not None for key in ('limit','pageKey','readProfile'))
            or plan.get('modulePins')!=[pin] or type(plan['requiredCapabilities'])is not list
            or not set(plan['requiredCapabilities'])<={'order.asc','project','relationship.boundedKeys','relationship.inverse','scan','type.string'}
            or 'relationship.boundedKeys'not in plan['requiredCapabilities']):raise ValueError('Only unfiltered unjoined related-key source supported')
    source=plan['source'];source_record=source['record']
    if set(source)!={'occurrence','pin','record'}or source.get('occurrence')!='s0'or source.get('pin')!=pin:raise ValueError('Exact original related source required')
    source_maps=[record for record in binding['records']if record['logical']==source_record]
    if len(source_maps)!=1:raise ValueError('Original related source record allocation required')
    source_map=source_maps[0];source_type=source_map['typeId']
    source_actual=selected['types'].get(source_type)
    if source_actual is None or source_actual['owner']!=(document_id,source_record['module'],source_record['element']):
        raise ValueError('Actual accepted related source identity differs')
    installation=json.loads(admission.batch.begin)['prior']['installationId']
    if source_map['sourceSystem']!=installation or source_map['schemaRevision']!=revision or source_map['kind']!='object':
        raise ValueError('Actual related source discriminator differs')
    definitions={(module['id'],element['id']):element for module in model['modules']for element in module['elements']}
    logical=lambda module,element:{'documentId':document_id,'revision':digest,'module':module,'element':element}
    def key(record,selected_key):
        declaration=definitions[record['module'],record['element']]
        choices=[item for item in declaration.get('keys',[])if item['id']==selected_key]if selected_key is not None else [item for item in declaration.get('keys',[])if item.get('primary')is True]
        if len(choices)!=1:raise ValueError('One actual authored endpoint key required')
        value=choices[0];fields=[]
        for field in value['fields']:
            declared=definitions[field['module'],field['element']]
            if declared['scalarType']!='string'or declared['nullability']!='required'or declared.get('facets',{})!={}:
                raise ValueError('Required facetless authored STRING key fields required')
            fields.append(logical(field['module'],field['element']))
        return {'id':value['id'],'fields':fields,'types':[{'family':'string','facets':{},'nullable':False}for _ in fields]}
    plans=[];inverse_used=False;columns=artifact['columns'];outputs=plan['outputs']
    if len(columns)!=len(outputs):raise ValueError('Complete original related output plan required')
    for column,output in zip(columns,outputs):
        if set(column)!={'nullable','outputName','position','representation','sourceIdentities'}or set(output)!={'name','expression'}:
            raise ValueError('Closed original related output metadata required')
        if column['outputName']!=output['name']:raise ValueError('Original related output identity differs')
        expression=output['expression'];representation=column['representation']
        if representation['kind']=='scalar':
            if set(expression)!={'op','scan','identity'}or expression.get('op')!='field'or expression.get('scan')!='s0':raise ValueError('Only original source field projections supported')
            identity=expression['identity'];homes=[field for field in source_map['properties']if field['logical']==identity]
            if len(homes)!=1 or column['sourceIdentities']!=[identity]:raise ValueError('Actual original source field home required')
            plans.append(('field',column['outputName'],homes[0]['home']['propertyId']))
            continue
        if set(expression)!={'op','scan','bound','relationship'}or representation['kind']!='relatedKeys'or expression.get('op')!='relatedKeys'or expression.get('scan')!='s0':raise ValueError('Original relatedKeys expression required')
        relation=expression['relationship'];identity=representation['relationship']
        inverse=relation.get('inverse')
        if type(inverse)is not bool:raise ValueError('Exact authored relationship direction required')
        inverse_used=inverse_used or inverse
        mappings=[item for item in binding.get('relationships',[])if item['logical']==identity]
        if len(mappings)!=1:raise ValueError('Actual original relationship mapping required')
        mapping=mappings[0];edge=selected['edges'].get(mapping['typeId'])
        if (edge is None or mapping['acceptedDefinition']!=edge['declaration'] or mapping['schemaRevision']!=revision
                or mapping['sourceSystem']!=installation or mapping['target'if inverse else'source']!=source_record
                or mapping['kind']!='edge'or edge['owner']!=(document_id,identity['module'],identity['relationship'])):
            raise ValueError('Actual accepted relationship/type/endpoints differ')
        declaration=edge['declaration']
        if inverse and (type(declaration.get('inverse'))is not str or not declaration['inverse']):
            raise ValueError('Actual authored inverse name required')
        endpoints=edge['endpoints'][::-1]if inverse else edge['endpoints'];target_record=mapping['source'if inverse else'target']
        actual_target=selected['types'].get(endpoints[1])
        if endpoints[0]!=source_type or actual_target is None or actual_target['owner']!=(document_id,target_record['module'],target_record['element']):
            raise ValueError('Actual typed relationship endpoints differ')
        source_key=key(source_record,declaration['target'if inverse else'source'][0].get('key'));target_key=key(target_record,declaration['source'if inverse else'target'][0].get('key'))
        expected_relation={'from':source_record,'to':target_record,'identity':identity,'inverse':inverse,
            'sourceKey':source_key,'targetKey':target_key,'sourceMultiplicity':declaration['sourceMultiplicity'],
            'targetMultiplicity':declaration['targetMultiplicity'],'targetLifecycle':declaration['targetLifecycle']}
        bound=representation['bound']
        if (relation!=expected_relation or type(bound)is not int or not 1<=bound<=1000 or expression.get('bound')!=bound
                or representation!={'kind':'relatedKeys','relationship':identity,'key':target_key,'bound':bound}
                or column['sourceIdentities']!=[source_record,target_record,*target_key['fields']]):
            raise ValueError('Exact original authored related key order/descriptor required')
        target_maps=[record for record in binding['records']if record['logical']==target_record]
        if len(target_maps)!=1:raise ValueError('Actual target property homes required')
        homes={encoded(field['logical']):field['home']['propertyId']for field in target_maps[0]['properties']}
        plans.append(('related',column['outputName'],mapping['typeId'],endpoints[1],tuple(homes[encoded(field)]for field in target_key['fields']),bound,inverse))
    if ('relationship.inverse'in plan['requiredCapabilities'])!=inverse_used:
        raise ValueError('Exact inverse relationship capability required')
    objects={(row['source_system'],row['type_id'],row['id']):row for row in expected['object_current']}
    answer=[]
    for (system,type_id,entity_id),row in objects.items():
        if system!=installation or type_id!=source_type or row['schema_revision']!=revision:continue
        props=json.loads(row['props_json']);result={}
        for item in plans:
            if item[0]=='field':result[item[1]]=props[item[2]];continue
            _,name,rel_type,target_type,fields,bound,inverse=item;tuples=[]
            from_role='target'if inverse else'source';to_role='source'if inverse else'target'
            for edge_row in expected['edge_current']:
                if (edge_row['source_system'],edge_row['rel_type_id'],edge_row[from_role+'_type'],edge_row[from_role+'_id'],edge_row['schema_revision'])!=(installation,rel_type,source_type,entity_id,revision):continue
                if edge_row[to_role+'_type']!=target_type:raise ValueError('Original related edge target type differs')
                target=objects[installation,target_type,edge_row[to_role+'_id']]
                if target['schema_revision']!=revision:raise ValueError('Original related target schema revision differs')
                values=json.loads(target['props_json'])
                tuples.append((tuple(values[field]for field in fields),int(edge_row['id'])))
            tuples.sort(key=lambda item:(tuple(value.encode('utf8')for value in item[0]),item[1]))
            result[name]={'items':[list(value)for value,_ in tuples[:bound]],'truncated':len(tuples)>bound}
        answer.append(result)
    return answer


def admit_truss_grouped_count(admission,artifact,binding,expected,*,revision,document_id):
    """Required STRING groups and one COUNT over the complete admitted source bag."""
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:
        raise ValueError('Actual accepted grouped count source required')
    if (artifact.get('status')!='compiled' or artifact.get('interfaceVersion')!='weft-compile/0.2.0'
            or artifact.get('dialect')!='weft-sql/0.2.0' or artifact.get('compilerVersion')!='weft/0.1.0'):
        raise ValueError('Exact original grouped compiler profile required')
    selected=admission.revisions[revision];model,raw=selected['models'][document_id]
    digest=hashlib.sha256(raw).hexdigest()
    pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':model['umf']}
    string={'family':'string','facets':{},'nullable':False};integer={'family':'integer','facets':{},'nullable':False}
    plan=artifact.get('logicalPlan');keys={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
    if (type(plan)is not dict or set(plan)!=keys or plan['irVersion']!='weft-ir/0.2.0'or plan['aggregate']is not True
            or any(plan[key]!=[]for key in ('filters','joins','order'))or any(plan[key]is not None for key in ('limit','pageKey','readProfile'))
            or plan['modulePins']!=[pin]or artifact['modelPins']!=[pin]or binding['modelPins']!=[pin]
            or plan['requiredCapabilities']!=['aggregate','aggregate.count','group','project','scan','type.string']):
        raise ValueError('Only unordered unfiltered STRING grouped COUNT supported')
    source=plan['source'];identity=source.get('record')
    if set(source)!={'occurrence','pin','record'}or source['occurrence']!='s0'or source['pin']!=pin:
        raise ValueError('Exact grouped source required')
    maps=[item for item in binding['records']if item['logical']==identity]
    if len(maps)!=1:raise ValueError('Exact grouped source allocation required')
    record=maps[0];actual=selected['types'].get(record['typeId']);installation=json.loads(admission.batch.begin)['prior']['installationId']
    index=record['table'];tables=binding['publication']['tables']
    if (actual is None or actual['owner']!=(document_id,identity['module'],identity['element'])or record['kind']!='object'
            or record['sourceSystem']!=installation or record['schemaRevision']!=revision or type(index)is not int
            or not 0<=index<len(tables)or tables[index]['name'][-1]!='object_current'):
        raise ValueError('Actual grouped physical record differs')
    definitions={(module['id'],element['id']):element for module in model['modules']for element in module['elements']}
    groups=plan['groups'];fields=[];homes=[]
    if type(groups)is not list or not 1<=len(groups)<=32:raise ValueError('Bounded original grouping fields required')
    for group in groups:
        if (type(group)is not dict or set(group)!={'identity','scan','span','type'}or group['scan']!='s0'or group['type']!=string
                or type(group['span'])is not dict or set(group['span'])!={'start','end'}
                or any(type(group['span'][key])is not int or group['span'][key]<0 for key in ('start','end'))or group['span']['end']<group['span']['start']):
            raise ValueError('Exact typed grouping field required')
        field=group['identity'];properties=[item for item in record['properties']if item['logical']==field]
        if len(properties)!=1 or field in fields:raise ValueError('Unique original grouping property home required')
        declaration=definitions.get((field['module'],field['element']))
        if (field.get('documentId')!=document_id or field.get('revision')!=digest or declaration is None
                or declaration.get('scalarType')!='string'or declaration.get('nullability')!='required'or declaration.get('facets',{})!={}):
            raise ValueError('Actual required STRING grouping field differs')
        allocations=[item for item in json.loads(selected['report'])['properties']
            if item['typeId']==record['typeId'] and item['moduleId']==field['module'] and item['elementId']==field['element']]
        if (len(allocations)!=1 or allocations[0]['propertyId'] not in actual['fields']
                or properties[0]['home']!={'kind':'props','propertyId':allocations[0]['propertyId']}):
            raise ValueError('Actual accepted grouping property home differs')
        fields.append(field);homes.append(properties[0]['home']['propertyId'])
    if plan['typeGraph']!=[{'availability':'required','identity':field,'kind':'scalar','type':string}for field in fields]:
        raise ValueError('Complete grouped type graph differs')
    outputs=plan['outputs'];columns=artifact['columns'];projected=[];count_name=None;names=[]
    if len(outputs)!=len(fields)+1 or len(columns)!=len(outputs):raise ValueError('Complete grouped outputs required')
    for position,(output,column)in enumerate(zip(outputs,columns),1):
        if (set(output)!={'name','expression'}or set(column)!={'nullable','outputName','position','representation','sourceIdentities'}
                or column['position']!=position or column['nullable']is not False or column['outputName']!=output['name']
                or type(output['name'])is not str or not output['name']or output['name']in names):raise ValueError('Exact grouped output metadata required')
        names.append(output['name']);expression=output['expression']
        if expression=={'op':'count','type':integer}:
            if count_name is not None or column['sourceIdentities']!=[identity]:raise ValueError('One original COUNT output required')
            count_name=output['name'];logical=integer;decoder='exact-integer'
        else:
            if set(expression)!={'op','scan','identity'}or expression['op']!='field'or expression['scan']!='s0'or expression['identity']not in fields:
                raise ValueError('Only original grouping field projections supported')
            field=expression['identity']
            if field in [item[0]for item in projected]or column['sourceIdentities']!=[field]:raise ValueError('Unique grouped projection identity required')
            projected.append((field,output['name']));logical=string;decoder='text'
        if column['representation']!={'kind':'scalar','logicalType':logical,'carrier':'text','decoder':decoder}:
            raise ValueError('Exact grouped scalar descriptor required')
    if count_name is None or len(projected)!=len(fields):raise ValueError('Complete grouping projections and COUNT required')
    from collections import Counter
    bag=Counter()
    for row in expected['object_current']:
        if (row['source_system'],row['type_id'],row['schema_revision'])!=(installation,record['typeId'],revision):continue
        props=json.loads(row['props_json']);values=tuple(props[home]for home in homes)
        if any(type(value)is not str for value in values):raise ValueError('Original grouped source STRING value required')
        bag[values]+=1
    return [{**{name:values[fields.index(field)]for field,name in projected},count_name:count}for values,count in bag.items()]
