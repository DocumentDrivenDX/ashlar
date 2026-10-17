"""Independent configured-source result bags for an explicit Spark041 subset.

Compiler IR selects admitted logical fields; values come only from the original
source-prefix carrier oracle. No SQL parsing, native cells or write plan is used.
"""
import hashlib,json
from ashlar.schema import decode_retained_json
from ashlar.weft_binding import LAYOUT_SHA256
from .configured_local_read import ConfiguredLocalRead
from .configured_weft_binding import configured_count_star_request
from .count_star_admission import BACKEND
from .delta_custody import encoded

STRING={'family':'string','facets':{},'nullable':False}
INTEGER={'family':'integer','facets':{},'nullable':False}
_IR={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}

def _require(value):
    if not value:raise ValueError('Configured projection oracle subset refused')


def configured_projection_oracle(scope,request,artifact):
    """Return the original-oracle port for required STRING fields or COUNT(*).

    This is an independent expected-result projection, not artifact admission,
    actual source authority or permission to execute any compiler obligation.
    """
    _require(type(scope)is ConfiguredLocalRead and type(request)is dict and type(artifact)is dict)
    scope.renew();scope.parity()
    binding=decode_retained_json(request['target']['bindingJson'].encode())
    original_tables=sorted(scope.descriptor.versions);physical=binding['publication']['tables']
    _require(type(physical)is list and len(physical)==len(original_tables)==4)
    aliases={table:'.'.join(selected['name'])for table,selected in zip(original_tables,physical)}
    _require(configured_count_star_request(scope,aliases,request['sql'],layout_sha256=LAYOUT_SHA256)==request)
    _require(artifact.get('status')=='compiled'and artifact.get('backend')==dict(BACKEND)and artifact.get('interfaceVersion')=='weft-compile/0.4.1'and artifact.get('dialect')=='weft-sql/0.4.1'and artifact.get('bindingSha256')==request['target']['bindingSha256'])
    pins=[m['pin']for m in request['modules']];_require(artifact.get('modelPins')==pins)
    ir=artifact.get('logicalPlan');_require(type(ir)is dict and set(ir)==_IR and ir['irVersion']=='weft-ir/0.4.1'and ir['modulePins']==pins)
    _require(all(ir[name]==[]for name in ('filters','joins','groups','order'))and all(ir[name]is None for name in ('limit','pageKey','readProfile'))and type(ir['aggregate'])is bool)
    scan=ir['source'];_require(type(scan)is dict and set(scan)=={'occurrence','pin','record'}and type(scan['occurrence'])is str and bool(scan['occurrence'])and scan['pin']in pins)
    logical=scan['record'];configured=scope.source.configuration;policy=configured.policy
    records=[r for r in binding['records']if r['logical']==logical];_require(len(records)==1);record=records[0]
    type_ids=[tid for tid,identity in policy.record_identities.items()if logical=={'documentId':configured.intake.document_id,'revision':configured.intake.document_revision,'module':identity[0],'element':identity[1]}]
    _require(len(type_ids)==1);type_id=type_ids[0]
    _require(record['kind']=='object'and record['typeId']==str(type_id)and record['sourceSystem']==policy.source_system and record['schemaRevision']==configured.intake.document_revision)
    outputs=ir['outputs'];columns=artifact.get('columns');_require(type(outputs)is list and 1<=len(outputs)<=128 and type(columns)is list and len(outputs)==len(columns))
    count=ir['aggregate'];properties=[];used={}
    for position,(output,column)in enumerate(zip(outputs,columns),1):
        _require(type(output)is dict and set(output)=={'expression','name'}and type(output['name'])is str and bool(output['name']))
        expression=output['expression'];_require(type(expression)is dict)
        if count:
            _require(len(outputs)==1 and expression=={'op':'count','type':INTEGER})
            representation={'carrier':'text','decoder':'exact-integer','kind':'scalar','logicalType':INTEGER};identities=[logical]
        else:
            _require(set(expression)=={'op','scan','identity'}and expression['op']=='field'and expression['scan']==scan['occurrence'])
            field=expression['identity'];homes=[p for p in record['properties']if p['logical']==field];_require(len(homes)==1)
            home=homes[0]['home'];_require(type(home)is dict and set(home)=={'kind','propertyId'}and home['kind']=='props')
            pid=home['propertyId'];_require(policy.types[type_id].get(pid)=='required')
            identity=policy.property_fields[type_id].get(pid)
            _require(identity is not None and field=={'documentId':configured.intake.document_id,'revision':configured.intake.document_revision,'module':identity[0],'element':identity[1]})
            properties.append(pid);used[encoded(field)]={'availability':'required','identity':field,'kind':'scalar','type':STRING}
            representation={'carrier':'text','decoder':'text','kind':'scalar','logicalType':STRING};identities=[field]
        _require(column=={'nullable':False,'outputName':output['name'],'position':position,'representation':representation,'sourceIdentities':identities})
    _require(type(ir['typeGraph'])is list and sorted(map(encoded,ir['typeGraph']))==sorted(map(encoded,used.values())))
    capabilities=['aggregate','aggregate.count','project','scan']if count else['project','scan','type.string']
    _require(type(ir['requiredCapabilities'])is list and sorted(ir['requiredCapabilities'])==capabilities)
    original_request=encoded(request);original_artifact=encoded(artifact)
    model=configured.intake.source;rawsource=scope.source.original_source;original_rows=encoded(scope.source_rows())
    def oracle(supplied_model,supplied_source,supplied_request):
        scope.renew();scope.parity()
        _require(type(supplied_model)is bytes and supplied_model==model and type(supplied_source)is bytes and supplied_source==rawsource and type(supplied_request)is dict and encoded(supplied_request)==original_request)
        _require(encoded(request)==original_request and encoded(artifact)==original_artifact and configured.intake.source==model and scope.source.original_source==rawsource)
        _require(configured_count_star_request(scope,aliases,request['sql'],layout_sha256=LAYOUT_SHA256)==request)
        observed=scope.source_rows();_require(encoded(observed)==original_rows)
        rows=[row for row in observed['object_current']if row['source_system']==policy.source_system and row['type_id']==str(type_id)]
        _require(len(rows)<=1000 and all(row['schema_revision']==configured.intake.document_revision for row in rows)and len(rows)*len(outputs)<=100000)
        if count:answer=[[str(len(rows))]]
        else:
            answer=[]
            for row in rows:
                values=decode_retained_json(row['props_json'].encode());_require(type(values)is dict and all(type(values.get(pid))is str for pid in properties))
                answer.append([values[pid]for pid in properties])
        scope.renew()
        return {'rows':answer,'witnesses':{'source_records':len(rows),'result_occurrences':len(answer),'publication_id':scope.descriptor.publication_id,'source_sha256':hashlib.sha256(rawsource).hexdigest()},'scope':'Complete original configured STRING source-prefix bag; positional required fields or global COUNT only. No native/source/publication authority.'}
    return oracle
