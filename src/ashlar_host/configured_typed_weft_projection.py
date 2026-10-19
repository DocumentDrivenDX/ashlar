# SPDX-License-Identifier: MIT OR Apache-2.0
"""Independent exact carriers for configured typed source projections.

Original numeric tokens are retained separately from compiler/native output
carriers. These pure expectations grant no schema, source or native authority.
"""
from ashlar.typed_source_policy import NumericToken
from ashlar.weft_decode import decode_exact_scalar
from .truss_indexed_commerce_oracle import number_parts


class TypedProjectionCapacityError(ValueError):
    """Selected exact output codec capacity, not a logical UMF domain claim."""


def _require(value):
    if not value:raise ValueError('Configured typed projection refused')


def typed_projection_carrier(value,representation):
    """Project a retained source atom to one explicitly selected scalar carrier.

    The caller must independently bind the original Field and compiler metadata.
    NumericToken.text is never changed. Decimal output follows the explicit fixed
    scale; native decimal zero has no sign, while the source token retains it.
    """
    _require(type(representation)is dict and set(representation)=={'kind','logicalType','carrier','decoder'}and representation['kind']=='scalar')
    logical=representation['logicalType']
    _require(type(logical)is dict and set(logical)=={'family','facets','nullable'}and logical['nullable']is False and type(logical['facets'])is dict)
    family=logical['family'];facets=logical['facets']
    if family=='string':
        _require(type(value)is str);candidate=value
    elif family=='boolean':
        _require(type(value)is bool)
        candidate=value if representation['carrier']=='boolean'else('true'if value else'false')
    elif family in ('integer','decimal'):
        _require(type(value)is NumericToken)
        sign,digits,exponent,_=number_parts(value.text)
        if exponent is None:raise TypedProjectionCapacityError('Numeric exponent exceeds selected projection capacity')
        if family=='integer':
            _require(exponent>=0)
            if len(digits)+exponent>20:raise TypedProjectionCapacityError('Integer exceeds selected exact output codec capacity')
            candidate=('-'if sign<0 else'')+digits+'0'*exponent
        else:
            if (set(facets)!={'precision','scale'}or type(facets['precision'])is not int or type(facets['scale'])is not int
                    or not 1<=facets['precision']<=28 or not 0<=facets['scale']<=facets['precision']):
                raise TypedProjectionCapacityError('Decimal facets exceed selected exact output codec capacity')
            scale=facets['scale'];shift=exponent+scale
            _require(shift>=0)
            if sign and len(digits)+shift>facets['precision']:raise TypedProjectionCapacityError('Decimal exceeds selected fixed output carrier capacity')
            coefficient=digits+'0'*shift
            if scale:
                coefficient=coefficient.rjust(scale+1,'0');candidate=coefficient[:-scale]+'.'+coefficient[-scale:]
            else:candidate=coefficient
            if sign<0:candidate='-'+candidate
    else:raise ValueError('Unselected configured scalar family')
    decode_exact_scalar(representation,candidate)
    return candidate


# These shapes are the original installed041 direct-field and COUNT IR, including
# its explicit optional native-null projection. Artifact admission remains owned
# by the existing public schema/obligation checker, separately from this oracle.
_IR={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
_INTEGER={'family':'integer','facets':{},'nullable':False}


def configured_typed_projection_oracle(scope,request,artifact):
    """Build an invocation-bound independent original typed source-prefix bag.

    Required direct scalars, optional facetless STRING native-null envelopes and
    global COUNT and one exact required INTEGER equality literal are supported. Original absence and null remain distinct in
    expected envelopes; compiler guard admission is a separate prerequisite.
    No SQL, native result cells or publisher effect plan contributes expectations.
    """
    import hashlib,json
    from ashlar.schema import decode_retained_json
    from ashlar.typed_source_config import TypedJsonlConfiguration
    from ashlar.weft_binding import LAYOUT_SHA256
    from .configured_local_read import ConfiguredLocalRead
    from .configured_typed_weft_binding import configured_typed_count_star_request
    from .count_star_admission import BACKEND
    from .delta_custody import encoded
    from .truss_scalar_projection import decode_truss_lexical_properties
    _require(type(scope)is ConfiguredLocalRead and scope.source.typed is True and type(request)is dict and type(artifact)is dict)
    scope.renew();scope.parity();configuration=scope.source.configuration
    _require(type(configuration)is TypedJsonlConfiguration and configuration.policy is not None)
    configuration.verify();model=configuration.model_source;rawsource=scope.source.original_source
    binding=decode_retained_json(request['target']['bindingJson'].encode())
    names=sorted(scope.descriptor.versions);tables=binding['publication']['tables']
    _require(type(tables)is list and len(tables)==len(names)==4)
    aliases={name:'.'.join(table['name'])for name,table in zip(names,tables)}
    def rebuilt():
        return configured_typed_count_star_request(configuration,dict(scope.descriptor.raw),scope.vector,scope.manifest_uuid,aliases,request['sql'],layout_sha256=LAYOUT_SHA256)
    _require(rebuilt()==request)
    pins=[m['pin']for m in request['modules']]
    _require(artifact.get('status')=='compiled'and artifact.get('backend')==dict(BACKEND)and artifact.get('interfaceVersion')=='weft-compile/0.4.1'and artifact.get('dialect')=='weft-sql/0.4.1'and artifact.get('modelPins')==pins and artifact.get('bindingSha256')==request['target']['bindingSha256'])
    plan=artifact.get('logicalPlan')
    _require(type(plan)is dict and set(plan)==_IR and plan['irVersion']=='weft-ir/0.4.1'and plan['modulePins']==pins and type(plan['aggregate'])is bool)
    _require(all(plan[k]==[]for k in ('groups','joins','order'))and all(plan[k]is None for k in ('limit','pageKey','readProfile')))
    scan=plan['source'];_require(type(scan)is dict and set(scan)=={'occurrence','pin','record'}and type(scan['occurrence'])is str and bool(scan['occurrence'])and scan['pin']in pins)
    records=[r for r in binding['records']if r['logical']==scan['record']];_require(len(records)==1);record=records[0]
    document=decode_retained_json(model);elements={(m['id'],e['id']):e for m in document['modules']for e in m['elements']}
    def logical(module,element):return {'documentId':document['id'],'revision':configuration.document_revision,'module':module,'element':element}
    choices=[tid for tid,b in configuration.bindings.items()if logical(*b['record'])==scan['record']]
    _require(len(choices)==1);tid=choices[0];bound=configuration.bindings[tid]
    _require(record['kind']=='object'and record['typeId']==str(tid)and record['sourceSystem']==configuration.source_system and record['schemaRevision']==configuration.document_revision)
    outputs=plan['outputs'];columns=artifact.get('columns');count=plan['aggregate']
    _require(type(outputs)is list and 1<=len(outputs)<=128 and type(columns)is list and len(columns)==len(outputs))
    expected_graph={};fields=[];capabilities={'aggregate','aggregate.count','project','scan'}if count else{'project','scan'};output_names=set()
    for position,(output,column)in enumerate(zip(outputs,columns),1):
        _require(type(output)is dict and set(output)=={'expression','name'}and type(output['name'])is str and bool(output['name'])and output['name']not in output_names);output_names.add(output['name'])
        expression=output['expression'];_require(type(expression)is dict)
        if count:
            _require(len(outputs)==1 and expression=={'op':'count','type':_INTEGER})
            representation={'kind':'scalar','logicalType':_INTEGER,'carrier':'text','decoder':'exact-integer'};identity=scan['record']
        else:
            _require(set(expression)=={'op','identity','scan'}and expression['op']=='field'and expression['scan']==scan['occurrence'])
            identity=expression['identity'];homes=[p for p in record['properties']if p['logical']==identity];_require(len(homes)==1)
            home=homes[0]['home'];pid=home['propertyId'];_require(pid in bound['fields'])
            module,element,family=bound['fields'][pid];_require(identity==logical(module,element));field=elements[module,element]
            _require(field['kind']=='field'and field['scalarType']==family and field.get('cardinality')=='one'and field.get('nullability')in ('required','absent-allowed'))
            optional=field['nullability']=='absent-allowed';facets=field.get('facets',{});_require(type(facets)is dict)
            scalar={'family':family,'facets':facets,'nullable':False}
            scalar_rep={'kind':'scalar','logicalType':scalar,'carrier':'text','decoder':{'integer':'exact-integer','decimal':'exact-decimal','boolean':'boolean','string':'text'}[family]}
            expected_home={'kind':'props','propertyId':pid}
            if optional:
                _require(family=='string'and facets=={});expected_home['encoding']='ashlar-weft-json-native-null/0.1-candidate'
                representation={'descriptor':identity,'kind':'value','nativeNull':True};capabilities.update(('value.presence','value.nativeNull'))
            else:representation=scalar_rep
            _require(home==expected_home)
            expected_graph[encoded(identity)]={'availability':field['nullability'],'identity':identity,'kind':'scalar','type':scalar}
            fields.append((pid,scalar_rep,optional));capabilities.add('type.'+family)
        _require(type(column)is dict and type(column.get('position'))is int and column=={'nullable':False,'outputName':output['name'],'position':position,'representation':representation,'sourceIdentities':[identity]})
    integer_filter=None
    _require(type(plan['filters'])is list and len(plan['filters'])<=1)
    if plan['filters']:
        _require(not count)
        wrapper=plan['filters'][0];_require(type(wrapper)is dict and set(wrapper)=={'op','predicate'}and wrapper['op']=='legacy')
        predicate=wrapper['predicate'];_require(type(predicate)is dict and set(predicate)=={'left','op','right'}and predicate['op']=='equal')
        left,right=predicate['left'],predicate['right']
        _require(type(left)is dict and set(left)=={'identity','scan','span','type'}and left['scan']==scan['occurrence'])
        identity=left['identity'];homes=[p['home']for p in record['properties']if p['logical']==identity];_require(len(homes)==1)
        home=homes[0];_require(type(home)is dict and set(home)=={'kind','propertyId'}and home['kind']=='props'and home['propertyId']in bound['fields'])
        pid=home['propertyId'];module,element,family=bound['fields'][pid];definition=elements[module,element]
        _require(identity==logical(module,element)and family=='integer'and definition.get('scalarType')=='integer'and definition.get('cardinality')=='one'and definition.get('nullability')=='required')
        facets=definition.get('facets',{});_require(type(facets)is dict and set(facets)=={'integerWidth'})
        width=facets['integerWidth'];_require(type(width)is dict and set(width)=={'bits','signed'}and type(width['bits'])is int and 1<=width['bits']<=64 and type(width['signed'])is bool)
        integer={'family':'integer','facets':facets,'nullable':False}
        _require(left['type']==integer and type(right)is dict and set(right)=={'kind','span','type','value'}and right['kind']=='literal'and right['type']==integer and type(right['value'])is str and 0<len(right['value'])<=20)
        import re
        value=right['value'];_require(re.fullmatch('-?(0|[1-9][0-9]*)',value)is not None and value!='-0')
        representation={'kind':'scalar','logicalType':integer,'carrier':'text','decoder':'exact-integer'}
        decode_exact_scalar(representation,value)
        sql=request['sql'].encode()
        def original_span(span):
            _require(type(span)is dict and set(span)=={'start','end'}and all(type(span[k])is int for k in span)and 0<=span['start']<span['end']<=len(sql))
            return sql[span['start']:span['end']].decode('utf-8')
        _require(original_span(right['span'])==value)
        selected=original_span(left['span']);name=definition.get('name')
        _require(type(name)is str and bool(name))
        quoted='"'+name.replace('"','""')+'"'
        # Check the original field token without deriving query semantics from
        # SQL: the independently compiled scan/identity owns its resolution.
        reference=re.fullmatch(r'(?:[A-Za-z_][A-Za-z0-9_]*|"(?:[^"]|"")+")\.(.+)',selected)
        field_token=selected if reference is None else reference.group(1)
        _require(field_token==quoted or field_token.isascii()and name.isascii()and field_token.lower()==name.lower())
        expected_graph[encoded(identity)]={'availability':'required','identity':identity,'kind':'scalar','type':integer}
        capabilities.update(('equal','filter','type.integer'));integer_filter=(pid,representation,value)
    _require(type(plan['typeGraph'])is list and sorted(map(encoded,plan['typeGraph']))==sorted(map(encoded,expected_graph.values())))
    _require(type(plan['requiredCapabilities'])is list and sorted(plan['requiredCapabilities'])==sorted(capabilities))
    original_request=encoded(request);original_artifact=encoded(artifact);original_rows=encoded(scope.source_rows())
    def oracle(supplied_model,supplied_source,supplied_request):
        scope.renew();scope.parity();configuration.verify()
        _require(type(supplied_model)is bytes and supplied_model==model and type(supplied_source)is bytes and supplied_source==rawsource and type(supplied_request)is dict and encoded(supplied_request)==original_request)
        _require(configuration.model_source==model and scope.source.original_source==rawsource and encoded(request)==original_request and encoded(artifact)==original_artifact and rebuilt()==request)
        observed=scope.source_rows();_require(encoded(observed)==original_rows)
        rows=[r for r in observed['object_current']if r['source_system']==configuration.source_system and r['type_id']==str(tid)]
        _require(len(rows)<=1000 and len(rows)*len(outputs)<=100000 and all(r['schema_revision']==configuration.document_revision for r in rows))
        answer=[];witnesses=[];matched=0
        for row in rows:
            props=decode_truss_lexical_properties(row['props_json']);values=[];atoms=[]
            for pid,representation,optional in fields:
                if optional and(pid not in props or props[pid]is None):
                    state='absent'if pid not in props else'null'
                    values.append(json.dumps({'state':state},separators=(',',':')));atoms.append({'property_id':pid,'state':state});continue
                _require(pid in props and props[pid]is not None);value=props[pid]
                carrier=typed_projection_carrier(value,representation)
                values.append(json.dumps({'state':'value','value':carrier},ensure_ascii=False,separators=(',',':'))if optional else carrier)
                atom={'property_id':pid,'state':'value'}
                if type(value)is NumericToken:atom['numeric_token']=value.text
                atoms.append(atom)
            match=True
            if integer_filter is not None:
                pid,representation,literal=integer_filter
                _require(pid in props and type(props[pid])is NumericToken)
                match=typed_projection_carrier(props[pid],representation)==literal
            if match:
                matched+=1
                if not count:answer.append(values)
            witnesses.append({'source_system':row['source_system'],'type_id':row['type_id'],'id':row['id'],'schema_revision':row['schema_revision'],'props_json':row['props_json'],'fields':atoms})
        if count:answer=[[str(matched)]]
        scope.renew();configuration.verify()
        return {'rows':answer,'witnesses':{'source_records':len(rows),'matched_records':matched,'result_occurrences':len(answer),'publication_id':scope.descriptor.publication_id,'source_sha256':hashlib.sha256(rawsource).hexdigest(),'original_atoms':witnesses},
                'scope':'Complete independent original typed source-prefix projection/count bag. Original absent/null/value states and lexical numeric tokens remain distinct; compiler guard admission is independent. No native/source/publication authority.'}
    return oracle
