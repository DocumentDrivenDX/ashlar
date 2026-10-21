# SPDX-License-Identifier: MIT OR Apache-2.0
"""Independent original typed-graph prefix bags; no SQL or native-result oracle.

This closed subset supports required facetless STRING projections, global count
forward one-hop related-key collections and exactly-two-hop root bags/counts
with required STRING grouping and distinct authored terminal-key tuples.
Broader IR requires separate
admission; public artifact/guard admission remains a distinct prerequisite.
"""
import hashlib,json,re
from ashlar.schema import decode_retained_json
from ashlar.typed_graph_source_config import TypedGraphJsonlConfiguration
from ashlar.typed_graph_source import TypedGraphSourcePolicy
from ashlar.weft_binding import LAYOUT_SHA256
from .configured_local_read import ConfiguredLocalRead
from .configured_typed_graph_weft_binding import configured_typed_graph_count_star_request
from .count_star_admission import BACKEND,CURRENT_BACKEND
from .delta_custody import encoded

STRING={'family':'string','facets':{},'nullable':False}
INTEGER={'family':'integer','facets':{},'nullable':False}
_IR={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}


def _require(value):
    if not value:raise ValueError('Configured typed graph projection subset refused')


def configured_typed_graph_projection_oracle(scope,request,artifact):
    """Return a renewable independent expected-bag port for an admitted artifact."""
    _require(type(scope)is ConfiguredLocalRead and type(request)is dict and type(artifact)is dict)
    scope.renew();scope.parity();configuration=scope.source.configuration
    _require(type(configuration)is TypedGraphJsonlConfiguration and type(configuration.policy)is TypedGraphSourcePolicy)
    configuration.verify();model=configuration.model_source;source=scope.source.original_source
    binding=decode_retained_json(request['target']['bindingJson'].encode());names=sorted(scope.descriptor.versions)
    tables=binding['publication']['tables'];_require(type(tables)is list and len(tables)==len(names)==4)
    aliases={name:'.'.join(table['name'])for name,table in zip(names,tables)}
    def rebuilt():return configured_typed_graph_count_star_request(configuration,dict(scope.descriptor.raw),scope.vector,scope.manifest_uuid,aliases,request['sql'],layout_sha256=LAYOUT_SHA256)
    _require(rebuilt()==request)
    pins=[m['pin']for m in request['modules']]
    request_target=request.get('target',{})
    current=all(request_target.get(k)==CURRENT_BACKEND[k]for k in ('backendId','backendVersion','targetProfile'))
    expected_backend=dict(CURRENT_BACKEND if current else BACKEND)
    _require(artifact.get('status')=='compiled'and artifact.get('backend')==expected_backend and artifact.get('interfaceVersion')=='weft-compile/0.4.1'and artifact.get('dialect')=='weft-sql/0.4.1'and artifact.get('modelPins')==pins and artifact.get('bindingSha256')==request['target']['bindingSha256'])
    plan=artifact.get('logicalPlan')
    allowed_ir={frozenset(_IR),frozenset(_IR|{'pathExpansion'})}
    if current:allowed_ir|={frozenset(_IR|{'having'}),frozenset(_IR|{'pathExpansion','having'})}
    _require(type(plan)is dict and frozenset(plan)in allowed_ir and plan['irVersion']=='weft-ir/0.4.1'and plan['modulePins']==pins and type(plan['aggregate'])is bool)
    _require(all(plan[k]==[]for k in ('filters','joins'))and all(plan[k]is None for k in ('limit','pageKey','readProfile')))
    scan=plan['source'];_require(type(scan)is dict and set(scan)=={'occurrence','pin','record'}and type(scan['occurrence'])is str and bool(scan['occurrence'])and scan['pin']in pins)
    records=[r for r in binding['records']if r['logical']==scan['record']];_require(len(records)==1);record=records[0]
    tid=record['typeId'];_require(tid in configuration.bindings and record['kind']=='object'and record['sourceSystem']==configuration.source_system and record['schemaRevision']==configuration.document_revision)
    document=decode_retained_json(model);elements={(m['id'],e['id']):e for m in document['modules']for e in m['elements']}
    def logical(module,element):return {'documentId':document['id'],'revision':configuration.document_revision,'module':module,'element':element}
    def field_home(mapping,identity):
        homes=[p['home']for p in mapping['properties']if p['logical']==identity];_require(len(homes)==1)
        home=homes[0];_require(type(home)is dict and set(home)=={'kind','propertyId'}and home['kind']=='props')
        selected=configuration.bindings[mapping['typeId']]['fields'].get(home['propertyId']);_require(selected is not None)
        module,element,family=selected;field=elements[module,element]
        _require(identity==logical(module,element)and family=='string'and field['scalarType']=='string'and field['nullability']=='required'and field.get('facets',{})=={}and field.get('cardinality')=='one')
        return home['propertyId']
    def key(mapping,selected):
        owner=elements[mapping['logical']['module'],mapping['logical']['element']]
        choices=[k for k in owner.get('keys',[])if k.get('id')==selected]if selected is not None else[k for k in owner.get('keys',[])if k.get('primary')is True]
        _require(len(choices)==1 and type(choices[0].get('fields'))is list and 1<=len(choices[0]['fields'])<=16)
        fields=[logical(f['module'],f['element'])for f in choices[0]['fields']]
        homes=tuple(field_home(mapping,f)for f in fields)
        return {'id':choices[0]['id'],'fields':fields,'types':[STRING for _ in fields]},homes
    def relation(selected,origin):
        _require(type(selected)is dict and selected.get('inverse')is False)
        matches=[m for m in binding['relationships']if m['logical']==selected.get('identity')];_require(len(matches)==1)
        mapping=matches[0];declaration=mapping['acceptedDefinition']
        _require(mapping['source']==origin and mapping['kind']=='edge'and declaration.get('directed')is True and len(declaration['source'])==len(declaration['target'])==1)
        origins=[r for r in binding['records']if r['logical']==origin];targets=[r for r in binding['records']if r['logical']==mapping['target']]
        _require(len(origins)==len(targets)==1)
        source_key,_=key(origins[0],declaration['source'][0].get('key'));target_key,homes=key(targets[0],declaration['target'][0].get('key'))
        expected={'from':origin,'to':mapping['target'],'identity':mapping['logical'],'inverse':False,'sourceKey':source_key,'targetKey':target_key,
            'sourceMultiplicity':declaration['sourceMultiplicity'],'targetMultiplicity':declaration['targetMultiplicity'],'targetLifecycle':declaration['targetLifecycle']}
        _require(selected==expected)
        return mapping,origins[0]['typeId'],targets[0]['typeId'],homes,target_key
    expansion=plan.get('pathExpansion');hops=[]
    if expansion is not None:
        _require(type(expansion)is dict and set(expansion)=={'occurrence','path'}and type(expansion['occurrence'])is str and bool(expansion['occurrence']))
        path=expansion['path'];_require(type(path)is dict and set(path)=={'hopSpans','hops','span','startScan'}and path['startScan']==scan['occurrence']and type(path['hops'])is list and len(path['hops'])==2 and type(path['hopSpans'])is list and len(path['hopSpans'])==2)
        for span in [path['span'],*path['hopSpans']]:
            _require(type(span)is dict and set(span)=={'start','end'}and type(span['start'])is int and type(span['end'])is int and 0<=span['start']<=span['end']<=65536)
        origin=scan['record']
        for selected in path['hops']:
            hop=relation(selected,origin);hops.append(hop);origin=hop[0]['target']
    outputs=plan['outputs'];columns=artifact.get('columns');_require(type(outputs)is list and 1<=len(outputs)<=128 and type(columns)is list and len(outputs)==len(columns))
    count=plan['aggregate'];plans=[];type_graph={};capabilities={'project','scan'};output_names=set()
    if hops:capabilities.update(('relationship.pathExpansion','relationship.twoHopPaths'))
    groups=plan['groups'];_require(type(groups)is list and len(groups)<=16 and (not groups or count and hops))
    group_homes=[];group_identities=set()
    for group in groups:
        _require(type(group)is dict and set(group)=={'scan','identity','type','span'}and group['scan']==scan['occurrence']and group['type']==STRING)
        span=group['span'];_require(type(span)is dict and set(span)=={'start','end'}and all(type(span[k])is int for k in span)and 0<=span['start']<=span['end']<=len(request['sql'].encode()))
        identity=group['identity'];marker=encoded(identity);_require(marker not in group_identities);group_identities.add(marker)
        group_homes.append(field_home(record,identity));type_graph[marker]={'availability':'required','identity':identity,'kind':'scalar','type':STRING}
    if groups:capabilities.update(('group','type.string'))
    if count:capabilities.add('aggregate')
    order=plan['order'];_require(type(order)is list and len(order)<=16)
    ordered_identities=[]
    for item in order:
        _require(type(item)is dict and set(item)=={'identity','scan','span','type'}
            and item['scan']==scan['occurrence']and item['type']==STRING and encoded(item['identity'])in group_identities)
        span=item['span'];_require(type(span)is dict and set(span)=={'start','end'}and type(span['start'])is int and type(span['end'])is int
            and 0<=span['start']<=span['end']<=len(request['sql'].encode()))
        ordered_identities.append(encoded(item['identity']))
    _require(len(set(ordered_identities))==len(ordered_identities))
    having_threshold=None
    if current:
        having=plan['having'];_require(type(having)is list and len(having)<=1)
        if having:
            _require(count and bool(groups)and len(having)==1 and type(having[0])is dict and set(having[0])=={'count','threshold'}
                and having[0]['count']=={'op':'count','type':INTEGER})
            threshold=having[0]['threshold'];_require(type(threshold)is dict and set(threshold)=={'kind','span','type','value'}
                and threshold['kind']=='literal'and threshold['type']==INTEGER and type(threshold['value'])is str
                and re.fullmatch('0|[1-9][0-9]{0,18}',threshold['value'])is not None)
            threshold_span=threshold['span'];sql_bytes=request['sql'].encode('utf-8')
            _require(type(threshold_span)is dict and set(threshold_span)=={'start','end'}and type(threshold_span['start'])is int
                and type(threshold_span['end'])is int and 0<=threshold_span['start']<threshold_span['end']<=len(sql_bytes)
                and sql_bytes[threshold_span['start']:threshold_span['end']].decode('utf-8')==threshold['value'])
            having_threshold=int(threshold['value']);_require(having_threshold<2**63)
            capabilities.add('aggregate.havingCountStarGreater')
        if order:
            _require(ordered_identities==[encoded(group['identity'])for group in groups])
            capabilities.add('order.asc')
    else:
        _require(not order)
    for position,(output,column)in enumerate(zip(outputs,columns),1):
        _require(type(output)is dict and set(output)=={'expression','name'}and type(output['name'])is str and bool(output['name'])and output['name']not in output_names);output_names.add(output['name'])
        expression=output['expression'];_require(type(expression)is dict)
        if expression.get('op')=='count':
            _require(count and expression=={'op':'count','type':INTEGER})
            plans.append(('count',None));representation={'kind':'scalar','logicalType':INTEGER,'carrier':'text','decoder':'exact-integer'};identities=[scan['record']];capabilities.add('aggregate.count')
        elif expression.get('op')=='countDistinctPathTargets':
            _require(count and hops and expression=={'op':'countDistinctPathTargets','pathOccurrence':expansion['occurrence'],'type':INTEGER})
            terminal=hops[-1];plans.append(('targets',None));capabilities.add('aggregate.pathTargetDistinctCount')
            representation={'kind':'scalar','logicalType':INTEGER,'carrier':'text','decoder':'exact-integer',
                'pathTarget':{'pathOccurrence':expansion['occurrence'],'record':terminal[0]['target'],'key':terminal[4]}}
            identities=[terminal[0]['target'],*terminal[4]['fields']]
        elif expression.get('op')=='field':
            _require(set(expression)=={'op','identity','scan'}and expression['scan']==scan['occurrence']);identity=expression['identity']
            _require(not count or encoded(identity)in group_identities)
            plans.append(('field',field_home(record,identity)));capabilities.add('type.string')
            type_graph[encoded(identity)]={'availability':'required','identity':identity,'kind':'scalar','type':STRING}
            representation={'kind':'scalar','logicalType':STRING,'carrier':'text','decoder':'text'};identities=[identity]
        elif expression.get('op')=='relatedKeys':
            _require(not count and not hops and set(expression)=={'op','scan','bound','relationship'}and expression['scan']==scan['occurrence']and type(expression['bound'])is int and 1<=expression['bound']<=1000)
            mapping,source_type,target_type,homes,target_key=relation(expression['relationship'],scan['record'])
            bound=expression['bound'];plans.append(('related',(mapping,source_type,target_type,homes,bound)))
            capabilities.add('relationship.boundedKeys')
            representation={'kind':'relatedKeys','relationship':mapping['logical'],'key':target_key,'bound':bound}
            identities=[scan['record'],mapping['target'],*target_key['fields']]
        else:raise ValueError('Unselected original graph expression')
        _require(column=={'nullable':False,'outputName':output['name'],'position':position,'representation':representation,'sourceIdentities':identities})
    _require(not count or any(kind in ('count','targets')for kind,_ in plans))
    _require(type(plan['typeGraph'])is list and sorted(map(encoded,plan['typeGraph']))==sorted(map(encoded,type_graph.values())))
    _require(type(plan['requiredCapabilities'])is list and sorted(plan['requiredCapabilities'])==sorted(capabilities))
    original_request=encoded(request);original_artifact=encoded(artifact);original_rows=encoded(scope.source_rows())
    def oracle(supplied_model,supplied_source,supplied_request):
        scope.renew();scope.parity();configuration.verify()
        _require(type(supplied_model)is bytes and supplied_model==model and type(supplied_source)is bytes and supplied_source==source and type(supplied_request)is dict and encoded(supplied_request)==original_request)
        _require(configuration.model_source==model and scope.source.original_source==source and encoded(request)==original_request and encoded(artifact)==original_artifact and rebuilt()==request)
        observed=scope.source_rows();_require(encoded(observed)==original_rows)
        rows=[r for r in observed['object_current']if r['source_system']==configuration.source_system and r['type_id']==tid]
        _require(len(rows)<=1000 and len(rows)*len(outputs)<=100000 and all(r['schema_revision']==configuration.document_revision for r in rows))
        objects={(r['source_system'],r['type_id'],r['id']):r for r in observed['object_current']}
        _require(len(objects)==len(observed['object_current'])and len(observed['edge_current'])<=1000)
        def adjacent(root,hop):
            mapping,source_type,target_type,homes=hop[:4];found=[]
            _require(root['type_id']==source_type)
            for edge in observed['edge_current']:
                if edge['source_system']!=configuration.source_system or edge['rel_type_id']!=mapping['typeId']or edge['source_type']!=source_type or edge['source_id']!=root['id']:continue
                _require(edge['target_type']==target_type and edge['schema_revision']==configuration.document_revision)
                target=objects.get((configuration.source_system,target_type,edge['target_id']))
                _require(target is not None and target['schema_revision']==configuration.document_revision)
                props=decode_retained_json(target['props_json'].encode());_require(all(type(props.get(pid))is str for pid in homes))
                found.append((target,edge,tuple(props[pid]for pid in homes)))
            return found
        roots=[];terminal_keys=[];occurrences=[]
        for row in rows:
            if not hops:roots.append(row);continue
            for middle,first,_ in adjacent(row,hops[0]):
                for terminal,second,keys in adjacent(middle,hops[1]):
                    terminal_keys.append(keys)
                    roots.append(row);occurrences.append([row['type_id'],row['id'],first['rel_type_id'],first['id'],middle['type_id'],middle['id'],second['rel_type_id'],second['id'],terminal['type_id'],terminal['id']])
                    _require(len(roots)<=1000)
        answer=[];collections=[]
        if count:
            buckets={}if groups else{():[]}
            for index,row in enumerate(roots):
                props=decode_retained_json(row['props_json'].encode());_require(all(type(props.get(pid))is str for pid in group_homes))
                group=tuple(props[pid]for pid in group_homes);buckets.setdefault(group,[]).append(index)
            selected_buckets=list(buckets.items())
            if ordered_identities:
                positions={encoded(group['identity']):position for position,group in enumerate(groups)}
                order_positions=[positions[identity]for identity in ordered_identities]
                selected_buckets.sort(key=lambda item:tuple(item[0][position].encode('utf-8')for position in order_positions))
            for group,indexes in selected_buckets:
                if having_threshold is not None and len(indexes)<=having_threshold:continue
                values=[]
                for kind,selection in plans:
                    if kind=='field':values.append(group[group_homes.index(selection)])
                    elif kind=='count':values.append(str(len(indexes)))
                    else:values.append(str(len({terminal_keys[i]for i in indexes})))
                answer.append(values)
        else:
            for row in roots:
                props=decode_retained_json(row['props_json'].encode());_require(type(props)is dict);values=[]
                for kind,selection in plans:
                    if kind=='field':
                        _require(type(props.get(selection))is str);values.append(props[selection]);continue
                    mapping,source_type,target_type,homes,bound=selection
                    found=adjacent(row,(mapping,source_type,target_type,homes))
                    found.sort(key=lambda item:(tuple(v.encode('utf-8')for v in item[2]),int(item[1]['id'])))
                    values.append(json.dumps({'items':[list(item[2])for item in found[:bound]],'truncated':len(found)>bound},ensure_ascii=False,separators=(',',':')))
                    collections.append({'root_type':row['type_id'],'root_id':row['id'],'relationship_type':mapping['typeId'],'bound':bound,
                        'ordered_edges':[edge['id']for _,edge,_ in found],'keys':[list(keys)for _,_,keys in found]})
                answer.append(values)
        scope.renew();scope.parity();configuration.verify();_require(rebuilt()==request and encoded(scope.source_rows())==original_rows)
        return {'rows':answer,'witnesses':{'source_records':len(rows),'result_occurrences':len(answer),'publication_id':scope.descriptor.publication_id,'source_sha256':hashlib.sha256(source).hexdigest(),'collections':collections,'path_occurrences':occurrences},'scope':'Independent complete original typed-graph required STRING projection/count, forward one-hop keys or exactly-two-hop root occurrence/grouped count and distinct terminal-key bag; no native/source/publication authority.'}
    return oracle
