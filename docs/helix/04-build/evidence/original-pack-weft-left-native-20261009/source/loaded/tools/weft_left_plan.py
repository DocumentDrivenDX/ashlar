"""Closed String LEFT proof for an explicitly selected experimental host.

UMF and the pinned public compiler own source interpretation. This checks their
retained identity/descriptor/binding correspondence; it never infers SQL meaning
or converts authored availability. Physical schema and full-source row guards
remain separate obligations inside the held publication interval.
"""
import hashlib,json,re

BACKEND={'backendId':'ashlar.databricks.left-join','backendVersion':'0.3.0-left-join-candidate','targetProfile':'spark4-delta4-left-join-candidate'}
STRING={'family':'string','facets':{},'nullable':False}
SLOT={'family':'integer','facets':{'integerWidth':{'bits':64,'signed':True}},'nullable':False}
ENCODING='ashlar-weft-json-native-null/0.1-candidate'

def closed(value,keys):
    if type(value)is not dict or set(value)!=set(keys):raise ValueError('Unknown LEFT proof structure')

def identity(value):
    closed(value,('documentId','module','element','revision'))
    if any(type(v)is not str or not v for v in value.values()):raise ValueError('Original LEFT identity required')
    return json.dumps(value,sort_keys=True,separators=(',',':'))

def admit_left_plan(artifact,binding,modules):
    plan=artifact['logicalPlan'];closed(plan,('aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph','outerJoinScans'))
    if plan['aggregate']is not False or plan['filters']!=[] or plan['groups']!=[] or any(plan[k]is not None for k in ('limit','pageKey','readProfile')) or plan['irVersion']!='weft-ir/0.3.0' or plan['modulePins']!=binding['modelPins']:raise ValueError('Closed LEFT row subset required')
    originals={};selected_modules=set()
    if type(modules)is not list or [m['pin'] for m in modules]!=binding['modelPins']:raise ValueError('Complete LEFT original pin vector required')
    for entry in modules:
        if type(entry['documentJson'])is not str or hashlib.sha256(entry['documentJson'].encode()).hexdigest()!=entry['pin']['sha256']:raise ValueError('Original LEFT model bytes changed')
        model=json.loads(entry['documentJson'])
        if model['id']!=entry['pin']['documentId'] or model['umf']!=entry['pin']['umfVersion']:raise ValueError('Original LEFT owning version changed')
        selected=entry.get('selectedModuleIds')
        if type(selected)is not list or not selected or any(type(v)is not str for v in selected) or len(set(selected))!=len(selected) or not set(selected)<={m['id']for m in model['modules']}:raise ValueError('Exact selected original module scope required')
        selected_modules.update((model['id'],entry['pin']['revision'],v)for v in selected)
        for module in model['modules']:
            for element in module['elements']:
                key=identity({'documentId':model['id'],'module':module['id'],'element':element['id'],'revision':entry['pin']['revision']})
                if key in originals:raise ValueError('Duplicate original LEFT element')
                originals[key]=element
    scans={};scan_fields={};used={};optional=set();needed=set();expected_graph=set();left=[];inner=False;equal=False;nullable_equal=False
    def scan(value):
        closed(value,('occurrence','pin','record'));name=value['occurrence'];key=identity(value['record'])
        if type(name)is not str or not re.fullmatch(r's[0-9]+',name) or name in scans or value['pin'] not in binding['modelPins'] or any(value['record'][k]!=value['pin'][k] for k in ('documentId','revision')):raise ValueError('Original ordered LEFT source occurrence required')
        if (value['record']['documentId'],value['record']['revision'],value['record']['module'])not in selected_modules:raise ValueError('Record outside selected original module')
        records=[r for r in binding['records'] if r['logical']==value['record']]
        if len(records)!=1 or records[0]['kind']!='object' or originals[key]['kind']!='record':raise ValueError('Original Object Record sentinel home required')
        record=records[0]
        if type(record['table'])is not int or not 0<=record['table']<len(binding['publication']['tables']):raise ValueError('Original source table index required')
        props={}
        for p in record['properties']:
            k=identity(p['logical'])
            if k in props:raise ValueError('Duplicate original property home')
            props[k]=p
        scans[name]={'identity':value['record'],'record':record,'properties':props};scan_fields[name]={};return name
    def field(value,visible,typed=True):
        closed(value,('identity','scan','span','type') if typed else ('identity','scan','op'))
        if not typed and value['op']!='field':raise ValueError('Only direct LEFT Field output admitted')
        name=value['scan'];key=identity(value['identity'])
        if name not in visible or key not in scans[name]['properties']:raise ValueError('LEFT Field outside original join prefix')
        record_identity=scans[name]['identity'];field_identity=value['identity']
        if any(field_identity[k]!=record_identity[k]for k in ('documentId','revision')) or (field_identity['documentId'],field_identity['revision'],field_identity['module'])not in selected_modules:raise ValueError('Field outside selected original source scope')
        members=originals[identity(record_identity)].get('members')
        if type(members)is not list or sum(m=={'module':field_identity['module'],'element':field_identity['element']}for m in members)!=1:raise ValueError('Consumed Field must be an exact original qualified Record member')
        scan_fields[name][key]=field_identity
        original=originals[key];availability=original.get('nullability')
        if original['kind']!='field' or original.get('cardinality')!='one' or original.get('scalarType')!='string' or original.get('facets',{})!={} or availability not in ('required','absent-allowed'):raise ValueError('Original scalar String Field required')
        home=scans[name]['properties'][key]['home'];closed(home,('kind','propertyId','encoding') if availability=='absent-allowed' else ('kind','propertyId'))
        if home['kind']!='props' or type(home['propertyId'])is not str or not home['propertyId'] or (availability=='absent-allowed' and home['encoding']!=ENCODING):raise ValueError('Exact original native-null property permission required')
        if typed:
            closed(value['span'],('start','end'))
            if any(type(v)is not int or v<0 for v in value['span'].values()) or value['span']['end']<value['span']['start'] or value['type']!=STRING or value['type'].get('nullable')is not False:raise ValueError('Exact original String type/span required')
        used[key]={'identity':value['identity'],'availability':availability,'kind':'scalar','type':STRING};needed.add((identity(scans[name]['identity']),key))
        if availability=='absent-allowed':optional.add(key)
        return key
    visible={scan(plan['source'])}
    if type(plan['joins'])is not list or not plan['joins']:raise ValueError('At least one LEFT stage required')
    for join in plan['joins']:
        is_left='kind' in join;closed(join,('right','on','kind') if is_left else ('right','on'))
        if is_left and join['kind']!='left':raise ValueError('Only ordered INNER/LEFT stages admitted')
        name=scan(join['right']);visible.add(name)
        if is_left:left.append(name)
        else:inner=True
        if type(join['on'])is not list or not join['on']:raise ValueError('Original nonempty String ON required')
        for predicate in join['on']:
            if predicate.get('op')=='legacy':
                closed(predicate,('op','predicate'));p=predicate['predicate'];closed(p,('op','left','right'));closed(p['right'],('kind','field'))
                if p['op']!='equal' or p['right']['kind']!='field':raise ValueError('Only original Field equality ON admitted')
                keys=[field(p['left'],visible),field(p['right']['field'],visible)]
                if any(k in optional for k in keys):raise ValueError('Optional equality requires explicit nullable comparison')
                equal=True
            elif predicate.get('op')=='nullableStringEqual':
                closed(predicate,('op','left','right'));keys=[field(predicate['left'],visible),field(predicate['right'],visible)]
                if not optional.intersection(keys):raise ValueError('Nullable equality requires original optional Field')
                nullable_equal=True;expected_graph.update(keys)
            else:raise ValueError('Unknown LEFT ON operation')
    if not left or plan['outerJoinScans']!=left:raise ValueError('Complete ordered unmatched-scan inventory required')
    if list(scans)!=['s'+str(i) for i in range(len(scans))]:raise ValueError('Original source-ordered scan inventory required')
    for value in plan['order']:
        key=field(value,visible);expected_graph.add(key)
        if value['scan']!=plan['source']['occurrence'] or key in optional:raise ValueError('Order must retain original required base-scan Field')
    columns=artifact['columns'];outputs=plan['outputs']
    if type(columns)is not list or not columns or len(columns)!=len(outputs):raise ValueError('Complete LEFT output tuple required')
    names=[]
    for index,(output,column) in enumerate(zip(outputs,columns),1):
        closed(output,('name','expression'));key=field(output['expression'],visible,False);name=output['expression']['scan'];descriptor=used[key];expected_graph.add(key)
        closed(column,('position','outputName','sourceIdentities','nullable','representation'))
        if type(column['position'])is not int or column['position']!=index or type(output['name'])is not str or not output['name'] or column['outputName']!=output['name'] or column['nullable']is not False or column['sourceIdentities']!=[descriptor['identity']]:raise ValueError('Original ordered LEFT column identity required')
        native_null=key in optional;outer={'scan':name,'record':scans[name]['identity']} if name in left else None
        if outer or native_null:
            expected={'kind':'value','descriptor':descriptor['identity'],'nativeNull':native_null}
            if outer:expected['outerJoin']=outer
            if column['representation']!=expected or column['representation'].get('nativeNull')is not native_null:raise ValueError('Original scan-qualified unmatched/optional descriptor required')
        elif column['representation']!={'kind':'scalar','logicalType':STRING,'carrier':'text','decoder':'text'} or column['representation'].get('logicalType',{}).get('nullable')is not False:raise ValueError('Original required String descriptor required')
        names.append(output['name'])
    if len(set(names))!=len(names):raise ValueError('Positioned LEFT outputs require separate host qualification')
    graph=plan['typeGraph']
    if type(graph)is not list or len(graph)!=len(expected_graph) or {identity(d['identity']) for d in graph}!=expected_graph:raise ValueError('Complete original LEFT descriptor graph required')
    for d in graph:
        if d!=used[identity(d['identity'])] or d['type'].get('nullable')is not False:raise ValueError('LEFT cannot rewrite original Field availability/type')
    caps={'scan','project','type.string','join.left','value.outerJoinPresence'}
    if inner:caps.add('innerJoin')
    if equal:caps.add('equal')
    if nullable_equal:caps.add('compare.nullAwareStringEqual')
    if optional:caps|={'value.nativeNull','value.presence'}
    if plan['order']:caps.add('order.asc')
    if type(plan['requiredCapabilities'])is not list or len(plan['requiredCapabilities'])!=len(caps) or set(plan['requiredCapabilities'])!=caps:raise ValueError('Explicit complete LEFT capability composition required')
    # Only physical discriminator/property slots exist in this literal-free subset.
    for i,p in enumerate(artifact['parameters'],1):
        closed(p,('position','logicalType','value','origin'));origin=p['origin']
        if type(p['position'])is not int or p['position']!=i or type(p['value'])is not str:raise ValueError('Original exact backend slot required')
        kind=origin.get('kind')
        if kind in ('sourceDiscriminator','typeDiscriminator','schemaRevision'):
            closed(origin,('kind','record'));rs=[s['record'] for s in scans.values() if s['identity']==origin['record']]
            if not rs:raise ValueError('Slot outside selected original Record')
            expected=rs[0][{'sourceDiscriminator':'sourceSystem','typeDiscriminator':'typeId','schemaRevision':'schemaRevision'}[kind]];logical=SLOT if kind=='typeDiscriminator' else STRING
        elif kind=='propertyPath':
            closed(origin,('kind','field'));key=identity(origin['field']);ps=[s['properties'][key] for s in scans.values() if key in s['properties']]
            if key not in used or not ps:raise ValueError('Slot outside selected original Field')
            expected='$.'+ps[0]['home']['propertyId'];logical=STRING
        else:raise ValueError('Named/literal/unknown LEFT slots refused')
        if p['value']!=expected or p['logicalType']!=logical or p['logicalType'].get('nullable')is not False:raise ValueError('Slot differs from original native binding')
        if logical==SLOT and (p['logicalType']['facets']['integerWidth']['signed']is not True or type(p['logicalType']['facets']['integerWidth']['bits'])is not int or not re.fullmatch('-?[0-9]+',p['value']) or str(int(p['value']))!=p['value'] or not -2**63<=int(p['value'])<2**63):raise ValueError('Physical slot exceeds its separately declared signed64 carrier')
    expected_slots=[]
    def slot(logical,value,origin):
        expected_slots.append({'position':len(expected_slots)+1,'logicalType':logical,'value':value,'origin':origin})
    # Mirror the public profile's declared scan/source/Field-origin inventory,
    # including repeated Record occurrences; never infer placeholders from SQL.
    for name in sorted(scans):
        rs=scans[name];record=rs['record'];rid=rs['identity']
        for kind,home,logical in [('sourceDiscriminator','sourceSystem',STRING),('typeDiscriminator','typeId',SLOT),('schemaRevision','schemaRevision',STRING)]:
            slot(logical,record[home],{'kind':kind,'record':rid})
        def rust_identity(item):
            return json.dumps({k:item[k]for k in ('documentId','module','element','revision')},ensure_ascii=False,separators=(',',':'))
        for fid in sorted(scan_fields[name].values(),key=rust_identity):
            slot(STRING,'$.'+rs['properties'][identity(fid)]['home']['propertyId'],{'kind':'propertyPath','field':fid})
    if json.dumps(artifact['parameters'],sort_keys=True,separators=(',',':'))!=json.dumps(expected_slots,sort_keys=True,separators=(',',':')):raise ValueError('Complete ordered original scan/Field slot inventory required')
    obligations=artifact['obligations'];matches=[o for o in obligations if o['id']=='outerJoin.matchIntegrity']
    if len(matches)!=1:raise ValueError('Mandatory LEFT match obligation required')
    obligation=matches[0];closed(obligation,('id','owner','failureCode','parameters'));parameters=obligation['parameters'];closed(parameters,('phase','samePublicationRequired','noPartialPublication','scans'))
    if obligation['owner']!='host' or obligation['failureCode']!='WFT-OBLIGATION' or parameters['phase']!='before-user-query' or parameters['samePublicationRequired']is not True or parameters['noPartialPublication']is not True or type(parameters['scans'])is not list or len(parameters['scans'])!=len(left):raise ValueError('Complete held LEFT source match checks required')
    for name,entry in zip(left,parameters['scans']):
        closed(entry,('scan','record','table','identityColumn','nativeType','sql'));record=scans[name]['record'];table=binding['publication']['tables'][record['table']]
        closed(entry['table'],('name','uuid','version'))
        if type(entry['table']['version'])is not int or type(entry['table']['name'])is not list or any(type(v)is not str for v in entry['table']['name']):raise ValueError('Exact native table scalar metadata required')
        if entry['scan']!=name or entry['record']!=scans[name]['identity'] or entry['table']!=table or entry['identityColumn']!='id' or entry['nativeType']!='BIGINT' or type(entry['sql'])is not str or not entry['sql']:raise ValueError('Real original scan/Record/table sentinel required')
    integrity=[o for o in obligations if o['id']=='ashlar.candidate.scalarIntegrity']
    if len(integrity)!=1:raise ValueError('Original source guards required')
    checks=integrity[0]['parameters']['checks'];covered=set()
    for c in checks:
        for flag in ('representabilityOnly','publicSourceOnly'):
            if flag in c and type(c[flag])is not bool:raise ValueError('Exact guard disposition flags required')
        if c.get('publicSourceOnly')is True:raise ValueError('String LEFT subset has no mathematical source-token guard')
        key=identity(c['field']);record_key=identity(c['record'])
        if not c.get('representabilityOnly'):covered.add((record_key,key))
        if key in optional:
            props=[s['properties'][key] for s in scans.values() if key in s['properties']]
            if c.get('encoding')!=ENCODING or c.get('propertyId')!=props[0]['home']['propertyId']:raise ValueError('Exact optional source guard home required')
    if not needed<=covered:raise ValueError('Guard every consumed Field before ON')
    for key in optional:
        if not any(identity(c['field'])==key and c.get('representabilityOnly')is True and c.get('failureCode')=='WFT-CAPABILITY' for c in checks):raise ValueError('Missing optional representation refusal guard')
    return {'scans':parameters['scans'],'outputs':columns}

def admit_left_schema(entry,receipt):
    closed(receipt,('table','schema','nativeTypes'))
    closed(receipt['table'],('name','uuid','version'))
    if type(receipt['table']['version'])is not int or receipt['table']!=entry['table']:raise ValueError('Native sentinel schema table differs')
    schema=receipt['schema'];closed(schema,('type','fields'))
    if schema['type']!='struct' or type(schema['fields'])is not list:raise ValueError('Complete actual native Struct schema required')
    fields=schema['fields']
    if any(type(f)is not dict or set(f)!={'name','type','nullable','metadata'} or type(f['nullable'])is not bool for f in fields):raise ValueError('Exact native field metadata required')
    types=receipt['nativeTypes']
    if (type(types)is not list or len(types)!=len(fields)
            or any(type(t)is not list or len(t)!=2 or any(type(v)is not str for v in t) for t in types)
            or [t[0] for t in types]!=[f['name'] for f in fields]
            or len({f['name'] for f in fields})!=len(fields)):
        raise ValueError('Complete injective native field/type inventory required')
    ids=[f for f in fields if f['name']==entry['identityColumn']]
    if len(ids)!=1 or ids[0]['type']!='long' or dict(types).get(entry['identityColumn'])!='BIGINT':raise ValueError('Original physical BIGINT sentinel schema required')
    return receipt

def admit_left_cells(artifact,received):
    """Preserve actual ordered carriers/bags and separate relational absence."""
    closed(received,('schema','rows'));columns=artifact['columns'];names=[c['outputName'] for c in columns]
    if len(set(names))!=len(names) or received['schema']!=[[name,'STRING'] for name in names] or type(received['rows'])is not list:raise ValueError('Exact native ordered LEFT String schema required')
    plan=artifact['logicalPlan'];graph={identity(d['identity']):d for d in plan['typeGraph']};decoded=[]
    if len(graph)!=len(plan['typeGraph']) or len(plan['outputs'])!=len(columns):raise ValueError('Original injective output descriptor graph required')
    records={s['occurrence']:s['record'] for s in [plan['source']]+[j['right'] for j in plan['joins']]}
    def unique(pairs):
        value={}
        for key,item in pairs:
            if key in value:raise ValueError('Duplicate LEFT tagged carrier member')
            value[key]=item
        return value
    for row in received['rows']:
        if type(row)is not list or len(row)!=len(columns) or any(type(cell)is not str for cell in row):raise ValueError('Complete non-null native String carrier tuple required')
        values=[]
        for index,(column,raw) in enumerate(zip(columns,row)):
            expression=plan['outputs'][index]['expression']
            if expression.get('op')!='field' or column['sourceIdentities']!=[expression['identity']] or column['nullable']is not False:raise ValueError('Original direct LEFT output lineage required')
            rep=column['representation']
            if rep['kind']=='scalar':
                if rep!={'kind':'scalar','logicalType':STRING,'carrier':'text','decoder':'text'} or rep['logicalType']['nullable']is not False:raise ValueError('Exact required String carrier required')
                values.append(raw);continue
            if rep.get('kind')!='value' or type(rep.get('nativeNull'))is not bool:raise ValueError('Explicit LEFT tagged representation required')
            closed(rep,('kind','descriptor','nativeNull','outerJoin') if 'outerJoin' in rep else ('kind','descriptor','nativeNull'))
            if rep['descriptor']!=expression['identity']:raise ValueError('Tagged LEFT Field identity differs')
            if 'outerJoin' in rep:
                expected={'scan':expression['scan'],'record':records[expression['scan']]}
                if expression['scan'] not in plan['outerJoinScans'] or rep['outerJoin']!=expected:raise ValueError('Exact unmatched scan/Record provenance required')
            descriptor=graph[identity(rep['descriptor'])]
            if descriptor['kind']!='scalar' or descriptor['type']!=STRING or descriptor['type']['nullable']is not False or (rep['nativeNull'] and descriptor['availability']!='absent-allowed'):raise ValueError('Original String/availability descriptor required')
            cell=json.loads(raw,object_pairs_hook=unique)
            if type(cell)is not dict:raise ValueError('Native tagged String carrier required')
            state=cell.get('state')
            if state=='absent':
                closed(cell,('state',))
                if 'outerJoin' not in rep:raise ValueError('Absence requires explicit relational unmatched provenance')
            elif state=='null':
                closed(cell,('state',))
                if rep['nativeNull']is not True:raise ValueError('Matched required Field null is invalid')
            elif state=='value':
                closed(cell,('state','value'))
                if type(cell['value'])is not str:raise ValueError('Original exact String value required')
            else:raise ValueError('Unknown LEFT presence state')
            values.append(cell)
        decoded.append(values)
    return {'schema':received['schema'],'rows':received['rows'],'decoded':decoded}
