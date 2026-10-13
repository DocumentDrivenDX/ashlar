"""Named fields from an already authorized complete Truss object singleton."""
from ashlar.schema import decode_retained_json
from ashlar.typed_source_policy import NumericToken
import json
from .truss_singleton import read_original_truss_singleton
from .lifecycle import owned_context
from .exact_report import report_body


def _unique_pairs(pairs):
    seen=set()
    for key,value in pairs:
        if key in seen:raise ValueError('Duplicate property member')
        seen.add(key);yield key,value


def read_original_truss_record(driver,*,type_id,entity_id):
    row=read_original_truss_singleton(driver,kind='object',type_id=type_id,entity_id=entity_id)
    if row is None:return None
    selected=driver.source_admission.revisions.get(row['schema_revision'])
    if selected is None or str(type_id)not in selected['types']:raise PermissionError('Actual row catalog/type required')
    report=decode_retained_json(selected['report'])
    record=selected['types'][str(type_id)];doc,module,element=record['owner']
    model=selected['models'][doc][0]
    elements={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    declaration=elements[module,element]
    raw=row['props_json']
    if type(raw)is not str or len(raw.encode())>1048576:raise ValueError('Bounded original property carrier required')
    props=json.loads(raw,object_pairs_hook=lambda pairs:dict(_unique_pairs(pairs)),parse_int=NumericToken,parse_float=NumericToken,parse_constant=lambda value:(_ for _ in ()).throw(ValueError('Nonfinite property token')))
    if type(props)is not dict or not set(props)<=set(record['fields']):raise PermissionError('Unknown executing property allocation')
    fields=[];allocated=set()
    for member in declaration['members']:
        field=elements[member['module'],member['element']]
        matches=[p for p in report['properties']if p['typeId']==str(type_id)and p['moduleId']==member['module']and p['elementId']==member['element']]
        if len(matches)!=1:raise PermissionError('Exact accepted property allocation required')
        pid=matches[0]['propertyId']
        if pid not in record['fields']or pid in allocated:raise PermissionError('Actual property definition home required')
        allocated.add(pid)
        # The nominal admission already matched these field homes to catalog pins.
        value=props.get(pid)
        family=field['scalarType']
        if value is not None and pid in props:
            if family in ('integer','decimal')and type(value)is NumericToken:value={family+'_token':value.text}
            elif not ((family=='string'and type(value)is str)or(family=='boolean'and type(value)is bool)):raise PermissionError('Selected scalar observation differs')
        elif pid in props and field['nullability']!='nullable':raise PermissionError('Declared nonnullable field is null')
        fields.append({'module_id':member['module'],'field_id':member['element'],'property_id':pid,'scalar_type':field['scalarType'],'nullability':field['nullability'],'state':'absent'if pid not in props else 'null'if value is None else'value','value':value})
    if allocated!=set(record['fields']):raise PermissionError('Complete accepted field correspondence required')
    return {'row':dict(row),'document_id':doc,'module_id':module,'record_id':element,'fields':fields}


def run_read_truss_record_command(configuration,output,*,type_id,entity_id):
    if any(type(v)is not int or not 0<v<2**63 for v in (type_id,entity_id)):raise ValueError('Positive signed64 identities required')
    from .truss_resume_cli import run_selected_truss_command
    from .truss_recovery import restore_applied_truss_publication
    def read(driver,config):
        with owned_context(driver.writer(driver.source_sessions.feed,driver.context)):restore_applied_truss_publication(driver,config['request_digest'])
        return {'publication_id':driver.policy.active['manifest']['publication_id'],'type_id':type_id,'entity_id':entity_id,'record':read_original_truss_record(driver,type_id=type_id,entity_id=entity_id),'qualification':'Named-field observation of an authorized complete original singleton; no new scalar support or source grant.'}
    def write(result,destination):
        from .truss_example import private_file
        private_file(destination,report_body(result,64*1048576));return result
    return run_selected_truss_command(configuration,output,read,report_writer=write)
