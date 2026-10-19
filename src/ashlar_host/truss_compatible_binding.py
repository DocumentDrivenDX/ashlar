# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact public compatible-revision request/proof foundation; no query authority.

Admission takes the original request binding, after independent full publication
and binding admission. It proves only supplemental accepted schema correspondence;
it neither replaces existing obligations nor admits SQL or buffered results.
"""
import hashlib,json,re
from .delta_custody import encoded
from .truss_graph_admission import TrussGraphAdmission
from .truss_weft_query import truss_scalar_compile_request
BACKEND={'backendId':'ashlar.databricks','backendVersion':'0.1.1-compatible-revisions-candidate','interfaceVersion':'weft-backend/0.2.0','targetProfile':'dbsql-compatible-revisions-candidate'}
REQUIREMENTS=[
 'verify every selected physical revision is accepted in the same immutable publication',
 'establish original logical field/key correspondence for every revision; the compiler does not prove compatibility',
 'enforce every integrity check before buffered publication; never filter unselected revisions']

def _selected_revisions(admission,revision,revisions):
    if type(admission)is not TrussGraphAdmission or type(revision)is not str or len(revision)>20 or re.fullmatch('[1-9][0-9]*',revision)is None or revision not in admission.revisions:raise ValueError('Actual accepted primary revision required')
    if type(revisions)is not tuple or not 1<=len(revisions)<=32 or any(type(r)is not str or len(r)>20 or re.fullmatch('[1-9][0-9]*',r)is None or int(r)>int(revision)or r not in admission.revisions for r in revisions)or len(set(revisions))!=len(revisions)or revision not in revisions:raise ValueError('Exact accepted compatible revisions required')

def _prove(admission,revision,document_id,revisions,records):
    _selected_revisions(admission,revision,revisions)
    primary=admission.revisions[revision]
    if document_id not in primary['models']:raise ValueError('Original accepted document required')
    new_model=primary['models'][document_id][0]
    def elements(model):return {(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    new_elements=elements(new_model)
    for old_revision in revisions:
        old=admission.revisions[old_revision]
        if document_id not in old['models']:raise ValueError('Original accepted document missing')
        model=old['models'][document_id][0]
        if {k:v for k,v in model.items()if k!='modules'}!={k:v for k,v in new_model.items()if k!='modules'}:raise ValueError('Original model semantics changed')
        old_modules={m['id']:m for m in model['modules']};new_modules={m['id']:m for m in new_model['modules']}
        for module_id,module in old_modules.items():
            if module_id not in new_modules or {k:v for k,v in module.items()if k!='elements'}!={k:v for k,v in new_modules[module_id].items()if k!='elements'}:raise ValueError('Original module semantics changed')
        for owner,declaration in elements(model).items():
            current=new_elements.get(owner)
            if current is None:raise ValueError('Original declaration discarded')
            if declaration.get('kind')!='record':
                if current!=declaration:raise ValueError('Original field/relationship semantics changed')
            else:
                if {k:v for k,v in current.items()if k!='members'}!={k:v for k,v in declaration.items()if k!='members'}:raise ValueError('Original logical record/key semantics changed')
                members=declaration['members'];new_members=current['members']
                retained=[member for member in new_members if member in members]
                if retained!=members:raise ValueError('Original member order changed')
                additions=[member for member in new_members if member not in members]
                if any(additions.count(member)!=1 for member in additions):raise ValueError('Duplicate new record member')
                for member in additions:
                    field=new_elements.get((member['module'],member['element']))
                    if field is None or field.get('kind')!='field'or field.get('nullability')!='absent-allowed':raise ValueError('Only absent-allowed new fields compatible')
        for bound in records:
            tid=bound['typeId'];new=primary['types'].get(tid);previous=old['types'].get(tid)
            if new is None or previous is None or previous['owner']!=new['owner']or new['owner']!=(document_id,bound['logical']['module'],bound['logical']['element']):raise ValueError('Original type allocation/owner changed')
            if previous['keys']!=new['keys']or any(new['fields'].get(pid)!=pin for pid,pin in previous['fields'].items())or any(new['domains'].get(pid)!=domain for pid,domain in previous['domains'].items()):raise ValueError('Original property pins/domains/keys changed')
            if any(new['domains'][pid][1]!='absent-allowed'for pid in set(new['fields'])-set(previous['fields'])):raise ValueError('Only absent-allowed new allocations compatible')
    return {'primary_revision':revision,'physical_revisions':list(revisions),'qualification':'Accepted schema correspondence only; no source, publication, integrity execution or result authority.'}

def truss_compatible_compile_request(admission,manifest,table_uuids,manifest_uuid,sql,*,revision,document_id,physical_revisions,aliases=None):
    request=truss_scalar_compile_request(admission,manifest,table_uuids,manifest_uuid,sql,revision=revision,document_id=document_id,aliases=aliases)
    binding=json.loads(request['target']['bindingJson'])
    _prove(admission,revision,document_id,physical_revisions,binding['records'])
    for record in binding['records']:record['compatibleSchemaRevisions']=list(physical_revisions)
    raw=encoded(binding);request['target'].update({k:BACKEND[k]for k in ('backendId','backendVersion','targetProfile')},bindingJson=raw,bindingSha256=hashlib.sha256(raw.encode()).hexdigest())
    return request

def _admit_compatible_revisions(admission,artifact,binding,*,revision,document_id,joined=False):
    if type(artifact)is not dict or artifact.get('status')!='compiled'or artifact.get('backend')!=BACKEND or artifact.get('bindingSha256')!=hashlib.sha256(encoded(binding).encode()).hexdigest()or artifact.get('modelPins')!=binding.get('modelPins'):raise ValueError('Exact compatible compiler binding required')
    _selected_revisions(admission,revision,(revision,))
    original=admission.revisions[revision]['models'].get(document_id)
    if original is None:raise ValueError('Original model required')
    digest=hashlib.sha256(original[1]).hexdigest();pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':original[0]['umf']}
    if binding.get('modelPins')!=[pin]:raise ValueError('Original model pin differs')
    plan=artifact.get('logicalPlan')
    if type(plan)is not dict or type(plan.get('requiredCapabilities'))is not list or type(artifact.get('columns'))is not list or any(type(c)is not dict or type(c.get('representation',{}))is not dict for c in artifact.get('columns',[]))or (not joined and plan.get('joins')!=[]) or any(type(c)is not str or any(token in c.lower()for token in ('relationship','related','path','travers'))for c in plan.get('requiredCapabilities',[]))or any(c.get('representation',{}).get('kind')in ('relatedKeys','paths')for c in artifact.get('columns',[])):raise ValueError('Unjoined single-record compatibility foundation required')
    def relationship_operation(value):
        if type(value)is dict:return value.get('op')in ('hasRelated','relatedKeys','paths')or any(relationship_operation(v)for v in value.values())
        return type(value)is list and any(relationship_operation(v)for v in value)
    if relationship_operation(plan):raise ValueError('Relationship plans require separate admission')
    scans=[plan.get('source')]
    if joined:
        joins=plan.get('joins')
        if type(joins)is not list or len(joins)!=1 or type(joins[0])is not dict or set(joins[0])!={'on','right'}:raise ValueError('One original compatibility join required')
        scans.append(joins[0]['right'])
    logicals=[]
    for position,source in enumerate(scans):
        if type(source)is not dict or set(source)!={'occurrence','pin','record'}or source['occurrence']!='s'+str(position)or source['pin']!=pin:raise ValueError('Original selected source required')
        if source['record']not in logicals:logicals.append(source['record'])
    records=[r for r in binding.get('records',[])if r.get('logical')in logicals]
    if len(records)!=len(logicals):raise ValueError('Exact selected binding records required')
    revision_sets=[]
    for record in records:
        installation=json.loads(admission.batch.begin)['prior']['installationId']
        accepted=admission.revisions[revision]['types'].get(record.get('typeId'))
        if accepted is None or record.get('logical')!={'documentId':document_id,'revision':digest,'module':accepted['owner'][1],'element':accepted['owner'][2]}or accepted['owner'][0]!=document_id or record.get('sourceSystem')!=installation:raise ValueError('Actual accepted logical record correspondence required')
        report=json.loads(admission.revisions[revision]['report'])
        properties=[{'logical':{'documentId':document_id,'revision':digest,'module':f['moduleId'],'element':f['elementId']},'home':{'kind':'props','propertyId':f['propertyId']}}for f in report['properties']if f['typeId']==record['typeId']]
        if record.get('properties')!=properties:raise ValueError('Complete actual property allocations required')
        if record.get('kind')!='object'or record.get('schemaRevision')!=revision:raise ValueError('Original primary object binding required')
        revisions=record.get('compatibleSchemaRevisions')
        if type(revisions)is not list:raise ValueError('Explicit compatible revisions required')
        revision_sets.append(tuple(revisions))
    if len(set(revision_sets))!=1:raise ValueError('Same explicit selected revision set required')
    revisions=list(revision_sets[0])
    obligations=artifact.get('obligations')
    if type(obligations)is not list or any(type(o)is not dict for o in obligations):raise ValueError('Original host obligations required')
    selected=[o for o in obligations if o.get('id')=='ashlar.compatibleSchemaRevisions']
    expected={'id':'ashlar.compatibleSchemaRevisions','owner':'host','failureCode':'WFT-OBLIGATION','parameters':{'records':[{'logical':record['logical'],'primarySchemaRevision':revision,'compatibleSchemaRevisions':record['compatibleSchemaRevisions']}for record in records],'modelPins':binding['modelPins'],'publication':binding['publication'],'requirements':REQUIREMENTS}}
    if selected!=[expected]:raise ValueError('Exact original compatible revision obligation required')
    return _prove(admission,revision,document_id,tuple(revisions),records)


def admit_truss_compatible_revisions(admission,artifact,binding,*,revision,document_id):
    return _admit_compatible_revisions(admission,artifact,binding,revision=revision,document_id=document_id)


def admit_truss_compatible_join_revisions(admission,artifact,binding,*,revision,document_id):
    """Supplemental exact schema correspondence for two original scan occurrences."""
    return _admit_compatible_revisions(admission,artifact,binding,revision=revision,document_id=document_id,joined=True)
