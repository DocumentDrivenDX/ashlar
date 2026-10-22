# SPDX-License-Identifier: MIT OR Apache-2.0
"""Accepted scalar graph correspondence; current Truss authority stays session-owned."""
import base64,hashlib,json
from ashlar.staging import batch_row
from ashlar.truss_native_feed import typed_scalar_entity_changes
from .truss_current_catalog import (CurrentCatalogEvidence,
    CURRENT_TRUSS_VERSION, CURRENT_TRUSS_045_VERSION, CURRENT_TRUSS_048_VERSION,
    CURRENT_EVIDENCE_PROFILE, CURRENT_045_EVIDENCE_PROFILE, CURRENT_048_EVIDENCE_PROFILE,
    interpret_current_catalog, require_installed_current_catalog_evidence)

PROFILE='ashlar-truss-scalar-graph-semantic-admission/0.1'
TYPED_PROFILE='ashlar-truss-typed-graph-semantic-admission/0.1'
CURRENT_PROFILE='ashlar-truss-current-umf010-graph-semantic-admission/0.1'
CURRENT_045_PROFILE='ashlar-truss-current-umf010-graph-semantic-admission/0.2'
CURRENT_048_PROFILE='ashlar-truss-current-umf010-graph-semantic-admission/0.3'

def current_admission_profile(version):
    try:
        return {CURRENT_TRUSS_VERSION: CURRENT_PROFILE,
                CURRENT_TRUSS_045_VERSION: CURRENT_045_PROFILE,
                CURRENT_TRUSS_048_VERSION: CURRENT_048_PROFILE}[version]
    except KeyError:
        raise ValueError('Supported versioned current Truss evidence required') from None

def admitted_facets(field):
    facets=field.get('facets',{})
    if type(facets)is not dict:raise ValueError('Exact declared scalar facets required')
    family=field['scalarType']
    signed=family=='integer'and facets=={'integerWidth':{'bits':64,'signed':True}}and type(facets['integerWidth']['bits'])is int and type(facets['integerWidth']['signed'])is bool
    decimal=(family=='decimal'and set(facets)=={'precision','scale'}
        and type(facets['precision'])is int and facets['precision']==18
        and type(facets['scale'])is int and facets['scale']in (2,4))
    if facets and not(signed or decimal):raise ValueError('Supported declared signed64/decimal18,2 or18,4 facets required')
    return json.loads(json.dumps(facets))


def require_facet_value(family,facets,value):
    if not facets:return
    token=value['text']
    if family=='integer':
        if len(token.lstrip('-'))>19 or not -(2**63)<=int(token)<2**63:raise PermissionError('Declared signed64 value overflow')
        return
    mantissa,*exponent=token.lower().split('e');fraction=mantissa.split('.',1)[1]if'.'in mantissa else''
    coefficient=mantissa.lstrip('-').replace('.','').lstrip('0')
    if not coefficient:return
    exponent_token=exponent[0]if exponent else'0';digits=exponent_token.lstrip('+-').lstrip('0')
    if len(digits)>7:raise PermissionError('Declared decimal value overflow')
    exponent_value=(int(digits)*(-1 if exponent_token.startswith('-')else 1))if digits else 0
    shift=facets['scale']-len(fraction)+exponent_value;trailing=len(coefficient)-len(coefficient.rstrip('0'))
    if shift<0 and -shift>trailing or len(coefficient)+shift>facets['precision']:raise PermissionError('Declared decimal requires exact no-rounding value')


class TrussGraphAdmission:
    def __init__(self,batch,catalog_views):
        batch_row(batch)
        if batch.profile!='truss-native-feed/0.1'or not (type(catalog_views)is dict or isinstance(catalog_views,CurrentCatalogEvidence)):raise ValueError('Original native page and explicit original catalog evidence required')
        self.typed=False;self.batch=batch;self.parsed=typed_scalar_entity_changes(batch.begin,sha256=hashlib.sha256(batch.begin).hexdigest(),prior=base64.b64decode(batch.cursor_before,validate=True),next=base64.b64decode(batch.cursor_after,validate=True),feed=batch.feed)
        payload=json.loads(batch.begin);self.revisions={};self.events={};self.current=isinstance(catalog_views,CurrentCatalogEvidence)
        self.catalog_views=dict(catalog_views.views if self.current else catalog_views)
        self.current_truss_version = catalog_views.truss_version if self.current else None
        self.current_catalog_evidence_profile = catalog_views.profile if self.current else None
        self.current_runtime_wheel_sha256 = catalog_views.runtime_wheel_sha256 if self.current else None
        self.current_runtime_manifest_sha256 = catalog_views.runtime_manifest_sha256 if self.current else None
        installation=json.loads(base64.b64decode(batch.cursor_before))['installationId']
        if self.current:
            require_installed_current_catalog_evidence(catalog_views)
            current_admission_profile(catalog_views.truss_version)
            expected_catalog_profile = {
                CURRENT_TRUSS_VERSION: CURRENT_EVIDENCE_PROFILE,
                CURRENT_TRUSS_045_VERSION: CURRENT_045_EVIDENCE_PROFILE,
                CURRENT_TRUSS_048_VERSION: CURRENT_048_EVIDENCE_PROFILE,
            }[catalog_views.truss_version]
            if catalog_views.profile != expected_catalog_profile:
                raise ValueError('Supported versioned current Truss evidence required')
            for prerequisite in payload['catalogPrerequisites']:
                if prerequisite.get('reportBytesBase64')is None:continue
                raw=base64.b64decode(prerequisite['reportBytesBase64'],validate=True)
                revision=prerequisite.get('revision')
                if hashlib.sha256(raw).hexdigest()!=prerequisite.get('reportSha256'):
                    raise ValueError('Original accepted report digest differs')
                if (catalog_views.reports.get(revision)!=raw
                        or revision not in self.catalog_views
                        or json.loads(raw).get('installationId')!=installation):
                    raise ValueError('Current report source authority differs from public current evidence')
                selected=interpret_current_catalog(revision,raw,self.catalog_views[revision],catalog_views.sources,
                                                   truss_version=catalog_views.truss_version)
                if revision in self.revisions:raise ValueError('Duplicate current accepted catalog revision')
                self.typed = self.typed or any(
                    family != 'string'
                    for record in selected['types'].values()
                    for family, _ in record['domains'].values())
                self.revisions[revision]=selected
        else:
          for prerequisite in payload['catalogPrerequisites']:
            if prerequisite.get('reportBytesBase64')is None:continue
            raw=base64.b64decode(prerequisite['reportBytesBase64'],validate=True)
            if hashlib.sha256(raw).hexdigest()!=prerequisite['reportSha256']:raise ValueError('Original accepted report digest differs')
            report=json.loads(raw);revision=report['revision']
            if revision!=prerequisite['revision']or report['installationId']!=installation or revision in self.revisions:raise ValueError('Original report installation/revision differs')
            catalog_raw=catalog_views.get(revision)
            if type(catalog_raw)is not bytes or len(catalog_raw)>16777216:raise ValueError('Original complete catalog view required for each accepted revision')
            catalog=json.loads(catalog_raw)
            if catalog.get('interfaceVersion')!='truss-catalog-view/0.1.0' or catalog.get('scope')!={'kind':'complete'}:
                raise ValueError('Complete public historical catalog view required')
            if str(catalog['catalogRevision'])!=revision:raise ValueError('Original catalog revision differs')
            definitions={};models={};elements={};relationships={}
            for entry in catalog['definitions']:
                ref=entry['reference'];owner=ref['owner'];key=(ref['kind'],owner['documentId'],owner['moduleId'],ref.get('typeId'),ref['authoredIdentity'])
                if key in definitions:raise ValueError('Duplicate catalog definition')
                definitions[key]=ref
            for original in report['acceptedInput']['documents']:
                artifact=original['artifact'];model_raw=base64.b64decode(artifact['bytesBase64'],validate=True)
                if hashlib.sha256(model_raw).hexdigest()!=artifact['sha256']:raise ValueError('Original UMF source digest differs')
                model=json.loads(model_raw);doc=model['id']
                if model.get('umf')not in ('0.7.0','0.8.0')or doc in models:raise ValueError('Explicit supported original UMF0.7 or0.8 required')
                models[doc]=(model,model_raw)
                for module in model['modules']:
                    for element in module['elements']:elements[doc,module['id'],element['id']]=element
                    for relation in module.get('relationships',[]):relationships[doc,module['id'],relation['id']]=relation
            types={};qualified={}
            for assigned in report['types']:
                owner=(assigned['documentId'],assigned['moduleId'],assigned['elementId']);record=elements[owner];type_id=assigned['typeId']
                ref=definitions['type',owner[0],owner[1],type_id,owner[2]]
                if record.get('kind')!='record'or type_id in types or owner in qualified:raise ValueError('Original distinct accepted record required')
                fields={};domains={};facets={};required=set();authored={}
                for member in record['members']:
                    field_owner=(owner[0],member['module'],member['element']);field=elements[field_owner]
                    if field.get('kind')!='field'or field.get('scalarType')not in ('string','integer','decimal','boolean')or field.get('cardinality')!='one'or field.get('nullability')not in ('required','absent-allowed'):raise ValueError('Selected exact scalar field declarations required')
                    matches=[p for p in report['properties']if p['typeId']==type_id and p['moduleId']==member['module']and p['elementId']==member['element']]
                    if len(matches)!=1:raise ValueError('Actual accepted field ID required')
                    prop_id=matches[0]['propertyId'];property_ref=definitions['property',owner[0],member['module'],type_id,member['element']]
                    if property_ref['propertyId']!=prop_id:raise ValueError('Actual catalog/report property allocation differs')
                    pin=property_ref['definitionPin']
                    if prop_id in fields:raise ValueError('Duplicate accepted field')
                    domains[prop_id]=(field['scalarType'],field['nullability']);facets[prop_id]=admitted_facets(field)
                    if domains[prop_id][0]!='string':self.typed=True
                    fields[prop_id]=pin;authored[member['module'],member['element']]=prop_id
                    if field['nullability']=='required':required.add(prop_id)
                keys={}
                for key in record.get('keys',[]):
                    components=tuple(authored[member['module'],member['element']]for member in key['fields'])
                    if not components or len(set(components))!=len(components)or key['id']in keys:raise ValueError('Original distinct scalar key components required')
                    if any(domains[pid][0]!='string'for pid in components):raise ValueError('Required STRING keys only')
                    keys[key['id']]=components
                    definitions['key',owner[0],owner[1],type_id,key['id']]
                types[type_id]={'owner':owner,'pin':ref['definitionPin'],'fields':fields,'domains':domains,'facets':facets,'required':required,'keys':keys};qualified[owner]=type_id
            edges={}
            for assigned in report['relationships']:
                owner=(assigned['documentId'],assigned['moduleId'],assigned['relationshipId']);relation=relationships[owner]
                if relation.get('directed')is not True or relation.get('targetLifecycle')!='independent'or len(relation['source'])!=1 or len(relation['target'])!=1:raise ValueError('Selected directed independent binary relationship required')
                endpoints=[]
                for role in ('source','target'):
                    selected=relation[role][0];type_id=qualified[owner[0],selected['module'],selected['element']]
                    if 'key'in selected and selected['key']not in types[type_id]['keys']:raise ValueError('Original declared endpoint key required')
                    endpoints.append(type_id)
                relationship_id=assigned['typeId'];pin=definitions['relationship',owner[0],owner[1],None,owner[2]]
                if pin['relationshipId']!=relationship_id or relationship_id in edges:raise ValueError('Actual accepted relationship ID differs')
                edges[relationship_id]={'owner':owner,'pin':pin['definitionPin'],'endpoints':tuple(endpoints),'declaration':relation}
            self.revisions[revision]={'types':types,'edges':edges,'report':raw,'models':models}
        for raw in self.parsed.event_bytes:
            event=json.loads(raw)
            for image in (event.get('before'),event.get('after')):
                if type(image)is dict and any(p['value'].get('kind')!='string'for p in image.get('retained',[])):self.typed=True
        self.changes=self.parsed.changes
        for raw in self.parsed.event_bytes:
            event=json.loads(raw)
            if event['operation']!='property':self.events[event['xid']+':'+event['seq']]=event
        for change in self.changes:self.admit(change)
    def metadata(self):
        if self.current:
            profile=current_admission_profile(self.current_truss_version)
            if self.current_truss_version == CURRENT_TRUSS_VERSION:
                qualification='Truss toolkit 0.0.44, UMF 0.10.0 accepted source, report 0.6, complete catalog view wire 0.2.0/profile 0.4; original source bytes are fetched and matched through the public accepted-document API. Supports this adapter’s scalar record, STRING key and directed independent binary relationship subset; semantic correspondence only, no publication or ACK authority.'
            elif self.current_truss_version == CURRENT_TRUSS_048_VERSION:
                qualification='Truss toolkit 0.0.48 public wheel and catalog runtime manifest are pinned; UMF 0.10.0 accepted source, report 0.6, complete catalog view wire 0.2.0/profile 0.4. Original source bytes are fetched and matched through the public accepted-document API. Supports this adapter’s scalar record, STRING key and directed independent binary relationship subset; semantic correspondence only. This profile has bounded local public-interface evidence only; no feed, publication, ACK or production authority.'
            else:
                qualification='Truss toolkit 0.0.45 public wheel and catalog runtime manifest are pinned; UMF 0.10.0 accepted source, report 0.6, complete catalog view wire 0.2.0/profile 0.4. Original source bytes are fetched and matched through the public accepted-document API. Supports this adapter’s scalar record, STRING key and directed independent binary relationship subset; semantic correspondence only. The 0.0.45 profile has effect-free public-interface qualification only; no native source acceptance, publication or ACK authority.'
        else:
            profile=TYPED_PROFILE if self.typed else PROFILE
            qualification=('Original accepted declared UMF0.7/0.8 exact typed scalar records with required STRING keys and native empty-property directed independent edges; semantic correspondence only, current source authority supplied by actual Truss sessions.'if self.typed else 'Original accepted declared UMF0.7/0.8 rootless STRING records and native empty-property directed independent edges; semantic correspondence only, current source authority supplied by actual Truss sessions.')
        return {'profile':profile,'source_profile':self.batch.profile,'feed':self.batch.feed,'epoch':self.batch.epoch,'source_transaction_sha256':hashlib.sha256(self.batch.begin).hexdigest(),'accepted_reports':{revision:hashlib.sha256(value['report']).hexdigest()for revision,value in self.revisions.items()},'catalog_views':{revision:hashlib.sha256(raw).hexdigest()for revision,raw in self.catalog_views.items()},'declared_umf':{revision:{doc:model[0]['umf']for doc,model in value['models'].items()}for revision,value in self.revisions.items()},'qualification':qualification}
    def admit(self,change):
        if change not in self.changes:raise PermissionError('Exact original native change required')
        event=self.events[change.delivery_id];image=event['before']if event['operation']=='delete'else event['after'];selected=self.revisions.get(image['catalogRevision'])
        if selected is None:raise PermissionError('Original image catalog interpretation required')
        identity=image['identity'];type_id=str(change.state.key.type_id)
        if change.state.key.kind=='object':
            record=selected['types'].get(type_id)
            if record is None or identity.get('definitionPin')!=record['pin']or identity.get('owner')!={'documentId':record['owner'][0],'moduleId':record['owner'][1]}:raise PermissionError('Actual accepted object pin required')
            props={p['propertyId']:p for p in image['properties']}
            if not record['required']<=set(props)<=set(record['fields']):raise PermissionError('Complete accepted field presence required')
            if any(not set(components)<=set(props)for components in record['keys'].values()):raise PermissionError('Complete declared scalar key values required')
            if any(prop['definitionPin']!=record['fields'][prop_id]for prop_id,prop in props.items()):raise PermissionError('Actual property definition pin differs')
            for pid,prop in props.items():
                family,nullability=record['domains'][pid];kind=prop['value']['kind']
                if kind!=family:raise PermissionError('Original scalar family/nullability differs')
                require_facet_value(family,record['facets'][pid],prop['value'])
        else:
            edge=selected['edges'].get(type_id)
            if edge is None or identity.get('definitionPin')!=edge['pin']or identity.get('owner')!={'documentId':edge['owner'][0],'moduleId':edge['owner'][1]}or tuple(str(key.type_id)for key in change.state.endpoints)!=edge['endpoints']:raise PermissionError('Actual accepted relationship pin/endpoints required')
            for role,endpoint in zip(('source','target'),edge['endpoints']):
                if image[role].get('definitionPin')!=selected['types'][endpoint]['pin']or image[role].get('owner')!={'documentId':selected['types'][endpoint]['owner'][0],'moduleId':selected['types'][endpoint]['owner'][1]}:raise PermissionError('Actual endpoint type pin differs')
    def admit_transition(self,previous,change):
        self.admit(change)
        if previous.key!=change.state.key or previous.schema_revision not in self.revisions or change.state.schema_revision not in self.revisions:raise PermissionError('Actual accepted graph revision transition required')
        role='types'if change.state.key.kind=='object'else'edges';identity=str(change.state.key.type_id)
        old=self.revisions[previous.schema_revision][role][identity];new=self.revisions[change.state.schema_revision][role][identity]
        if old['owner']!=new['owner']:raise PermissionError('Stable accepted graph identity required')
        if role=='types'and (any(new['fields'].get(field)!=pin for field,pin in old['fields'].items())or any(new['facets'].get(pid)!=facet for pid,facet in old['facets'].items())or old['keys']!=new['keys']or any(new['domains'].get(pid)!=domain for pid,domain in old['domains'].items())):raise PermissionError('Only additive accepted scalar fields supported')
        if role=='edges'and old['endpoints']!=new['endpoints']:raise PermissionError('Stable declared relationship endpoints required')
