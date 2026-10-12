"""Accepted scalar graph correspondence; current Truss authority stays session-owned."""
import base64,hashlib,json
from ashlar.staging import batch_row
from ashlar.truss_native_feed import scalar_entity_changes

PROFILE='ashlar-truss-scalar-graph-semantic-admission/0.1'

class TrussGraphAdmission:
    def __init__(self,batch,catalog_views):
        batch_row(batch)
        if batch.profile!='truss-native-feed/0.1'or type(catalog_views)is not dict:raise ValueError('Original native page and explicit original catalog views required')
        self.batch=batch;self.parsed=scalar_entity_changes(batch.begin,sha256=hashlib.sha256(batch.begin).hexdigest(),prior=base64.b64decode(batch.cursor_before,validate=True),next=base64.b64decode(batch.cursor_after,validate=True),feed=batch.feed)
        payload=json.loads(batch.begin);self.revisions={};self.events={};self.catalog_views=dict(catalog_views)
        installation=json.loads(base64.b64decode(batch.cursor_before))['installationId']
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
                if model.get('umf')!='0.7.0'or doc in models:raise ValueError('Explicit supported original UMF 0.7.0 required')
                models[doc]=(model,model_raw)
                for module in model['modules']:
                    for element in module['elements']:elements[doc,module['id'],element['id']]=element
                    for relation in module.get('relationships',[]):relationships[doc,module['id'],relation['id']]=relation
            types={};qualified={}
            for assigned in report['types']:
                owner=(assigned['documentId'],assigned['moduleId'],assigned['elementId']);record=elements[owner];type_id=assigned['typeId']
                ref=definitions['type',owner[0],owner[1],type_id,owner[2]]
                if record.get('kind')!='record'or type_id in types or owner in qualified:raise ValueError('Original distinct accepted record required')
                fields={};required=set();authored={}
                for member in record['members']:
                    field_owner=(owner[0],member['module'],member['element']);field=elements[field_owner]
                    if field.get('kind')!='field'or field.get('scalarType')!='string'or field.get('cardinality')!='one'or field.get('nullability')not in ('required','absent-allowed'):raise ValueError('Selected scalar STRING field declarations required')
                    matches=[p for p in report['properties']if p['typeId']==type_id and p['moduleId']==member['module']and p['elementId']==member['element']]
                    if len(matches)!=1:raise ValueError('Actual accepted field ID required')
                    prop_id=matches[0]['propertyId'];property_ref=definitions['property',owner[0],member['module'],type_id,member['element']]
                    if property_ref['propertyId']!=prop_id:raise ValueError('Actual catalog/report property allocation differs')
                    pin=property_ref['definitionPin']
                    if prop_id in fields:raise ValueError('Duplicate accepted field')
                    fields[prop_id]=pin;authored[member['module'],member['element']]=prop_id
                    if field['nullability']=='required':required.add(prop_id)
                keys={}
                for key in record.get('keys',[]):
                    components=tuple(authored[member['module'],member['element']]for member in key['fields'])
                    if not components or len(set(components))!=len(components)or key['id']in keys:raise ValueError('Original distinct scalar key components required')
                    keys[key['id']]=components
                    definitions['key',owner[0],owner[1],type_id,key['id']]
                types[type_id]={'owner':owner,'pin':ref['definitionPin'],'fields':fields,'required':required,'keys':keys};qualified[owner]=type_id
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
        self.changes=self.parsed.changes
        for raw in self.parsed.event_bytes:
            event=json.loads(raw)
            if event['operation']!='property':self.events[event['xid']+':'+event['seq']]=event
        for change in self.changes:self.admit(change)
    def metadata(self):
        return {'profile':PROFILE,'source_profile':self.batch.profile,'feed':self.batch.feed,'epoch':self.batch.epoch,'source_transaction_sha256':hashlib.sha256(self.batch.begin).hexdigest(),'accepted_reports':{revision:hashlib.sha256(value['report']).hexdigest()for revision,value in self.revisions.items()},'catalog_views':{revision:hashlib.sha256(raw).hexdigest()for revision,raw in self.catalog_views.items()},'declared_umf':{revision:{doc:model[0]['umf']for doc,model in value['models'].items()}for revision,value in self.revisions.items()},'qualification':'Original accepted UMF 0.7.0 rootless STRING records and native empty-property directed independent edges; semantic correspondence only, current source authority supplied by actual Truss sessions.'}
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
        if role=='types'and (any(new['fields'].get(field)!=pin for field,pin in old['fields'].items())or old['keys']!=new['keys']):raise PermissionError('Only additive accepted scalar fields supported')
        if role=='edges'and old['endpoints']!=new['endpoints']:raise PermissionError('Stable declared relationship endpoints required')
