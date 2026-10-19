# SPDX-License-Identifier: MIT OR Apache-2.0
"""Note semantics from original native pages and accepted catalog reports.

This port admits exact semantic correspondence only. NativeDriver's concrete
Truss source sessions must independently establish current source authority.
"""
import base64,hashlib,json
from ashlar.staging import batch_row
from ashlar.truss_native_feed import scalar_object_changes

class TrussNoteAdmission:
    def __init__(self,batch):
        batch_row(batch)
        if batch.profile!='truss-native-feed/0.1':raise ValueError('Original native Truss batch required')
        self.batch=batch
        self.parsed=scalar_object_changes(batch.begin,sha256=hashlib.sha256(batch.begin).hexdigest(),prior=base64.b64decode(batch.cursor_before,validate=True),next=base64.b64decode(batch.cursor_after,validate=True),feed=batch.feed)
        payload=json.loads(batch.begin);self.revisions={}
        for prerequisite in payload['catalogPrerequisites']:
            encoded=prerequisite.get('reportBytesBase64')
            if encoded is None:continue # Actual genesis revision has no report.
            raw=base64.b64decode(encoded,validate=True)
            if hashlib.sha256(raw).hexdigest()!=prerequisite['reportSha256']:raise ValueError('Original accepted report digest differs')
            report=json.loads(raw);revision=report['revision']
            if revision!=prerequisite['revision']or report['installationId']!=json.loads(base64.b64decode(batch.cursor_before))['installationId']:raise ValueError('Actual report installation/revision differs')
            types=[t for t in report['types']if (t['documentId'],t['moduleId'],t['elementId'])==('ashlar-note','notes','Note')]
            if len(types)!=1:raise ValueError('Actual accepted Note type required')
            documents=[entry for entry in report['acceptedInput']['documents']if entry['artifact']['identity']=='ashlar-note']
            if len(documents)!=1:raise ValueError('Original accepted Note document required')
            artifact=documents[0]['artifact'];document_bytes=base64.b64decode(artifact['bytesBase64'],validate=True)
            if hashlib.sha256(document_bytes).hexdigest()!=artifact['sha256']:raise ValueError('Original accepted Note source digest differs')
            model=json.loads(document_bytes)
            if model.get('umf')!='0.7.0'or model.get('id')!='ashlar-note':raise ValueError('Supported original Note UMF version required')
            modules=[module for module in model['modules']if module['id']=='notes']
            if len(modules)!=1:raise ValueError('Original Note module required')
            elements={element['id']:element for element in modules[0]['elements']}
            for field in ('note-id','note-text','note-tag'):
                if field=='note-tag'and field not in elements:continue
                selected=elements.get(field,{})
                if selected.get('kind')!='field'or selected.get('scalarType')!='string'or selected.get('cardinality')!='one'or selected.get('nullability')!=('absent-allowed'if field=='note-tag'else'required'):raise ValueError('Original supported Note field declaration required')
            record=elements.get('Note',{})
            if record.get('kind')!='record'or {member['element']for member in record['members']}!=set(elements)-{'Note'}:raise ValueError('Original complete Note record membership required')
            keys=record.get('keys')
            if type(keys)is not list or len(keys)!=1 or keys[0].get('primary')is not True or keys[0].get('fields')!=[{'module':'notes','element':'note-id'}]:raise ValueError('Original Note primary key required')
            type_id=types[0]['typeId'];properties={p['elementId']:p['propertyId']for p in report['properties']if p['typeId']==type_id and p['moduleId']=='notes'}
            if set(properties)!=set(elements)-{'Note'}:raise ValueError('Accepted Note fields differ from original source')
            if set(properties)not in ({'note-id','note-text'},{'note-id','note-text','note-tag'})or len(set(properties.values()))!=len(properties):raise ValueError('Exact supported accepted Note properties required')
            if revision in self.revisions:raise ValueError('Duplicate accepted revision')
            self.revisions[revision]=(int(type_id),properties,raw)
        self.changes=self.parsed.changes
        for change in self.changes:self.admit(change)
    def metadata(self):
        return {'profile':'ashlar-truss-note-semantic-admission/0.1','source_profile':self.batch.profile,'feed':self.batch.feed,'epoch':self.batch.epoch,'source_transaction_sha256':hashlib.sha256(self.batch.begin).hexdigest(),'accepted_reports':{revision:hashlib.sha256(value[2]).hexdigest()for revision,value in self.revisions.items()},'qualification':'Exact scalar Note semantic correspondence; current authority supplied separately by actual Truss source sessions.'}
    def admit(self,change):
        if change not in self.changes:raise PermissionError('Exact original native change required')
        selected=self.revisions.get(change.state.schema_revision)
        if selected is None or change.state.key.kind!='object'or change.state.key.type_id!=selected[0]:raise PermissionError('Actual accepted Note revision/type required')
        props=json.loads(change.state.props_json);fields=selected[1]
        if not {fields['note-id'],fields['note-text']}<=set(props)<=set(fields.values())or any(type(value)is not str for value in props.values()):raise PermissionError('Complete required original Note STRING values required')
    def admit_transition(self,previous,change):
        self.admit(change)
        if previous.key!=change.state.key or previous.schema_revision not in self.revisions:raise PermissionError('Actual previous accepted Note revision required')
        old=self.revisions[previous.schema_revision];new=self.revisions[change.state.schema_revision]
        if old[0]!=new[0]or any(new[1].get(field)!=identity for field,identity in old[1].items()):raise PermissionError('Compatible stable accepted Note identity required')
