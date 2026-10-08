"""Assemble source/effect report parts from the selected native candidate probe.

These are unregistered candidate parts, not an accepted report. Original current
execution/authority/origin registration and protected persistence are separate.
"""
import base64
import hashlib
import json
from .assertions import selected_assertion_inventory
from .binding import plan_string_record_binding
from .catalog import Identity, MappingEntry
from .lineage import lineage_bytes, TYPE
from .schema import _json
from .semantic_policy import StringRecordPolicy
from .truss_input import AcceptanceInputCustody, artifact

class ReportPartsError(ValueError):
    pass

DIAGNOSTIC_PROFILE_BYTES=b'{"status":"unregistered candidate","scope":"one upstream_validation entry retains the complete original ashlar-schema-intake/0.1 producer artifact and its entire native validation/diagnostic set without extraction or reserialization"}'
DIAGNOSTIC_PROFILE={'identity':'ashlar-umf-original-validation-receipt','version':'0.1','sha256':hashlib.sha256(DIAGNOSTIC_PROFILE_BYTES).hexdigest()}
INTERPRETATION_PROFILE_BYTES=b'{"status":"unregistered candidate","scope":"complete original ashlar-umf-interpretation/0.1 artifact; completeness is the original validation.complete flag; retain unavailable API diagnostics without relabeling them as invalid source"}'
INTERPRETATION_PROFILE={'identity':'ashlar-umf-original-interpretation-receipt','version':'0.1','sha256':hashlib.sha256(INTERPRETATION_PROFILE_BYTES).hexdigest()}
NATIVE_FIELDS={'phase','head','original_request','document','types','properties','other_counts','report_count'}
OTHER={'keys','relationships','endpoints','schema_changes','journal','objects','edges'}
TYPE_FIELDS={'document_id','type_id','module','element','kind','since_rev','doc_ord','retired_rev','provisional','lineage_profile','lineage_hex','lineage_sha256','definition_source_kind','definition_rev','definition_doc_ord','definition_document_id','binding_source_rev','binding_source_pointer','binding_source_bytes'}
PROP_FIELDS={'prop_id','type_id','element','name','scalar_type','nullability','cardinality','home','since_rev','doc_ord','retired_rev','facets','item','definition_source_kind','definition_rev','definition_doc_ord','definition_document_id','binding_source_rev','binding_source_pointer','binding_source_bytes'}


def _id(value):
    if type(value) is not int or not 1<=value<2**31:raise ReportPartsError('Exact native positive catalog ID required')
    return value


def _exact(a,b):
    if type(a) is not type(b):return False
    if type(a) is dict:return set(a)==set(b) and all(_exact(a[k],b[k]) for k in a)
    if type(a) is list:return len(a)==len(b) and all(_exact(x,y) for x,y in zip(a,b))
    return a==b


def _provenance(row,did):
    if any(not _exact(row[k],v) for k,v in {'since_rev':1,'doc_ord':0,'retired_rev':None,'definition_source_kind':'accepted_document','definition_rev':1,'definition_doc_ord':0,'definition_document_id':did,'binding_source_rev':None,'binding_source_pointer':None,'binding_source_bytes':None}.items()):
        raise ReportPartsError('Candidate definition provenance differs')


def initial_candidate_report_parts(intake,interpretation,request,native):
    inventory=selected_assertion_inventory(intake,interpretation)
    plan=plan_string_record_binding(interpretation,trusted_validator_revision=intake.validator_revision)
    if not isinstance(request,AcceptanceInputCustody) or request.document_sources!=(intake.source,):raise ReportPartsError('Original request source custody required')
    if type(native) is not dict or set(native)!=NATIVE_FIELDS or native.get('phase')!='candidate' or type(native.get('head')) is not int or native['head']!=0 or type(native.get('report_count')) is not int or native['report_count']!=0:
        raise ReportPartsError('Initial unpublished candidate observation required')
    original=native.get('original_request',{})
    if not _exact(original,{'probe':'rollback-only','acceptanceInputSha256':request.sha256,'acceptanceInputPreimageBase64':base64.b64encode(request.canonical_preimage).decode('ascii'),'originalInputBase64':base64.b64encode(request.original).decode('ascii')}):
        raise ReportPartsError('Native original request correspondence differs')
    validation=_json(intake.artifact)['validation']
    if not _exact(_json(interpretation)['validation'],validation):raise ReportPartsError('Producer validation/diagnostic inventories disagree')
    expected_doc={'doc_id':intake.document_id,'doc_revision':intake.document_revision,'umf_version':'0.7.0','content_sha256':intake.source_sha256,'document':intake.source.decode('utf-8'),'validation':validation}
    if not _exact(native.get('document'),expected_doc):raise ReportPartsError('Native original document/validation differs')
    other=native.get('other_counts')
    if type(other) is not dict or set(other)!=OTHER or any(type(v) is not int or v!=0 for v in other.values()):
        raise ReportPartsError('Unobserved or nonempty additional effect scope')
    types=native.get('types');props=native.get('properties')
    if type(types) is not list or type(props) is not list or len(types)!=len(plan['types']) or len(props)!=len(plan['properties']):raise ReportPartsError('Incomplete native definition inventories')
    wanted_types={tuple(t['identity']) for t in plan['types']};type_ids={};entries=[]
    for row in types:
        if type(row) is not dict or set(row)!=TYPE_FIELDS:raise ReportPartsError('Unknown or missing native type columns')
        key=(row['document_id'],row['module'],row['element']);tid=_id(row['type_id'])
        if key not in wanted_types or key in type_ids or tid in type_ids.values():raise ReportPartsError('Foreign/duplicate native type identity')
        _provenance(row,intake.document_id);raw=lineage_bytes(TYPE,list(key))
        if row['kind']!='record' or row['provisional'] is not False or row['lineage_profile']!=TYPE or row['lineage_hex']!=raw.hex() or row['lineage_sha256']!=hashlib.sha256(raw).hexdigest():raise ReportPartsError('Native kind/lineage differs')
        type_ids[key]=tid;entries.append(MappingEntry(Identity('type',key),tid,True))
    wanted_props={}
    for prop in plan['properties']:
        identity=tuple(prop['identity']);key=(type_ids[identity[:3]],identity[4])
        if key in wanted_props:raise ReportPartsError('Selected native property identity ambiguous')
        wanted_props[key]=prop
    seen=set();ids=set()
    for row in props:
        if type(row) is not dict or set(row)!=PROP_FIELDS:raise ReportPartsError('Unknown or missing native property columns')
        key=(_id(row['type_id']),row['element']);pid=_id(row['prop_id'])
        if key not in wanted_props or key in seen or pid in ids:raise ReportPartsError('Foreign/duplicate native property identity')
        seen.add(key);ids.add(pid);prop=wanted_props[key];_provenance(row,intake.document_id)
        expected={'name':prop['displayName'] if prop['displayName'] is not None else prop['identity'][4],
            'scalar_type':'string','nullability':prop['nullability']['nullability'],'cardinality':'one','home':'json','facets':None,'item':None}
        if any(not _exact(row[k],v) for k,v in expected.items()):raise ReportPartsError('Native property meaning differs')
        entries.append(MappingEntry(Identity('property',tuple(prop['identity'])),pid,True))
    StringRecordPolicy.from_intake(intake,interpretation,entries,source_system='truss-native-candidate')
    source=artifact('original-source:'+intake.source_sha256,intake.source)
    return {'format':'ashlar-truss-initial-report-parts/0.1','status':'unregistered candidate; unpublished',
        'documents':[{'doc_id':intake.document_id,'doc_revision':intake.document_revision,'content_sha256':intake.source_sha256,'ord':'0'}],
        'diagnostics':[{'classification':'upstream_validation','source':{'kind':'document','artifact':source,'sourcePointer':''},'diagnosticProfile':dict(DIAGNOSTIC_PROFILE),'diagnostic':artifact('original-umf-validation-receipt',intake.artifact)}],
        'documentInterpretations':[{'documentId':intake.document_id,'contentSha256':intake.source_sha256,'interpretationProfile':dict(INTERPRETATION_PROFILE),'completeness':'complete' if intake.complete_interpretation else 'partial','evidence':artifact('original-umf-interpretation-receipt',interpretation)}],
        'counts':{'typesAdded':str(len(types)),'propertiesAdded':str(len(props)),'keysAdded':'0','relationshipsAdded':'0','endpointsAdded':'0','elementsRetired':'0'},
        'assertionInventory':inventory,'originalRequest':artifact('original-acceptance-input',request.original),
        'profileArtifacts':[artifact(DIAGNOSTIC_PROFILE['identity'],DIAGNOSTIC_PROFILE_BYTES),artifact(INTERPRETATION_PROFILE['identity'],INTERPRETATION_PROFILE_BYTES)],
        'nativeObservation':artifact('candidate-observation-projection',json.dumps(native,ensure_ascii=False,separators=(',',':')).encode()),
        'qualification':'Source/diagnostic/interpretation and initial candidate effect correspondence only. Missing registered execution/origin/report/lifecycle contexts, complete protected producer and accepted report/head publication; these parts confer no ID or ingestion authority.'}
