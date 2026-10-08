"""Selected source assertion inventory for report-producer development.

No entry receives database/engine qualification here. The original source,
interpretation, diagnostics and target gaps remain retained separately.
"""
import hashlib
import json
from .binding import plan_string_record_binding
from .schema import SchemaIntake, _json
from .truss_input import artifact

class AssertionInventoryError(ValueError):
    pass

MAX_SOURCE_BYTES = 65536
MAX_RECEIPT_BYTES = 1048576
MAX_ASSERTIONS = 256
MAX_INVENTORY_BYTES = 1048576

PROFILE = {
    'identity': 'ashlar-string-record-assertion-source',
    'version': '0.1',
    'status': 'unregistered candidate',
    'scope': 'explicit Record kind and each member; explicit Field kind/scalarType/nullability/cardinality; original source must pass selected binding closure',
    'definitionPin': 'SHA256 of the complete original source bytes; sourcePointer resolves the assertion in that exact document',
    'bounds': 'source <=65536 bytes; each receipt <=1048576 bytes; <=256 assertion entries; identity inventory <=1048576 bytes',
    'classification': 'none/unqualified only; this producer supplies no enforcement qualification',
}
PROFILE_BYTES = json.dumps(PROFILE,sort_keys=True,separators=(',',':')).encode('utf-8')
PROFILE_PIN = {'identity':PROFILE['identity'],'version':PROFILE['version'],'sha256':hashlib.sha256(PROFILE_BYTES).hexdigest()}


def selected_assertion_inventory(intake, interpretation):
    if not isinstance(intake,SchemaIntake):raise AssertionInventoryError('Original intake required')
    if type(interpretation) is not bytes or len(interpretation)>MAX_RECEIPT_BYTES or type(intake.source) is not bytes or len(intake.source)>MAX_SOURCE_BYTES or type(intake.artifact) is not bytes or len(intake.artifact)>MAX_RECEIPT_BYTES:
        raise AssertionInventoryError('Selected source/receipt resource bound exceeded')
    recovered=SchemaIntake.read(intake.artifact,intake.document_revision,trusted_validator_revision=intake.validator_revision)
    if recovered!=intake:raise AssertionInventoryError('Altered original intake custody')
    plan=plan_string_record_binding(interpretation,trusted_validator_revision=intake.validator_revision)
    if plan['sourceSha256']!=intake.source_sha256:raise AssertionInventoryError('Interpretation belongs to another original source')
    if plan['status']!='candidate' or plan['blocked']:
        raise AssertionInventoryError('Unsupported assertion closure; no complete selected inventory')
    source=_json(intake.source)
    original=artifact('original-source:'+intake.source_sha256,intake.source)
    definitions={}
    for mi,module in enumerate(source['modules']):
        for ei,element in enumerate(module['elements']):
            definitions[(source['id'],module['id'],element['id'])]=(element,f'/modules/{mi}/elements/{ei}')
    rows={}
    def add(identity,path,rule):
        key=(tuple(identity),path,rule)
        row={'assertion':{'kind':'source','sourceKind':'umf_document',
            'owner':{'documentId':identity[0],'moduleId':identity[1]},
            'definitionPin':intake.source_sha256,'sourcePointer':path,'sourceIdentityProfile':dict(PROFILE_PIN)},
            'source':dict(original),'ruleName':rule,'ruleNameOrigin':'profile_generated',
            'enforcement':'none','reason':'unqualified'}
        if key in rows and rows[key]!=row:raise AssertionInventoryError('Ambiguous source assertion')
        if key not in rows and len(rows)>=MAX_ASSERTIONS:
            raise AssertionInventoryError('Assertion count bound exceeded; no partial inventory')
        rows[key]=row
    for record in plan['types']:
        identity=tuple(record['identity']);element,path=definitions[identity]
        add(identity,path+'/kind','record.kind')
        for i,_member in enumerate(element.get('members',[])):
            add(identity,path+'/members/'+str(i),'record.member')
    for prop in plan['properties']:
        owner=prop['identity'];identity=(owner[0],owner[3],owner[4]);element,path=definitions[identity]
        for field in ['kind','scalarType','nullability','cardinality']:
            if field not in element:raise AssertionInventoryError('Selected explicit assertion missing')
            add(identity,path+'/'+field,'field.'+field)
    ordered=[row for key,row in sorted(rows.items(),key=lambda pair:tuple(v.encode('utf-8') for v in pair[0][0])+(pair[0][1].encode('utf-8'),pair[0][2].encode('utf-8')))]
    # Inventory hash covers identities/source correspondence, independent of future
    # evidence classification. It is a proposed artifact hash, not registration.
    inventory=[{'assertion':r['assertion'],'source':r['source'],'ruleName':r['ruleName'],'ruleNameOrigin':r['ruleNameOrigin']} for r in ordered]
    if sum(len(json.dumps(r,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8'))+1 for r in inventory)+2>MAX_INVENTORY_BYTES:
        raise AssertionInventoryError('Inventory byte bound exceeded; no partial inventory')
    inventory_bytes=json.dumps(inventory,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')
    return {'format':'ashlar-selected-assertion-inventory/0.1','status':'unregistered candidate',
        'sourceSha256':intake.source_sha256,'interpretationSha256':hashlib.sha256(interpretation).hexdigest(),
        'profile':dict(PROFILE_PIN),'profileArtifact':artifact(PROFILE['identity'],PROFILE_BYTES),
        'assertionInventorySha256':hashlib.sha256(inventory_bytes).hexdigest(),
        'inventoryArtifact':artifact('selected-assertion-inventory',inventory_bytes),'entries':ordered,
        'originalIntake':artifact('original-intake',intake.artifact),
        'originalInterpretation':artifact('original-interpretation',interpretation),
        'completeInterpretation':intake.complete_interpretation,
        'qualification':'Complete explicit assertion inventory for this selected candidate source profile only. Not an admitted Truss complete enforcement report, registered assertion/profile authority, native/engine enforcement or accepted schema.'}
