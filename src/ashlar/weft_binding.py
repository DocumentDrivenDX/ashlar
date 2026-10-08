"""Owner-authored string Record bindings for pinned Weft; no execution authority."""
import hashlib
import json
import re
from .schema import SchemaIntake
from .semantic_policy import StringRecordPolicy
from .publication import Descriptor

WEFT_REVISION='2744531735c2a771fbe7ed24a7f67e3afc851b25'
LAYOUT_SHA256='ad4a264508c971aefcd94e3ae90f8f74dcf119b7d767f6060c638f4abde3284e'

class WeftBindingError(ValueError):
    pass


def string_compile_request(sql,intake,policy,descriptor,*,schema_alias,table_uuids,manifest_uuid,layout_sha256,dialect='0.2.0'):
    """Bind exact owner model/IDs to consumed published object storage.

    Host must independently resolve manifest/UUIDs/retention, enforce caller
    authorization, validate native schemas/payloads and obey all compiler checks.
    This function grants none of that authority and invents no typed homes/keys.
    """
    if not isinstance(intake,SchemaIntake) or not isinstance(policy,StringRecordPolicy) or not isinstance(descriptor,Descriptor):
        raise WeftBindingError('Original intake, binding policy and publication descriptor required')
    recovered=SchemaIntake.read(intake.artifact,intake.document_revision,trusted_validator_revision=intake.validator_revision)
    if recovered!=intake or policy.source_sha256!=intake.source_sha256 or policy.document_revision!=intake.document_revision:
        raise WeftBindingError('Original intake and selected binding differ')
    if descriptor.profile!='ashlar-delta/0.3' or layout_sha256!=LAYOUT_SHA256 or descriptor.revisions.get(schema_alias)!=intake.document_revision:
        raise WeftBindingError('Exact layout and selected model revision required')
    if dialect not in ('0.1.0','0.2.0') or not isinstance(sql,str) or not sql:
        raise WeftBindingError('Explicit supported Weft query required')
    uuid=lambda v:isinstance(v,str) and re.fullmatch('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',v)
    if not uuid(manifest_uuid) or set(table_uuids)!=set(descriptor.versions) or not all(uuid(v) for v in table_uuids.values()):
        raise WeftBindingError('Complete original canonical native table UUID inventory required')
    names=sorted(descriptor.versions);tables=[]
    for name in names:
        parts=name.split('.');version=descriptor.versions[name]
        if len(parts)!=3 or any(not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*',p) for p in parts) or type(version) is not int or not 0<=version<2**63:
            raise WeftBindingError('Exact qualified table/version required')
        tables.append({'name':parts,'uuid':table_uuids[name],'version':version})
    objects=[i for i,t in enumerate(tables) if t['name'][-1]=='object_current']
    if len(objects)!=1:raise WeftBindingError('One admitted object carrier required')
    pin={'documentId':intake.document_id,'revision':intake.document_revision,'umfVersion':'0.7.0','sha256':intake.source_sha256}
    logical=lambda module,element:dict(documentId=intake.document_id,revision=intake.document_revision,module=module,element=element)
    records=[]
    for type_id,(module,element) in sorted(policy.record_identities.items()):
        properties=[{'logical':logical(*field),'home':{'kind':'props','propertyId':pid}} for pid,field in sorted(policy.property_fields[type_id].items())]
        records.append({'logical':logical(module,element),'table':objects[0],'kind':'object','sourceSystem':policy.source_system,'typeId':str(type_id),'schemaRevision':policy.schema_revision,'properties':properties})
    binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3','layoutSha256':layout_sha256,'modelPins':[pin],
        'publication':{'id':descriptor.publication_id,'manifestUuid':manifest_uuid,'tables':tables},'records':records}
    text=json.dumps(binding,ensure_ascii=False,separators=(',',':'),sort_keys=True)
    return {'interfaceVersion':'weft-compile/'+dialect,'dialect':'weft-sql/'+dialect,'sql':sql,
        'modules':[{'documentJson':intake.source.decode('utf-8'),'pin':pin,'selectedModuleIds':sorted({r['logical']['module'] for r in records})}],
        'target':{'backendId':'ashlar.databricks','backendVersion':'0.1.0-qualified','targetProfile':'dbsql2026.39-qualified','bindingJson':text,'bindingSha256':hashlib.sha256(text.encode()).hexdigest()},'options':{'allowCandidate':False}}
