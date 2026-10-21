# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure original core0.10 graph binding metadata; no query authority.

Explicit aliases describe physical names only. Their lifetime, original native
identity and source/publication custody must be admitted by a separate owner.
"""
import hashlib,json,re,uuid
from ashlar.typed_graph_source_config import TypedGraphJsonlConfiguration
from ashlar.typed_graph_source import TypedGraphSourcePolicy
from ashlar.schema import decode_retained_json
from ashlar.manifest import manifest_pin_vector
from ashlar.pins import PinVector
from ashlar.weft_binding import LAYOUT_SHA256
from .count_star_admission import CURRENT_BACKEND


def _require(value):
    if not value:raise ValueError('Configured typed graph Weft binding refused')


def configured_typed_graph_count_star_request(configuration,manifest,vector,manifest_uuid,aliases,sql,*,layout_sha256):
    """Build a detached Spark041 request without compiling or interpreting SQL."""
    _require(type(configuration)is TypedGraphJsonlConfiguration and type(configuration.policy)is TypedGraphSourcePolicy)
    configuration.verify()
    _require(type(manifest)is dict and type(vector)is PinVector and layout_sha256==LAYOUT_SHA256)
    original=json.dumps(manifest,ensure_ascii=False,separators=(',',':'),sort_keys=True)
    _require(type(sql)is str and 0<len(sql.encode())<=65536 and '\0'not in sql)
    _require(type(manifest_uuid)is str and str(uuid.UUID(manifest_uuid))==manifest_uuid)
    targets=dict(vector.targets);names=sorted(targets)
    roles={'object_current','edge_current','tombstone','whole_source_history'}
    _require(len(names)==4 and {n.split('.')[-1]for n in names}==roles)
    ids={name:pair[0]for name,pair in targets.items()}
    _require(all(type(v)is str and str(uuid.UUID(v))==v for v in ids.values())and len(set(ids.values())|{manifest_uuid})==5)
    _require(manifest_pin_vector(manifest,ids,authority=vector.authority)==vector)
    _require(manifest['profile_version']=='ashlar-delta/0.3'
        and decode_retained_json(manifest['schema_revisions_json'].encode())=={configuration.schema_alias:configuration.document_revision}
        and set(decode_retained_json(manifest['source_progress_json'].encode()))=={configuration.feed})
    _require(type(aliases)is dict and set(aliases)==set(names)and all(type(v)is str for v in aliases.values())and len(set(aliases.values()))==4)
    namespace=None
    for name in names:
        alias=aliases[name]
        _require(re.fullmatch(r'spark_catalog\.ashlar_configured_query_[A-Za-z0-9_]{1,96}\.[A-Za-z_][A-Za-z0-9_]*',alias)is not None)
        parts=alias.split('.');_require(parts[-1]==name.split('.')[-1]and alias!=name)
        if namespace is None:namespace=parts[1]
        _require(parts[1]==namespace)
    model=decode_retained_json(configuration.model_source)
    elements={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    pin={'documentId':model['id'],'revision':configuration.document_revision,'umfVersion':'0.10.0','sha256':configuration.model_sha256}
    def logical(module,element):return dict(documentId=model['id'],revision=configuration.document_revision,module=module,element=element)
    records=[];object_index=next(i for i,n in enumerate(names)if n.endswith('.object_current'))
    for tid,bound in sorted(configuration.bindings.items()):
        module,element=bound['record'];record=elements[module,element]
        homes={(v[0],v[1]):pid for pid,v in bound['fields'].items()};properties=[]
        for member in record['members']:
            key=(member['module'],member['element']);home={'kind':'props','propertyId':homes[key]}
            if elements[key].get('nullability')=='absent-allowed':home['encoding']='ashlar-weft-json-native-null/0.1-candidate'
            properties.append({'logical':logical(*key),'home':home})
        records.append({'kind':'object','logical':logical(module,element),'sourceSystem':configuration.source_system,'typeId':str(tid),
            'table':object_index,'schemaRevision':configuration.document_revision,'properties':properties})
    definitions={(m['id'],r['id']):r for m in model['modules']for r in m.get('relationships',[])}
    record_map={r['typeId']:r['logical']for r in records}
    edge_index=next(i for i,n in enumerate(names)if n.endswith('.edge_current'))
    relationships=[]
    for tid,bound in sorted(configuration.relationships.items()):
        module,relationship=bound['identity']
        source=record_map[str(bound['source_type_id'])];target=record_map[str(bound['target_type_id'])]
        relationships.append({'logical':dict(documentId=model['id'],revision=configuration.document_revision,module=module,relationship=relationship),
            'acceptedDefinition':definitions[module,relationship],'table':edge_index,'kind':'edge','sourceSystem':configuration.source_system,
            'typeId':str(tid),'schemaRevision':configuration.document_revision,'source':source,'target':target})
    binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3','layoutSha256':layout_sha256,'modelPins':[pin],
        'publication':{'id':manifest['publication_id'],'manifestUuid':manifest_uuid,'tables':[{'name':aliases[n].split('.'),'uuid':targets[n][0],'version':targets[n][1]}for n in names]},'records':records,'relationships':relationships}
    raw=json.dumps(binding,ensure_ascii=False,separators=(',',':'),sort_keys=True)
    request={'interfaceVersion':'weft-compile/0.4.1','dialect':'weft-sql/0.4.1','sql':sql,
        'modules':[{'documentJson':configuration.model_source.decode('utf-8'),'pin':pin,'selectedModuleIds':sorted({r['logical']['module']for r in records+relationships})}],
        'target':{k:CURRENT_BACKEND[k]for k in ('backendId','backendVersion','targetProfile')},'options':{'allowCandidate':True}}
    request['target'].update(bindingJson=raw,bindingSha256=hashlib.sha256(raw.encode()).hexdigest())
    configuration.verify();_require(json.dumps(manifest,ensure_ascii=False,separators=(',',':'),sort_keys=True)==original)
    return request
