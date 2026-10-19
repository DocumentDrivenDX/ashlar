# SPDX-License-Identifier: MIT OR Apache-2.0
"""Publication-derived local staging; declared query profile is not a UMF grant.

Only the original commerce products/sku subset is selected. Original complete
publication and ordinary source/ACK authority remain owned by the held reader.
"""
from __future__ import annotations
from collections import Counter
from dataclasses import dataclass
import hashlib
import json
import re
from .path_capture import PathCaptureConfig, capture_string_frame

PROFILE = 'ashlar.publication-commerce-current/0.1'

def _text(value):
    return json.dumps(value,ensure_ascii=False,separators=(',', ':'),allow_nan=False)

def _sha(raw): return hashlib.sha256(raw).hexdigest()

def commerce_current_rows(opened):
    """Derive exact identity/STRING cells from the admitted complete population."""
    document=json.loads(opened.model)
    if document.get('umf')!='0.8.0': raise ValueError('Original UMF 0.8 commerce required')
    elements={(m['id'],e['id']):e for m in document['modules'] for e in m['elements']}
    record=elements['domain','products']; field=elements['domain','products.sku']
    if (record['kind']!='record' or field['kind']!='field' or field['name']!='sku'
            or field['scalarType']!='string' or field['nullability']!='required'
            or field['cardinality']!='one'
            or {'module':'domain','element':'products.sku'} not in record['members']):
        raise ValueError('Original required products SKU field required')
    types=[e['type_id'] for e in opened.bindings['types'] if e['identity']==['object','domain','products']]
    homes=[e['property_id'] for e in opened.bindings.get('properties',[]) if e['identity']==[document['id'],'domain','products.sku']]
    if len(types)!=1 or len(homes)!=1: raise ValueError('Exact accepted record/Field home required')
    rows=[]
    for row in opened.independent_expected['object_current']:
        if row['type_id']!=types[0]: continue
        if row['source_system']!='private-original-commerce-fixture': raise ValueError('Original source identity required')
        props=json.loads(row['props_json']); value=props[homes[0]]
        if type(value) is not str: raise ValueError('Exact original STRING carrier required')
        key=_text([row['source_system'],row['type_id'],row['id']])
        if len(key.encode())>256 or len(value.encode())>65536: raise ValueError('Finite stage capacity exceeded')
        rows.append((key,value))
    if not rows or len(rows)>1000 or len({k for k,_ in rows})!=len(rows):
        raise ValueError('Complete nonempty bounded products bag required')
    return tuple(rows)


def commerce_current_request(opened, *, case: str):
    """Original full model plus explicitly authored, non-authoritative local profile."""
    if type(case) is not str or case not in ('count','sku'): raise ValueError('Closed commerce query required')
    commerce_current_rows(opened)
    document=json.loads(opened.model); revision=_sha(opened.model)
    pin=dict(documentId=document['id'],revision=revision,umfVersion='0.8.0',sha256=revision)
    identity=dict(documentId=document['id'],moduleId='domain',elementId='products')
    sources=[]
    def add(role,value):
        raw=_text(value); ident=dict(documentId=PROFILE+'/'+role,revision='1')
        result=dict(role=role,identity=ident,mediaType='application/json',utf8=raw,sha256=_sha(raw.encode()))
        sources.append(result);return result
    # These documents describe the local query scope only. No source ACL is
    # interpreted as application Subject/action/disclosure authorization.
    add('policy',dict(profile=PROFILE,authority=False,rules=[]))
    add('ontology',dict(version='0.2.0',documentId=PROFILE+'/ontology',revision='1',actions=['read'],actionPurposes=[dict(action='read',purposes=['read'])]))
    add('catalog',dict(profile=PROFILE,authority=False,transforms=[]))
    add('owner',dict(profile=PROFILE,authority=False,storage='original-publication-values'))
    native=add('native-binding',dict(profile=PROFILE,publication=opened.manifest['publication_id'],manifestSha256=_sha(opened.manifest_bytes),authority=False))
    profile=dict(version='weft.current-query.designation-profile/0.1-candidate',id=PROFILE+'/query-designation',revision='1',modelPins=[pin],
        sources=[{k:s[k] for k in ('role','identity','sha256')} for s in sources if s['role'] in ('policy','ontology','catalog','owner')],
        binding=dict(backendId=PROFILE,backendVersion='0.1',targetProfile='local-spark-publication',source=native['identity'],sha256=native['sha256']),
        targets=[identity],primaryAction='read',bindings=[])
    add('query-designation',profile)
    return dict(interfaceVersion='weft-application-lineage/0.2-candidate',modules=[dict(documentJson=opened.model.decode(),pin=pin,selectedModuleIds=['domain'])],
        sql='SELECT COUNT(*) AS n FROM products p' if case=='count' else 'SELECT p.sku FROM products p',parameters={},opaqueSources=sources)


_MINT=object()

@dataclass(frozen=True,init=False)
class PublicationCurrentStage:
    """Factory-owned exact staged bag, never an imported authority receipt."""
    held_publication: object
    spark: object
    original_child: object
    original_binding: str
    _identity: tuple

    def __init__(self, held, spark, child, binding, rows, qualified, observation, *, _mint=None):
        if _mint is not _MINT: raise TypeError('Stage factory ownership required')
        for name,value in (('held_publication',held),('spark',spark),('original_child',child),('original_binding',binding),('_identity',(held,spark,child,binding,rows,qualified,observation))):
            object.__setattr__(self,name,value)

    def require_current(self, held_publication, child, binding, context):
        held,spark,original,raw,rows,qualified,observation=self._identity
        if (held_publication is not held or child is not original or binding!=raw
                or self.held_publication is not held or self.spark is not spark
                or self.original_child is not original or self.original_binding!=raw):
            raise PermissionError('Original publication stage identity required')
        held.renew_current(context)
        if commerce_current_rows(held.opened)!=rows or _observe(spark,qualified)!=observation:
            raise PermissionError('Original publication/stage correspondence changed')
        captured=capture_string_frame(spark.read.format('delta').option('versionAsOf',observation[1]).table(qualified).select('private_key','sku'),config=PathCaptureConfig(1000,65536,1048576))
        if Counter(map(tuple,captured['rows']))!=Counter(rows): raise PermissionError('Complete staged source bag differs')


def _observe(spark,qualified):
    detail=spark.sql('DESCRIBE DETAIL '+qualified).collect()
    history=spark.sql('DESCRIBE HISTORY '+qualified+' LIMIT 1').collect()
    if len(detail)!=1 or len(history)!=1: raise ValueError('Single actual stage identity required')
    return (detail[0]['id'],str(history[0]['version']),detail[0]['location'],spark.read.format('delta').table(qualified).schema.json())


_SCHEMA_MINT = object()


@dataclass(frozen=True, init=False)
class PublicationStageSchema:
    held_publication: object
    spark: object
    schema: str

    def __init__(self, held, spark, schema, *, _mint=None):
        if _mint is not _SCHEMA_MINT: raise TypeError('Actual fresh schema factory required')
        for key,value in (('held_publication',held),('spark',spark),('schema',schema)):
            object.__setattr__(self,key,value)

    def require_current(self, held, spark, schema):
        if held is not self.held_publication or spark is not self.spark or schema != self.schema:
            raise PermissionError('Original once-owned publication stage schema required')
        held.renew_current(held.context)


def create_publication_stage_schema(held, spark, *, schema):
    from .publication_current_authority import HeldPublicationCurrent
    if type(held) is not HeldPublicationCurrent: raise TypeError('Actual held publication required')
    held.renew_current(held.context)
    if type(schema)is not str or re.fullmatch(r'[a-z][a-z0-9_]{0,62}',schema)is None:
        raise ValueError('Explicit simple fresh stage schema required')
    # A pre-existing schema refuses; neither IF NOT EXISTS nor namespace reuse.
    spark.sql('CREATE DATABASE `spark_catalog`.`'+schema+'`')
    held.renew_current(held.context)
    return PublicationStageSchema(held,spark,schema,_mint=_SCHEMA_MINT)


def stage_commerce_current(held_publication,spark,original_child,*,schema:str,table:str='commerce_products',schema_owner=None):
    """Materialize only within the actual full publication/source ACK interval."""
    from .publication_current_authority import HeldPublicationCurrent
    if type(held_publication) is not HeldPublicationCurrent: raise TypeError('Actual held publication required')
    held_publication.renew_current(held_publication.context)
    for value in (schema,table):
        if type(value) is not str or re.fullmatch(r'[a-z][a-z0-9_]{0,62}',value) is None: raise ValueError('Explicit simple stage identifier required')
    import weft
    if type(original_child) is not weft.CurrentQueryPreparation: raise TypeError('Actual native CQP required')
    request=json.loads(original_child.source_preparation().source_request_json())
    case='count' if request['sql']=='SELECT COUNT(*) AS n FROM products p' else 'sku'
    if request!=commerce_current_request(held_publication.opened,case=case): raise PermissionError('Original publication-derived CQP required')
    rows=commerce_current_rows(held_publication.opened)
    from pyspark.sql.types import StructType,StructField,BooleanType,StringType
    qualified='`spark_catalog`.`'+schema+'`.`'+table+'`'
    if schema_owner is None:
        schema_owner=create_publication_stage_schema(held_publication,spark,schema=schema)
    if type(schema_owner)is not PublicationStageSchema:raise TypeError('Actual once-owned stage schema required')
    schema_owner.require_current(held_publication,spark,schema)
    # Physical name uses a simple column alias; logical identity stays original.
    native=StructType([StructField('eligible',BooleanType(),False),StructField('private_key',StringType(),False),StructField('sku',StringType(),False)])
    spark.createDataFrame([(True,*r) for r in rows],native).write.format('delta').mode('errorifexists').saveAsTable(qualified)
    observation=_observe(spark,qualified)
    packet=json.loads(original_child.native_request_json())
    relation=dict(catalog='spark_catalog',schema=schema,table=table,uuid=observation[0],version=observation[1],schemaSha256=_sha(observation[3].encode()),eligibleColumn='eligible',rowKeyColumn='private_key')
    values=[]
    for projection in packet['projections']:
        field=projection['field']
        if field['element']!='products.sku': raise ValueError('Closed original required STRING projection required')
        values.append(dict(path=projection['path'],scan=projection['scan'],field=field,domain=field,column='sku',comparisonScope=_sha(held_publication.opened.manifest_bytes),mode=None,originalAction=None,exposure='original'))
    binding=_text(dict(version='weft.spark.current-values/0.1-candidate',sourceRequestSha256=packet['sourceRequestSha256'],designationSha256=packet['designationSha256'],sourceBindingSha256=packet['binding']['sha256'],scans=[dict(scan=s['scan'],target=s['record'],relation=relation) for s in packet['scans']],values=values))
    stage=PublicationCurrentStage(held_publication,spark,original_child,binding,rows,qualified,observation,_mint=_MINT)
    stage.require_current(held_publication,original_child,binding,held_publication.context)
    return stage
