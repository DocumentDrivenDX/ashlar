"""Separately authored required String kernel; no canonical publication authority."""
import hashlib,uuid
from local_delta_custody import encoded
from run_commerce_arithmetic_weft import BACKEND


def fixture_model():
    elements=[{'id':'Item','name':'Item','kind':'record','extensions':{},'members':[{'module':'control','element':n}for n in ('id','value')]}]
    elements += [{'id':n,'name':n,'kind':'field','scalarType':'string','cardinality':'one','nullability':'required','facets':{},'extensions':{}}for n in ('id','value')]
    return {'umf':'0.8.0','id':'urn:ashlar:authored:distinct-kernel:1','vocabularies':{},'extensions':{},'modules':[{'id':'control','namespace':'urn:ashlar:authored:distinct-kernel','elements':elements}]}


def rows():
    return [{'id':str(i),'value':v}for i,v in enumerate(('A','A','A ','é','e\u0301',''))]


def controls():
    # This oracle follows exact original scalar strings, never native returned rows.
    values=sorted({row['value']for row in rows()},key=lambda v:v.encode('utf-8'))
    return [
      {'name':'joined-duplicates','sql':'SELECT DISTINCT a.value FROM Item a JOIN Item b ON a.value=b.value','expected':[[v]for v in values]},
      {'name':'repeated-positions','sql':'SELECT DISTINCT a.value,a.value FROM Item a JOIN Item b ON a.value=b.value','expected':[[v,v]for v in values]},
      {'name':'empty-survivors','sql':"SELECT DISTINCT value FROM Item WHERE value='missing-original-value'",'expected':[]},
      {'name':'projected-order-limit','sql':'SELECT DISTINCT value FROM Item ORDER BY value LIMIT 2','expected':[[v]for v in values[:2]],'ordered':True},
    ]


def request(sql,vector,manifest_uuid):
    if len(vector)!=4 or len({t['uuid']for t in vector})!=4 or str(uuid.UUID(manifest_uuid))!=manifest_uuid:raise ValueError('Actual independent native vector required')
    for t in vector:
        if set(t)!={'name','uuid','version'}or len(t['name'])!=3 or any(type(v)is not str or not v for v in t['name'])or str(uuid.UUID(t['uuid']))!=t['uuid']or type(t['version'])is not int or t['version']<0:raise ValueError('Closed actual UUID/version pin required')
    model=fixture_model();source=encoded(model);sha=hashlib.sha256(source.encode()).hexdigest();pin={'documentId':model['id'],'revision':sha,'sha256':sha,'umfVersion':'0.8.0'}
    identity=lambda n:{'documentId':model['id'],'module':'control','element':n,'revision':sha}
    from run_commerce_publication_weft import LAYOUT_SHA
    binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3','layoutSha256':LAYOUT_SHA,'modelPins':[pin],
       'publication':{'id':'unpublished-independent-distinct-kernel','manifestUuid':manifest_uuid,'tables':vector},
       'records':[{'kind':'object','logical':identity('Item'),'sourceSystem':'private-distinct-kernel','typeId':'1','table':0,'schemaRevision':sha,
       'properties':[{'logical':identity(n),'home':{'kind':'props','propertyId':str(i)}}for i,n in enumerate(('id','value'),1)]}]}
    raw=encoded(binding)
    return {'interfaceVersion':'weft-compile/0.3.0','dialect':'weft-sql/0.3.0','sql':sql,'modules':[{'documentJson':source,'pin':pin,'selectedModuleIds':['control']}],'target':{**BACKEND,'bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()},'options':{'allowCandidate':True}}
