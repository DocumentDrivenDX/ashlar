"""Separately authored unpublished null kernel inputs; no canonical authority.

The original pack publications remain untouched. Each control requires freshly
created actual Delta UUID/version pins and compiler SQL executed unchanged.
These inputs do not assert Truss admission, a manifest/ACK or publication holds.
"""
import copy,hashlib,json
from local_delta_custody import encoded
from run_commerce_arithmetic_weft import BACKEND

ENCODING='ashlar-weft-json-native-null/0.1-candidate'

def fixture_model():
    fields=[('id','string','required',{}),('text','string','absent-allowed',{}),('amount','decimal','absent-allowed',{'precision':18,'scale':2}),('flag','boolean','absent-allowed',{}),('zero','integer','required',{'integerWidth':{'bits':64,'signed':True}})]
    elements=[{'id':'Item','name':'Item','kind':'record','extensions':{},'members':[{'module':'control','element':f[0]}for f in fields]}]
    elements += [{'id':name,'name':name,'kind':'field','scalarType':family,'cardinality':'one','nullability':availability,'facets':facets,'extensions':{}}for name,family,availability,facets in fields]
    return {'umf':'0.8.0','id':'urn:ashlar:authored:native-null-kernel:1','vocabularies':{},'extensions':{},'modules':[{'id':'control','namespace':'urn:ashlar:authored:native-null-kernel','elements':elements}]}

def controls():
    base={'id':'original-control','text':'','amount':0,'flag':False,'zero':0}
    result=[]
    for name,mutation,sql,disposition in [
      ('zero-false-empty',{},'SELECT id,text,amount,flag,zero FROM Item WHERE amount IS NOT NULL','pass'),
      ('explicit-null',{'text':None,'amount':None,'flag':None},'SELECT id,text,amount,flag FROM Item WHERE amount IS NULL','pass'),
      ('missing-optional',{'amount':'REMOVE'},'SELECT id FROM Item WHERE amount IS NULL','capability'),
      ('required-null',{'id':None},'SELECT id FROM Item WHERE amount IS NULL','source-invalid'),
      ('malformed-nonnull',{'amount':'not-a-decimal'},'SELECT id FROM Item WHERE amount IS NULL','source-invalid'),
      ('null-null-join',{'text':None},'SELECT a.id AS left_id,b.id AS right_id FROM Item a JOIN Item b ON a.text=b.text','pass-empty'),
    ]:
        props=copy.deepcopy(base)
        for k,v in mutation.items():
            if v=='REMOVE':del props[k]
            else:props[k]=v
        result.append({'name':name,'originalPropsJson':encoded(props),'sql':sql,'expectedDisposition':disposition})
    return result

def request(model,sql,table_vector,manifest_uuid):
    """Bind only actual caller-supplied private fixture UUID/version identities."""
    import uuid
    if len(table_vector)!=4 or len({t['uuid']for t in table_vector})!=4 or str(uuid.UUID(manifest_uuid))!=manifest_uuid:
        raise ValueError('Actual distinct fixture vector/manifest-table UUID required')
    for table in table_vector:
        if set(table)!={'name','uuid','version'} or len(table['name'])!=3 or any(type(x)is not str or not x for x in table['name']) or str(uuid.UUID(table['uuid']))!=table['uuid'] or type(table['version'])is not int or table['version']<0:
            raise ValueError('Actual closed native fixture pin required')
    source=encoded(model);sha=hashlib.sha256(source.encode()).hexdigest();pin={'documentId':model['id'],'revision':sha,'sha256':sha,'umfVersion':'0.8.0'}
    identity=lambda element:{'documentId':model['id'],'module':'control','element':element,'revision':sha}
    fields=next(m['elements']for m in model['modules']if m['id']=='control')
    props=[]
    for position,field in enumerate((e for e in fields if e['kind']=='field'),1):
        home={'kind':'props','propertyId':str(position)}
        if field['nullability']=='absent-allowed':home['encoding']=ENCODING
        props.append({'logical':identity(field['id']),'home':home})
    from run_commerce_publication_weft import LAYOUT_SHA
    binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3','layoutSha256':LAYOUT_SHA,'modelPins':[pin], 'publication':{'id':'unpublished-independent-null-kernel','manifestUuid':manifest_uuid,'tables':table_vector},'records':[{'kind':'object','logical':identity('Item'),'sourceSystem':'private-null-kernel','typeId':'1','table':0,'schemaRevision':sha,'properties':props}]}
    raw=encoded(binding)
    return {'interfaceVersion':'weft-compile/0.3.0','dialect':'weft-sql/0.3.0','sql':sql,'modules':[{'documentJson':source,'pin':pin,'selectedModuleIds':['control']}],'target':{**BACKEND,'bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()},'options':{'allowCandidate':True}}
