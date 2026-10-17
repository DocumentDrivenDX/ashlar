"""Actual medical public admission plus complete original mathematical Field rows.

This port supplies source meaning only inside the existing acquired publication;
it grants no writer, source ACK, UC or clinical authority.
"""
from collections import Counter
import hashlib,json,re
from ashlar.medical_source import SOURCE_SHA,UMF_PIN
from ashlar.pins import PinVector
from .delta_custody import encoded
from .original_medical_publication import MedicalAdmission
from .medical_producer import HeldMedicalDataset
from .medical_read_sources import MedicalReadSourceFactory


def medical_integer_projection(model,graph,baseline,bindings,feed,check):
    """Independent original lexical source projection; no event/compiler oracle."""
    document=json.loads(model);original=json.loads(graph);public=json.loads(baseline)
    fields={(m['id'],e['id']):e for m in document['modules']for e in m['elements']}
    record,field=check['record'],check['field']
    for identity in (record,field):
        if identity.get('documentId')!=document['id']or identity.get('revision')!=SOURCE_SHA:raise ValueError('Original medical Field identity required')
    definition=fields[record['module'],record['element']];scalar=fields[field['module'],field['element']]
    properties={tuple(p['identity']):p['property_id']for p in bindings['properties']}
    if (scalar.get('scalarType')!='integer'or scalar.get('facets',{})!={}or scalar.get('nullability')!='required'
            or scalar.get('cardinality')!='one'or {'module':field['module'],'element':field['element']}not in definition['members']
            or check['propertyId']!=properties[document['id'],field['module'],field['element']]
            or check['logicalType']!={'family':'integer','facets':{},'nullable':False}):raise ValueError('Original required mathematical medical Field required')
    assigned={v['originalKey']:v for v in bindings['entities']if v['kind']=='object'}
    boolean={(v['request']['sourceContext']['objectKey'],v['request']['field']['element']):v for v in public['booleanReceipts']};rows=[]
    for obj in original['objects']:
        if obj['type']!={'document':document['id'],'module':record['module'],'element':record['element']}:continue
        parts=[]
        for ref in definition['members']:
            authored=fields[ref['module'],ref['element']];token=obj['values'][ref['element']]
            if token is None:literal='null'
            elif authored['scalarType']=='string':literal=encoded(token)
            elif authored['scalarType']=='boolean':
                proof=boolean[obj['key'],ref['element']]
                if proof['request']['token']!=token or proof['source']!=document or proof['validation']['valid']is not True or proof['validation']['complete']is not True or type(proof['value']['boolean'])is not bool:raise ValueError('Original public Boolean witness required')
                literal=encoded(proof['value']['boolean'])
            elif authored['scalarType']=='integer'and type(token)is str and re.fullmatch(r'-?(0|[1-9][0-9]*)',token):literal=token
            else:raise ValueError('Original lexical medical property required')
            parts.append(encoded(properties[document['id'],ref['module'],ref['element']])+':'+literal)
        entity=assigned[obj['key']];token=obj['values'][field['element']]
        rows.append({'source_system':feed,'type_id':entity['type_id'],'id':entity['id'],'schema_revision':SOURCE_SHA,'props_json':'{'+','.join(parts)+'}','extracted_token':token})
    return rows


class MedicalQuerySource:
    def __setattr__(self,name,value):
        if name=='_selection'and hasattr(self,'_selection'):raise AttributeError('Original medical source selection is immutable')
        object.__setattr__(self,name,value)
    def __delattr__(self,name):
        if name=='_selection':raise AttributeError('Original medical source selection is immutable')
        object.__delattr__(self,name)
    def _require_selection(self):
        from .driver import NativeDriver
        selected=self._selection
        if (type(self)is not MedicalQuerySource or type(selected)is not tuple or len(selected)!=11
                or type(self.driver)is not NativeDriver or type(self.admission)is not MedicalAdmission
                or type(self.producer)is not HeldMedicalDataset or type(self.factory)is not MedicalReadSourceFactory
                or type(self.vector)is not PinVector or type(self.expected)is not str or type(self.maximum)is not int
                or type(self.originals)is not tuple or any(type(v)is not bytes for v in self.originals)or type(self.bindings)is not bytes
                or any(a is not b for a,b in zip((self.driver,self.context,self.admission,self.producer,self.factory,self.vector),selected[:6]))
                or (self.originals,self.bindings,self.expected,self.maximum)!=(selected[6],selected[7],selected[8],selected[9])
                or selected[10]is not self):raise PermissionError('Exact construction-bound medical source selection required')
    def __init__(self,driver,context,original_binding,maximum):
        from .driver import NativeDriver
        if (type(driver)is not NativeDriver or type(driver.source_admission)is not MedicalAdmission
                or type(driver.source_admission.producer)is not HeldMedicalDataset
                or type(driver.query_source_factory)is not MedicalReadSourceFactory
                or type(maximum)is not int or not 0<maximum<=1048576):raise PermissionError('Actual bounded original medical source owner required')
        if type(original_binding)is not str or not original_binding or type(getattr(driver,'_held_original_vector',None))is not PinVector or not driver.pin_held:raise PermissionError('Actual original held vector/binding required')
        driver.require(context)
        self.driver,self.context,self.expected,self.maximum=driver,context,original_binding,maximum
        self.vector=driver._held_original_vector
        self.admission=driver.source_admission;self.producer=self.admission.producer;self.factory=driver.query_source_factory;self.originals=tuple(self.admission.originals);self.bindings=self.admission.bindings_bytes
        self._selection=(self.driver,self.context,self.admission,self.producer,self.factory,self.vector,self.originals,self.bindings,self.expected,self.maximum,self)
    def renew(self):
        self._require_selection()
        self.driver.require(self.context)
        if type(self.driver._held_original_vector)is not PinVector or self.driver._held_original_vector!=self.vector or not self.driver.pin_held:raise PermissionError('Original medical source hold changed')
        self.driver.renew_original_graph_read_source(self.vector,context=self.context)
        if (self.driver.source_admission is not self.admission or self.admission.producer is not self.producer or self.driver.query_source_factory is not self.factory or tuple(self.admission.originals)!=self.originals
                or self.admission.bindings_bytes!=self.bindings or type(self.driver._held_original_vector)is not PinVector):raise PermissionError('Original medical source owner changed')
        self.driver.query_source_factory.require_admission(self.admission)
    def __call__(self,request,artifact,projections):
        self.renew();model,graph,baseline=self.originals;binding=json.loads(request['target']['bindingJson'])
        if request['modules'][0]['documentJson'].encode()!=model or encoded(binding)!=self.expected:raise PermissionError('Original held medical source binding differs')
        checks=[c for o in artifact['obligations']for c in o['parameters'].get('checks',[])];sources=[c for c in checks if c.get('publicSourceOnly')is True]
        projected={encoded([artifact['logicalPlan']['source']['record'],o['expression']['identity']])for o,c in zip(artifact['logicalPlan']['outputs'],artifact['columns'])if o['expression']['op']=='field'and c['representation'].get('logicalType')=={'family':'integer','facets':{},'nullable':False}}
        if {encoded([c['record'],c['field']])for c in sources}!=projected or len({encoded(c)for c in sources})!=len(sources):raise ValueError('Complete projected mathematical Field coverage required')
        if not sources or len(projections)!=len(sources):raise ValueError('Complete medical source guard inventory required')
        for check in sources:
            paired=[c for c in checks if c.get('representabilityOnly')is True and c.get('field')==check['field']and c.get('record')==check['record']]
            selected=[p for p in projections if p['check']==check]
            record=[r for r in binding['records']if r['logical']==check['record']]
            if len(paired)!=1 or len(selected)!=1 or len(record)!=1:raise ValueError('Exact paired medical source guard required')
            homes=[p for p in record[0]['properties']if p['logical']==check['field']]
            if len(homes)!=1 or homes[0]['home']!={'kind':'props','propertyId':check['propertyId']}:raise ValueError('Original medical property home differs')
            if (set(check)!={'publicSourceOnly','field','record','propertyId','logicalType','sql','failureCode'} or check['failureCode']!='WFT-NUMERIC-DOMAIN'
                    or set(paired[0])!={'representabilityOnly','field','record','sql','failureCode'}or paired[0]['failureCode']!='WFT-CAPABILITY'):
                raise ValueError('Original closed mathematical guard pair required')
            rows=selected[0]['rows']
            columns={'source_system','type_id','id','schema_revision','props_json','extracted_token'}
            if type(rows)is not list or len(rows)>10000 or any(type(r)is not dict or set(r)!=columns or any(type(v)is not str or len(v.encode())>65536 for v in r.values())for r in rows)or sum(len(v.encode())for r in rows for v in r.values())>1048576:raise ValueError('Complete bounded six STRING source projection required')
            expected=medical_integer_projection(model,graph,baseline,json.loads(self.bindings),self.admission.batch.feed,check)
            if Counter(map(encoded,selected[0]['rows']))!=Counter(map(encoded,expected)):raise ValueError('Complete original medical numeric population differs')
        original={'sourceText':model.decode(),'modelPins':artifact['modelPins'],'bindingSha256':artifact['bindingSha256'],'checks':projections}
        raw=encoded(original);receipt=encoded({'originalRequestText':raw,'umfRevision':UMF_PIN,'admitted':True,'originalDatasetReceiptText':self.admission.revalidation.decode(),'scope':'Actual original medical admission and complete Field population; no authority grant'})
        if max(len(raw.encode()),len(receipt.encode()))>self.maximum:raise ValueError('Complete medical public source receipt bound exceeded')
        self.renew();return {'originalRequestText':raw,'originalReceiptText':receipt,'receiptSha256':hashlib.sha256(receipt.encode()).hexdigest()}
