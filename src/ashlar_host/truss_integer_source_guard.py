# SPDX-License-Identifier: MIT OR Apache-2.0
"""Nominal complete Truss mathematical-integer source projections, never authority."""
import hashlib,json,re
from collections import Counter
from dataclasses import dataclass
from ashlar.schema import decode_retained_json
from ashlar.typed_source_policy import NumericToken
from .delta_custody import encoded
from .truss_graph_admission import TrussGraphAdmission
from .truss_scalar_projection import mathematical_integer_token, decode_truss_lexical_properties

SOURCE_KEYS={'publicSourceOnly','field','record','propertyId','logicalType','sql','failureCode'}
REPRESENTATION_KEYS={'representabilityOnly','field','record','sql','failureCode'}
SCHEMA=[['source_system','STRING'],['type_id','STRING'],['id','STRING'],['schema_revision','STRING'],['props_json','STRING'],['extracted_token','STRING']]

@dataclass(frozen=True)
class TrussIntegerSourceGuard:
    entries_json:str
    def entry(self,check):
        matches=[e for e in json.loads(self.entries_json)if check in (e['source'],e['representability'])]
        if len(matches)!=1:raise ValueError('Original nominal integer guard required')
        return matches[0]
    def require_source(self,check,captured):
        entry=self.entry(check)
        if check!=entry['source']or captured.get('schema')!=SCHEMA:raise ValueError('Complete six STRING source columns required')
        rows=captured.get('rows')
        if type(rows)is not list or any(type(r)is not list or len(r)!=6 or any(type(v)is not str for v in r)for r in rows):raise ValueError('Complete original integer source rows required')
        if Counter(encoded(r[:-1]+[mathematical_integer_token(r[-1])])for r in rows)!=Counter(encoded(r[:-1]+[mathematical_integer_token(r[-1])])for r in entry['rows']):raise ValueError('Independent complete original integer source population differs')
    def source_budget(self,check):return max(1,len(self.entry(check)['rows']))


def admit_truss_integer_source_guard(admission,artifact,request,expected,*,revision,document_id,physical_revisions):
    if type(admission)is not TrussGraphAdmission:raise ValueError('Actual Truss source admission required')
    model,raw=admission.revisions[revision]['models'][document_id]
    if model.get('umf')!='0.8.0' or encoded(model)!=encoded(json.loads(raw)):raise ValueError('Original UMF0.8 mathematical source required')
    digest=hashlib.sha256(raw).hexdigest();binding=decode_retained_json(request['target']['bindingJson'].encode())
    checks=next(o['parameters']['checks']for o in artifact['obligations']if o['id']=='ashlar.candidate.scalarIntegrity')
    if type(checks)is not list or len(checks)>128:raise ValueError('Bounded original source guards required')
    source=[c for c in checks if c.get('publicSourceOnly')is True];represent=[c for c in checks if c.get('representabilityOnly')is True]
    entries=[];seen=set()
    fields={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    rows=expected['object_current']
    if type(rows)is not list or len(rows)>10000:raise ValueError('Bounded complete source population required')
    for check in source:
        if set(check)!=SOURCE_KEYS or check['failureCode']!='WFT-NUMERIC-DOMAIN':raise ValueError('Original mathematical source guard shape required')
        field=check['field'];record=check['record'];marker=encoded([record,field])
        if marker in seen:raise ValueError('Duplicate mathematical source guard')
        seen.add(marker)
        for identity in (record,field):
            if type(identity)is not dict or set(identity)!={'documentId','module','element','revision'}or identity['documentId']!=document_id or identity['revision']!=digest:raise ValueError('Original source guard identity required')
        definition=fields[record['module'],record['element']];scalar=fields[field['module'],field['element']]
        logical={'family':'integer','facets':{},'nullable':False}
        if (definition.get('kind')!='record'or {'module':field['module'],'element':field['element']}not in definition['members']
                or scalar.get('kind')!='field'or scalar.get('scalarType')!='integer'or scalar.get('facets',{})!={}
                or scalar.get('nullability')!='required'or scalar.get('cardinality')!='one'or check['logicalType']!=logical):raise ValueError('Original required mathematical integer field required')
        records=[r for r in binding['records']if r['logical']==record]
        if len(records)!=1:raise ValueError('One actual source record binding required')
        bound=records[0];actual=admission.revisions[revision]['types'][bound['typeId']]
        if actual['owner']!=(document_id,record['module'],record['element']):raise ValueError('Actual accepted source record required')
        properties=[p for p in bound['properties']if p['logical']==field]
        if len(properties)!=1 or properties[0]['home']!={'kind':'props','propertyId':check['propertyId']}or check['propertyId']not in actual['fields']or actual['domains'][check['propertyId']]!=('integer','required')or actual['facets'][check['propertyId']]!={}:raise ValueError('Exact accepted original property home required')
        paired=[c for c in represent if c.get('record')==record and c.get('field')==field]
        if len(paired)!=1 or set(paired[0])!=REPRESENTATION_KEYS or paired[0]['failureCode']!='WFT-CAPABILITY':raise ValueError('One paired original representability guard required')
        population=[];identities=set();total_bytes=0
        for row in rows:
            if (row['source_system'],row['type_id'])!=(bound['sourceSystem'],bound['typeId']):continue
            if row['schema_revision']not in physical_revisions:raise ValueError('Source outside admitted physical revisions')
            prior=admission.revisions[row['schema_revision']]['types'][bound['typeId']]
            if prior['owner']!=actual['owner']or check['propertyId']not in prior['fields']or prior['domains'][check['propertyId']]!=('integer','required')or prior['facets'][check['propertyId']]!={}:raise ValueError('Original mathematical source revision differs')
            props=decode_truss_lexical_properties(row['props_json'])
            token=props.get(check['propertyId'])
            if type(token)is not NumericToken or not re.fullmatch(r'-?(?:0|[1-9][0-9]*)',token.text,flags=re.ASCII):raise ValueError('Original exact JSON integer token required')
            values=[row['source_system'],str(row['type_id']),str(row['id']),row['schema_revision'],row['props_json'],token.text]
            key=tuple(values[:3])
            if key in identities:raise ValueError('Distinct complete source identities required')
            identities.add(key)
            if any(type(v)is not str or len(v.encode())>65536 for v in values):raise ValueError('Bounded original source cells required')
            total_bytes+=sum(len(v.encode())for v in values)
            if total_bytes>1048576:raise ValueError('Complete source capture exceeds existing byte budget')
            population.append(values)
        entries.append({'source':check,'representability':paired[0],'rows':population})
    projected={encoded([artifact['logicalPlan']['source']['record'],column['sourceIdentities'][0]])for column in artifact['columns']
        if column.get('representation',{}).get('logicalType')=={'family':'integer','facets':{},'nullable':False}}
    if seen!=projected:raise ValueError('Complete mathematical output field guard coverage required')
    if len(represent)!=len(entries)or not entries:raise ValueError('Complete paired mathematical guard inventory required')
    return TrussIntegerSourceGuard(encoded(entries))
