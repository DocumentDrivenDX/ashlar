"""Original Truss mathematical source projections; no dataset receipt or authority."""
from dataclasses import dataclass
from collections import Counter
import json,hashlib
from ashlar.typed_source_policy import NumericToken
from .delta_custody import encoded
from .truss_graph_admission import TrussGraphAdmission
from .truss_scalar_projection import decode_truss_lexical_properties
from .truss_indexed_commerce_request import CASE_FIELDS

SOURCE_KEYS={'publicSourceOnly','field','record','propertyId','logicalType','sql','failureCode'}
REPRESENTATION_KEYS={'representabilityOnly','field','record','sql','failureCode'}
SCHEMA=[['source_system','STRING'],['type_id','STRING'],['id','STRING'],['schema_revision','STRING'],['props_json','STRING'],['extracted_token','STRING']]

@dataclass(frozen=True)
class TrussIndexedCommerceGuard:
    entries_json:str

    def entry(self,check):
        matches=[entry for entry in json.loads(self.entries_json)if check==entry['source']or check==entry['representability']]
        if len(matches)!=1:raise ValueError('Original indexed Truss guard required')
        return matches[0]

    def source_budget(self,check):return max(1,len(self.entry(check)['rows']))

    def require_source(self,check,captured):
        from .truss_indexed_commerce_oracle import integer_source_key
        entry=self.entry(check)
        if check!=entry['source']or captured.get('schema')!=SCHEMA:raise ValueError('Complete six STRING source columns required')
        rows=captured.get('rows')
        if type(rows)is not list or any(type(row)is not list or len(row)!=6 or any(type(v)is not str for v in row)for row in rows):raise ValueError('Complete original source projection required')
        def key(row):return tuple(row[:5])+(integer_source_key(row[5]),)
        if Counter(key(row)for row in rows)!=Counter(key(row)for row in entry['rows']):raise ValueError('Complete original Truss source population differs')


def admit_truss_indexed_commerce_guard(admission,request,artifact,expected,*,revision,document_id,case_id):
    """Snapshot complete accepted source homes and all consumed mathematical Fields."""
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions or case_id not in CASE_FIELDS:raise ValueError('Original indexed commerce source required')
    selected=admission.revisions[revision];model,raw=selected['models'][document_id];digest=hashlib.sha256(raw).hexdigest()
    if request['modules'][0]['documentJson'].encode()!=raw or model.get('umf')!='0.8.0':raise ValueError('Original accepted UMF0.8 source required')
    binding=json.loads(request['target']['bindingJson']);fields={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
    obligations=[o for o in artifact['obligations']if o['id']=='ashlar.candidate.scalarIntegrity']
    if len(obligations)!=1:raise ValueError('One original scalar-integrity obligation required')
    checks=obligations[0]['parameters']['checks']
    if type(checks)is not list or len(checks)>128:raise ValueError('Bounded original scalar guards required')
    sources=[check for check in checks if check.get('publicSourceOnly')is True];representations=[check for check in checks if check.get('representabilityOnly')is True]
    wanted=set()
    for record,names in CASE_FIELDS[case_id].items():
        for name in names:
            declaration=fields['domain',record+'.'+name]
            if declaration['scalarType']=='integer'and declaration.get('facets',{})=={}:wanted.add(('domain',record,'domain',declaration['id']))
    entries=[];seen=set();rows=expected['object_current']
    if type(rows)is not list or len(rows)>10000:raise ValueError('Bounded complete current source rows required')
    for check in sources:
        if set(check)!=SOURCE_KEYS or check['failureCode']!='WFT-NUMERIC-DOMAIN':raise ValueError('Original mathematical source check shape required')
        record,field=check['record'],check['field']
        for identity in(record,field):
            if type(identity)is not dict or set(identity)!={'documentId','revision','module','element'}or identity['documentId']!=document_id or identity['revision']!=digest:raise ValueError('Original mathematical Field/Record identity required')
        marker=(record['module'],record['element'],field['module'],field['element'])
        if marker in seen or marker not in wanted:raise ValueError('One source check per consumed mathematical Field required')
        seen.add(marker);declaration=fields[field['module'],field['element']];logical={'family':'integer','facets':{},'nullable':False}
        if (check['logicalType']!=logical or declaration['scalarType']!='integer'or declaration.get('facets',{})!={}or declaration['nullability']!='required'or declaration['cardinality']!='one'
                or {'module':field['module'],'element':field['element']}not in fields[record['module'],record['element']]['members']):raise ValueError('Original required mathematical Field meaning required')
        mappings=[bound for bound in binding['records']if bound['logical']==record]
        if len(mappings)!=1:raise ValueError('One actual source record mapping required')
        bound=mappings[0];physical=bound['schemaRevision'];actual=admission.revisions[physical]['types'][bound['typeId']]
        homes=[p['home']for p in bound['properties']if p['logical']==field]
        if actual['owner']!=(document_id,record['module'],record['element'])or homes!=[{'kind':'props','propertyId':check['propertyId']}]or actual['domains'].get(check['propertyId'])!=('integer','required')or actual['facets'].get(check['propertyId'])!={}:raise ValueError('Original accepted physical property home required')
        paired=[item for item in representations if item.get('record')==record and item.get('field')==field]
        if len(paired)!=1 or set(paired[0])!=REPRESENTATION_KEYS or paired[0]['failureCode']!='WFT-CAPABILITY':raise ValueError('One original paired representability check required')
        population=[];identities=set();total=0
        for row in rows:
            if(row['source_system'],row['type_id'])!=(bound['sourceSystem'],bound['typeId']):continue
            if row['schema_revision']!=physical:raise ValueError('Source row outside true physical revision')
            token=decode_truss_lexical_properties(row['props_json']).get(check['propertyId'])
            if type(token)is not NumericToken:raise ValueError('Original mathematical numeric atom required')
            from .truss_indexed_commerce_oracle import integer_source_key
            integer_source_key(token.text)
            values=[row['source_system'],str(row['type_id']),str(row['id']),row['schema_revision'],row['props_json'],token.text]
            identity=tuple(values[:3])
            if identity in identities:raise ValueError('Distinct complete native source identities required')
            identities.add(identity)
            if any(type(v)is not str or len(v.encode())>65536 for v in values):raise ValueError('Bounded original source cells required')
            total+=sum(len(v.encode())for v in values)
            if total>1048576:raise ValueError('Complete original source projection exceeds byte budget')
            population.append(values)
        entries.append({'source':check,'representability':paired[0],'rows':population})
    if seen!=wanted or len(representations)!=len(entries):raise ValueError('Complete source/representability Field coverage required')
    return TrussIndexedCommerceGuard(encoded(entries))
