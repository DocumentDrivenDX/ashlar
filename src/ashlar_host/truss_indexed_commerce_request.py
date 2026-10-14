"""Original commerce SQL with truthful homogeneous physical Truss record homes."""
import hashlib,json,re
from types import MappingProxyType
from ashlar.typed_source_policy import NumericToken
from .commerce_original_scenarios import commerce_original_cases
from .delta_custody import encoded
from .truss_graph_admission import TrussGraphAdmission
from .truss_implicit_arithmetic_request import truss_implicit_arithmetic_request

# Exact consumed fields of the four authored cases, not a general SQL interpreter.
CASE_FIELDS={
 'partial-return':{'order_lines':('id','quantity'),'fulfillments':('line_id','quantity'),'returns':('line_id','quantity')},
 'fulfillment':{'order_lines':('id','quantity'),'fulfillments':('id','line_id','quantity')},
 'settlement':{'payments':('id','invoice_id','amount','currency'),'invoices':('id','amount','currency')},
 'refund':{'refunds':('id','return_id','amount'),'returns':('id','line_id','quantity'),'order_lines':('id','product_id'),'products':('id','unit_price')},
}
CASE_FIELDS=MappingProxyType({case:MappingProxyType(fields)for case,fields in CASE_FIELDS.items()})


def lexical_document(raw):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate original document member')
            result[key]=value
        return result
    def nonfinite(value):raise ValueError('Nonfinite original document token')
    return json.loads(raw,object_pairs_hook=pairs,parse_int=NumericToken,parse_float=NumericToken,parse_constant=nonfinite)


def truss_indexed_commerce_request(admission,manifest,table_uuids,manifest_uuid,*,
        case_id,pack_bytes,expected,revision,document_id,empty_record_revisions=(),aliases=None):
    """Map each record to its actual ONE physical revision without union claims.

    Empty records require explicit (qualified owner tuple, accepted revision)
    selections. This is effect-free request correspondence, not native authority.
    """
    cases=dict(commerce_original_cases(pack_bytes))
    if type(case_id)is not str or case_id not in cases or type(admission)is not TrussGraphAdmission or revision not in admission.revisions:raise ValueError('Original selected Truss commerce case required')
    selected=admission.revisions[revision];model,raw=selected['models'][document_id]
    if model.get('id')!='urn:umf:domain:commerce'or model.get('umf')!='0.8.0'or encoded(model)!=encoded(json.loads(raw)):raise ValueError('Original accepted commerce UMF0.8 bytes required')
    if type(expected)is not dict or type(expected.get('object_current'))is not list or len(expected['object_current'])>10000:raise ValueError('Complete bounded original source population required')
    if type(empty_record_revisions)is not tuple:raise ValueError('Immutable explicit empty-record selections required')
    empty={}
    for selection in empty_record_revisions:
        if type(selection)is not tuple or len(selection)!=2 or type(selection[0])is not tuple or len(selection[0])!=3 or any(type(x)is not str or not x for x in selection[0])or type(selection[1])is not str or selection[0]in empty:raise ValueError('Qualified distinct empty record revisions required')
        empty[selection[0]]=selection[1]
    request=truss_implicit_arithmetic_request(admission,manifest,table_uuids,manifest_uuid,cases[case_id],revision=revision,document_id=document_id,aliases=aliases)
    binding=json.loads(request['target']['bindingJson']);used_empty=set();consumed=set()
    # These exact authored cases join scalar foreign-key properties, never edges.
    # Original UMF relationships and the four-role publication remain retained.
    binding.pop('relationships',None)
    lexical=lexical_document(raw);modules={m['id']:m for m in lexical['modules']}
    for bound in binding['records']:
        actual=selected['types'][bound['typeId']];owner=actual['owner']
        if owner!=(document_id,bound['logical']['module'],bound['logical']['element']):raise ValueError('Actual accepted record home differs')
        population=[r for r in expected['object_current']if (r['source_system'],r['type_id'])==(bound['sourceSystem'],bound['typeId'])]
        identities=[r['id']for r in population]
        if len(set(identities))!=len(identities):raise ValueError('Complete current record identities must be distinct')
        physical={r['schema_revision']for r in population}
        if population:
            if len(physical)!=1 or owner in empty:raise ValueError('One true homogeneous physical revision required')
            physical_revision=next(iter(physical))
        else:
            if owner not in empty:raise ValueError('Empty record requires explicit accepted physical revision')
            physical_revision=empty[owner];used_empty.add(owner)
        if type(physical_revision)is not str or physical_revision not in admission.revisions or re.fullmatch(r'[1-9][0-9]{0,19}',physical_revision,flags=re.ASCII)is None or int(physical_revision)>int(revision):raise ValueError('Original accepted historical physical revision required')
        old=admission.revisions[physical_revision]
        prior=old['types'].get(bound['typeId'])
        if prior is None or prior['owner']!=owner or prior['keys']!=actual['keys']:raise ValueError('Original accepted record/key correspondence required')
        prior_model,prior_raw=old['models'][document_id]
        if prior_model.get('umf')!='0.8.0'or encoded(prior_model)!=encoded(json.loads(prior_raw)):raise ValueError('Original accepted historical document required')
        prior_lexical=lexical_document(prior_raw)
        if {k:v for k,v in prior_lexical.items()if k!='modules'}!={k:v for k,v in lexical.items()if k!='modules'}:raise ValueError('Original document context differs')
        prior_modules={m['id']:m for m in prior_lexical['modules']}
        module=modules[owner[1]];old_module=prior_modules[owner[1]]
        if {k:v for k,v in old_module.items()if k!='elements'}!={k:v for k,v in module.items()if k!='elements'}:raise ValueError('Original module context differs')
        fields={(m['id'],e['id']):e for m in model['modules']for e in m['elements']}
        old_fields={(m['id'],e['id']):e for m in prior_model['modules']for e in m['elements']}
        lexical_fields={(m['id'],e['id']):e for m in lexical['modules']for e in m['elements']}
        prior_lexical_fields={(m['id'],e['id']):e for m in prior_lexical['modules']for e in m['elements']}
        declaration=fields[owner[1],owner[2]];old_declaration=old_fields[owner[1],owner[2]]
        current_record=lexical_fields[owner[1],owner[2]];prior_record=prior_lexical_fields[owner[1],owner[2]]
        if {k:v for k,v in prior_record.items()if k!='members'}!={k:v for k,v in current_record.items()if k!='members'}:raise ValueError('Original record metadata/keys differ')
        if owner[2]in CASE_FIELDS[case_id]:
            consumed.add(owner[2])
            for name in CASE_FIELDS[case_id][owner[2]]:
                identity=(owner[1],owner[2]+'.'+name)
                field=fields.get(identity);old_field=old_fields.get(identity)
                if field is None or prior_lexical_fields.get(identity)!=lexical_fields.get(identity)or field.get('kind')!='field'or field.get('cardinality')!='one'or field.get('nullability')!='required':raise ValueError('Original consumed field meaning differs')
                member={'module':identity[0],'element':identity[1]}
                if member not in declaration['members']or member not in old_declaration['members']:raise ValueError('Original consumed record member differs')
                homes=[p['home']for p in bound['properties']if p['logical']['module']==identity[0]and p['logical']['element']==identity[1]]
                if len(homes)!=1 or homes[0].get('kind')!='props':raise ValueError('One actual consumed property home required')
                pid=homes[0]['propertyId']
                if (pid not in actual['fields']or prior['fields'].get(pid)!=actual['fields'][pid]
                        or prior['domains'].get(pid)!=actual['domains'].get(pid)or prior['facets'].get(pid)!=actual['facets'].get(pid)
                        or actual['domains'].get(pid)!=(field['scalarType'],'required')or actual['facets'].get(pid)!=field.get('facets',{})):raise ValueError('Accepted consumed property pin/domain differs')
        if 'compatibleSchemaRevisions'in bound:raise ValueError('Indexed profile has no compatibility union capability')
        bound['schemaRevision']=physical_revision
    if consumed!=set(CASE_FIELDS[case_id])or used_empty!=set(empty):raise ValueError('Complete exact case/empty selections required')
    text=encoded(binding);request['target']['bindingJson']=text;request['target']['bindingSha256']=hashlib.sha256(text.encode()).hexdigest()
    return request
