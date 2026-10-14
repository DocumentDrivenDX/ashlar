"""Pure four-case oracle over current published rows and accepted original homes.

Metadata correspondence and finite arithmetic are not native/source guard discharge.
"""
from __future__ import annotations
from dataclasses import dataclass
from fractions import Fraction
import json,re
from .commerce_original_scenarios import CASES

class IndexedOracleDomainError(ValueError):
    """Selected physical atom or accepted logical domain is inconsistent."""
class IndexedOracleCapacityError(ValueError):
    """Finite oracle capacity refused; this does not narrow mathematical Integer."""

def name(value: object) -> str:
    if type(value)is not str or not value or len(value.encode('utf8'))>4096 or '\0'in value:raise IndexedOracleDomainError('original-home')
    return value

@dataclass(frozen=True)
class AcceptedCommerceHome:
    source_system:str
    type_id:str
    schema_revision:str
    record:str
    field_name:str
    property_id:str
    scalar_type:str
    facets:tuple=()

    def __post_init__(self):
        for value in(self.source_system,self.type_id,self.schema_revision,self.record,self.field_name,self.property_id):name(value)
        if self.scalar_type not in('string','integer','decimal')or type(self.facets)is not tuple or any(type(v)is not tuple or len(v)!=2 or type(v[0])is not str or type(v[1])not in(int,bool)for v in self.facets)or len(dict(self.facets))!=len(self.facets):raise IndexedOracleDomainError('original-domain')
        facets=dict(self.facets)
        if self.scalar_type=='string'and facets:raise IndexedOracleDomainError('selected-string-domain')
        if self.scalar_type=='integer'and(set(facets)-{'bits','signed'}or('bits'in facets and(type(facets['bits'])is not int or not 1<=facets['bits']<=64))or('signed'in facets and type(facets['signed'])is not bool)):raise IndexedOracleDomainError('original-integer-domain')
        if self.scalar_type=='decimal'and(set(facets)!={'precision','scale'}or any(type(v)is not int for v in facets.values())or not 1<=facets['precision']<=38 or not 0<=facets['scale']<=facets['precision']):raise IndexedOracleDomainError('original-decimal-domain')

@dataclass(frozen=True)
class CurrentCommerceObject:
    source_system:str
    type_id:str
    id:str
    schema_revision:str
    props_json:str

    def __post_init__(self):
        for value in(self.source_system,self.type_id,self.id,self.schema_revision):name(value)
        if type(self.props_json)is not str:raise IndexedOracleDomainError('original-props')

@dataclass(frozen=True)
class IndexedCommercePopulation:
    objects:tuple
    homes:tuple
    maximum_source_bytes:int
    maximum_objects:int

    def __post_init__(self):
        if type(self.maximum_source_bytes)is not int or not 1<=self.maximum_source_bytes<=16*1024*1024 or type(self.maximum_objects)is not int or not 1<=self.maximum_objects<=10000:raise IndexedOracleCapacityError('source-budget')
        if type(self.objects)is not tuple or any(type(o)is not CurrentCommerceObject for o in self.objects)or len(self.objects)>self.maximum_objects or type(self.homes)is not tuple or any(type(h)is not AcceptedCommerceHome for h in self.homes)or not 1<=len(self.homes)<=4096:raise IndexedOracleCapacityError('source-inventory')
        if sum(len(o.props_json.encode('utf8'))for o in self.objects)>self.maximum_source_bytes:raise IndexedOracleCapacityError('source-bytes')
        sources={h.source_system for h in self.homes}
        if len(sources)!=1 or any(o.source_system not in sources for o in self.objects):raise IndexedOracleDomainError('single-original-source-required')
        identities=[(o.source_system,o.type_id,o.id)for o in self.objects]
        if len(set(identities))!=len(identities):raise IndexedOracleDomainError('duplicate-current-identity')
        keys=[(h.source_system,h.type_id,h.schema_revision,h.field_name)for h in self.homes]
        if len(set(keys))!=len(keys):raise IndexedOracleDomainError('duplicate-home')
        groups={}
        for home in self.homes:
            group=groups.setdefault((home.source_system,home.type_id,home.schema_revision),[]);group.append(home)
        if any(len({h.record for h in group})!=1 or len({h.property_id for h in group})!=len(group)for group in groups.values()):raise IndexedOracleDomainError('ambiguous-record-home')

    @classmethod
    def from_rows(cls,rows: list | tuple,homes: list | tuple,*,maximum_source_bytes: int,maximum_objects: int) -> IndexedCommercePopulation:
        """Snapshot five original current-row fields; never derive homes from compiler IR."""
        if type(rows)not in(list,tuple)or type(homes)not in(list,tuple):raise IndexedOracleDomainError('original-inventory')
        if len(rows)>maximum_objects:raise IndexedOracleCapacityError('source-inventory')
        objects=[]
        for row in rows:
            if type(row)is not dict or any(k not in row for k in('source_system','type_id','id','schema_revision','props_json')):raise IndexedOracleDomainError('original-current-row')
            objects.append(CurrentCommerceObject(*(row[k]for k in('source_system','type_id','id','schema_revision','props_json'))))
        return cls(tuple(objects),tuple(homes),maximum_source_bytes,maximum_objects)

@dataclass(frozen=True)
class NumericAtom:
    token:str

MAX_NUMERIC_TOKEN_BYTES=16384
MAX_SYMBOLIC_EXPONENT=1000000


def number_parts(token: str) -> tuple[int,str,int | None,int]:
    """Bounded lexical decomposition; None exponent carries its overflow direction."""
    if type(token)is not str:raise IndexedOracleDomainError('logical-numeric-token')
    if len(token)>MAX_NUMERIC_TOKEN_BYTES:raise IndexedOracleCapacityError('numeric-token-resource')
    match=re.fullmatch(r'(-?)(0|[1-9][0-9]*)(?:\.([0-9]+))?(?:[eE]([+-]?)([0-9]+))?',token)
    if match is None:raise IndexedOracleDomainError('logical-numeric-token')
    minus,whole,fraction,exponent_sign,exponent_digits=match.groups();fraction=fraction or''
    coefficient=(whole+fraction).lstrip('0')
    if not coefficient:return 0,'0',0,0
    trimmed=coefficient.rstrip('0');trailing=len(coefficient)-len(trimmed)
    exponent=0
    for digit in(exponent_digits or'0'):
        exponent=exponent*10+ord(digit)-48
        if exponent>MAX_SYMBOLIC_EXPONENT:
            return -1 if minus else 1,trimmed,None,-1 if exponent_sign=='-'else 1
    if exponent_sign=='-':exponent=-exponent
    return -1 if minus else 1,trimmed,exponent-len(fraction)+trailing,0


def integer_source_key(token: str) -> tuple[int,str,int]:
    """Symbolic mathematical equality key, not a native-width or authority proof.

    Canonical sign/significand/effective exponent never allocates an arbitrary
    integer coefficient or expands exponent zeros. Nonintegral values are domain
    refusals; bounded lexical/exponent resources refuse independently.
    """
    sign,digits,exponent,direction=number_parts(token)
    if exponent is None:raise IndexedOracleCapacityError('numeric-exponent-resource')
    if exponent<0:raise IndexedOracleDomainError('logical-integer-domain')
    return sign,digits,exponent


def pairs(items):
    result={}
    for key,value in items:
        if key in result:raise IndexedOracleDomainError('duplicate-props-member')
        result[key]=value
    return result

def current_truss_commerce_oracle(population: IndexedCommercePopulation,sql: str,*,maximum_rows: int,maximum_numeric_digits: int) -> dict:
    """Return positional lexical bags and native identity witnesses, not an authority grant."""
    if type(population)is not IndexedCommercePopulation or sql not in dict(CASES).values():raise IndexedOracleDomainError('selected-original-case')
    if type(maximum_rows)is not int or not 1<=maximum_rows<=10000 or type(maximum_numeric_digits)is not int or not 1<=maximum_numeric_digits<=38:raise IndexedOracleCapacityError('oracle-budget')
    homes={}
    for home in population.homes:homes.setdefault((home.source_system,home.type_id,home.schema_revision),{})[home.field_name]=home
    records={};props={}
    for obj in population.objects:
        key=(obj.source_system,obj.type_id,obj.schema_revision);selected=homes.get(key)
        if selected is None:raise IndexedOracleDomainError('unmapped-current-record')
        try:parsed=json.loads(obj.props_json,object_pairs_hook=pairs,parse_int=NumericAtom,parse_float=NumericAtom,parse_constant=lambda _:(_ for _ in()).throw(IndexedOracleDomainError('nonfinite-props')))
        except (ValueError,RecursionError)as error:raise IndexedOracleDomainError('original-props-json')from None
        if type(parsed)is not dict:raise IndexedOracleDomainError('original-props-object')
        props[obj]=parsed;records.setdefault(next(iter(selected.values())).record,[]).append(obj)
    def field(obj,field_name,family):
        home=homes[(obj.source_system,obj.type_id,obj.schema_revision)].get(field_name)
        if home is None or home.scalar_type!=family:raise IndexedOracleDomainError('selected-field-home')
        raw=props[obj].get(home.property_id)
        if family=='string':
            if type(raw)is not str:raise IndexedOracleDomainError('selected-string')
            return raw
        if type(raw)is not NumericAtom:raise IndexedOracleDomainError('selected-numeric-atom')
        token=raw.token;sign,digits,exponent,direction=number_parts(token);facets=dict(home.facets)
        # Mathematical membership precedes finite execution, where symbolically decidable.
        if family=='integer':
            if exponent is None:raise IndexedOracleCapacityError('numeric-exponent-resource')
            if exponent<0:raise IndexedOracleDomainError('logical-integer-domain')
            if facets.get('signed')is False and sign<0:raise IndexedOracleDomainError('logical-integer-facets')
        else:
            if exponent is None:raise IndexedOracleDomainError('logical-decimal-facets')
            if sign and(max(0,len(digits)+exponent)>facets['precision']-facets['scale']or max(0,-exponent)>facets['scale']):raise IndexedOracleDomainError('logical-decimal-facets')
        if exponent is None:raise IndexedOracleCapacityError('numeric-exponent-resource')
        required_digits=1 if not sign else len(digits)+max(0,exponent)
        if family=='integer'and'bits'in facets:
            bits=facets['bits'];signed=facets.get('signed',True)
            boundary=str(2**(bits-1)if signed and sign<0 else 2**(bits-1)-1 if signed else 2**bits-1)
            if required_digits>len(boundary)or(required_digits==len(boundary)and(digits+'0'*max(0,exponent))>boundary):raise IndexedOracleDomainError('logical-integer-facets')
        if required_digits>maximum_numeric_digits or len(digits)>maximum_numeric_digits or abs(exponent)>maximum_numeric_digits:raise IndexedOracleCapacityError('numeric-execution-capacity')
        coefficient=sign*int(digits);number=Fraction(coefficient*10**max(0,exponent),10**max(0,-exponent))
        # Native result expectation is canonical; original lexical token stays in props_json.
        return str(number.numerator)if family=='integer'else token,number
    def rows(record):return records.get(record,())
    def join(left,right,lf,rf):return ((a,b)for a in rows(left)for b in rows(right)if field(a,lf,'string')==field(b,rf,'string'))
    output=[];witnesses=[]
    def retain(row,objects):
        if len(output)>=maximum_rows:raise IndexedOracleCapacityError('result-bag-capacity')
        output.append(row);witnesses.append([[o.source_system,o.type_id,o.id]for o in objects])
    if sql==CASES[0][1]:
        for line,fulfillment in join('order_lines','fulfillments','id','line_id'):
            for returned in rows('returns'):
                if field(returned,'line_id','string')!=field(line,'id','string'):continue
                a,b,c=(field(o,'quantity','integer')for o in(line,fulfillment,returned))
                if b[1]<a[1]:
                    result=a[1]-b[1]+c[1]
                    if len(str(abs(result.numerator)))>maximum_numeric_digits:raise IndexedOracleCapacityError('arithmetic-execution-capacity')
                    retain([b[0],c[0],str(result.numerator)],(line,fulfillment,returned))
    elif sql==CASES[1][1]:
        for fulfillment,line in join('fulfillments','order_lines','line_id','id'):
            if field(fulfillment,'quantity','integer')[1]>field(line,'quantity','integer')[1]:retain([field(fulfillment,'id','string')],(fulfillment,line))
        ordered=sorted(zip(output,witnesses),key=lambda pair:pair[0][0].encode('utf8'));output=[r for r,_ in ordered];witnesses=[w for _,w in ordered]
    elif sql==CASES[2][1]:
        for payment,invoice in join('payments','invoices','invoice_id','id'):
            if field(payment,'amount','decimal')[1]==field(invoice,'amount','decimal')[1]and field(payment,'currency','string')==field(invoice,'currency','string'):retain([field(payment,'id','string')],(payment,invoice))
    else:
        for refund,returned in join('refunds','returns','return_id','id'):
            for line,product in join('order_lines','products','product_id','id'):
                if field(returned,'line_id','string')!=field(line,'id','string'):continue
                product_value=field(returned,'quantity','integer')[1]*field(product,'unit_price','decimal')[1]
                if len(str(abs(product_value.numerator)))>maximum_numeric_digits:raise IndexedOracleCapacityError('arithmetic-execution-capacity')
                if field(refund,'amount','decimal')[1]==product_value:retain([field(refund,'id','string')],(refund,returned,line,product))
    return {'rows':output,'witnesses':{'current_native_occurrences':witnesses,'count':len(output)},'scope':'Complete supplied current-row relational occurrence bag and accepted-home mathematical oracle; no compiler/native/source authority, capacity guard discharge or implicit integer width.'}
