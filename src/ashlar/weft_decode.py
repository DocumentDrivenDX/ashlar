"""Exact selected string/presence results; no generic JSON or numeric coercion."""
from .publication import ResolutionError
from .schema import _json
from types import MappingProxyType
from dataclasses import dataclass
from decimal import Decimal
import re


def decode_string_column(column,value,policy,intake):
    if policy.source_sha256!=intake.source_sha256:
        raise ResolutionError('Original result model differs from admitted binding')
    representation=column['representation']
    def text(value):
        if type(value) is not str or '\x00' in value:raise ResolutionError('Exact Unicode string required')
        try:value.encode('utf-8')
        except UnicodeError as error:raise ResolutionError('Unicode scalar string required') from error
        return value
    if representation.get('kind')=='scalar':
        if representation!={'carrier':'text','decoder':'text','kind':'scalar','logicalType':{'facets':{},'family':'string','nullable':False}}:
            raise ResolutionError('Unadmitted scalar decoder')
        return text(value)
    if representation.get('kind')!='value' or set(representation)!= {'descriptor','kind','nativeNull'} or representation['nativeNull'] is not False:
        raise ResolutionError('Unadmitted value decoder')
    identity=representation['descriptor']
    if set(identity)!= {'documentId','revision','module','element'} or identity['documentId']!=intake.document_id or identity['revision']!=intake.document_revision:
        raise ResolutionError('Original selected Field identity required')
    availability=[]
    for type_id,fields in policy.property_fields.items():
        for pid,ref in fields.items():
            if ref==(identity['module'],identity['element']):availability.append(policy.types[type_id][pid])
    if not availability or len(set(availability))!=1:raise ResolutionError('Unadmitted result Field')
    item=_json(text(value).encode('utf-8'))
    if type(item) is not dict:raise ResolutionError('Exact value-state envelope required')
    if item=={'state':'absent'} and availability[0]=='absent-allowed':return MappingProxyType(item)
    if set(item)=={'state','value'} and item['state']=='value':
        text(item['value']);return MappingProxyType(item)
    raise ResolutionError('Invalid absence/null/value result')


# Source contract: Weft 2744531735c2a771fbe7ed24a7f67e3afc851b25,
# crates/weft-core/src/{backend.rs,backend_emission.rs,exact.rs,model.rs}.
# This decoder foundation grants no model/field binding or native admission.
@dataclass(frozen=True)
class ExactScalar:
    """Exact host value plus original carrier (including decimal signed zero)."""
    value: object
    original: object


def decode_exact_scalar(representation, value, *, count=False):
    """Decode selected non-null public scalar metadata without numeric rounding.

    Count admission is explicit: only facetless integer COUNT output is accepted
    with count=True, using nonnegative signed BIGINT output bounds. Integer fields
    require integerWidth (1..64 bits); decimal fields require precision1..28 and
    scale0..precision. Nullable SUM and exponent notation remain unsupported.
    Caller must independently admit the original compiler column and publication.
    This function is intentionally separate from decode_string_column and never
    enables numeric/presence Field bindings.
    """
    fail = lambda message: ResolutionError(message)
    if type(count) is not bool or type(representation) is not dict or set(representation) != {'kind','logicalType','carrier','decoder'} or representation['kind'] != 'scalar':
        raise fail('Exact selected scalar representation required')
    logical=representation['logicalType']
    if type(logical) is not dict or set(logical) != {'family','facets','nullable'} or logical['nullable'] is not False or type(logical['facets']) is not dict:
        raise fail('Explicit non-null scalar logical type required')
    family,facets=logical['family'],logical['facets']
    carrier,decoder=representation['carrier'],representation['decoder']
    if any(type(v) is not str for v in (family,carrier,decoder)):
        raise fail('Scalar family, carrier and decoder must be exact names')
    if count and (family != 'integer' or facets):
        raise fail('Count admission requires facetless integer output')
    expected={'string':('text','text'),'integer':('text','exact-integer'),'decimal':('text','exact-decimal')}
    if family == 'boolean':
        if facets or decoder != 'boolean' or carrier not in ('text','boolean'):
            raise fail('Unadmitted boolean representation')
        if carrier == 'boolean':
            if type(value) is not bool:raise fail('Native boolean carrier required')
            return ExactScalar(value,value)
        if type(value) is not str or value not in ('true','false'):
            raise fail('Canonical boolean text required')
        return ExactScalar(value == 'true',value)
    if family not in expected or (carrier,decoder) != expected[family]:
        raise fail('Unsupported scalar carrier or decoder')
    if type(value) is not str or '\x00' in value:
        raise fail('Exact text scalar carrier required')
    try:value.encode('utf-8')
    except UnicodeError as error:raise fail('Unicode scalar string required') from error
    if family == 'string':
        if facets:raise fail('Unknown string facets')
        return ExactScalar(value,value)
    # Bounded decoding is a host subset. Do not feed unbounded text to int/Decimal.
    if len(value)>1024:raise fail('Numeric carrier exceeds decoder bound')
    if family == 'integer':
        if count:
            minimum,maximum=0,2**63-1
        else:
            if set(facets) != {'integerWidth'} or type(facets['integerWidth']) is not dict:
                raise fail('Explicit integer width required')
            width=facets['integerWidth']
            if set(width) != {'bits','signed'} or type(width['bits']) is not int or not 1<=width['bits']<=64 or type(width['signed']) is not bool:
                raise fail('Unsupported integer width')
            bits=width['bits']
            minimum,maximum=(-(2**(bits-1)),2**(bits-1)-1) if width['signed'] else (0,2**bits-1)
        if not re.fullmatch(r'-?[0-9]+',value):raise fail('Integral base-ten text required')
        number=int(value)
        if not minimum<=number<=maximum:raise fail('Integer outside declared domain')
        return ExactScalar(number,value)
    if set(facets) != {'precision','scale'} or type(facets['precision']) is not int or type(facets['scale']) is not int or not 1<=facets['precision']<=28 or not 0<=facets['scale']<=facets['precision']:
        raise fail('Unsupported exact decimal facets')
    if not re.fullmatch(r'-?[0-9]+(?:\.[0-9]+)?',value):
        raise fail('Exact base-ten decimal text required; exponent unsupported')
    whole,_,fraction=value.lstrip('-').partition('.')
    fraction=fraction.rstrip('0')
    scale=facets['scale']
    if len(fraction)>scale:raise fail('Decimal scale outside declared domain')
    coefficient=whole+fraction+'0'*(scale-len(fraction))
    if len(coefficient.lstrip('0'))>facets['precision']:
        raise fail('Decimal precision outside declared domain')
    return ExactScalar(Decimal(value),value)


def admit_compiled_count(column,artifact,policy,intake):
    """Admit a proved global COUNT from original public 0.2 IR.

    Host must supply its freshly recompiled/admitted original artifact; this helper
    does not authenticate compiler custody or execute its obligations. Only one
    unfiltered, unjoined global COUNT over an admitted string Record is added.
    SQL text is never interpreted to identify aggregation. Numeric Fields, SUM,
    grouped counts and other logical plans remain outside this result route.
    """
    from .semantic_policy import StringRecordPolicy
    from .schema import SchemaIntake
    if not isinstance(policy,StringRecordPolicy) or not isinstance(intake,SchemaIntake) or policy.source_sha256!=intake.source_sha256:
        raise ResolutionError('Original admitted string policy and intake required')
    if type(artifact) is not dict or type(column) is not dict:
        raise ResolutionError('Original compiled column required')
    columns=artifact.get('columns')
    if type(columns) is not list or not columns or any(type(c) is not dict or type(c.get('position')) is not int or c['position']!=i for i,c in enumerate(columns,1)):
        raise ResolutionError('Complete ordered original compiler columns required')
    if type(column.get('position')) is not int or not 1<=column['position']<=len(columns) or columns[column['position']-1]!=column:
        raise ResolutionError('Result column differs from original compiler output')
    representation=column.get('representation')
    if type(representation) is not dict:raise ResolutionError('Result representation required')
    logical=representation.get('logicalType')
    if representation.get('kind')=='scalar' and type(logical) is not dict:
        raise ResolutionError('Complete scalar logical type required')
    if representation.get('kind')!='scalar' or logical.get('family')!='integer':
        raise ResolutionError('Only scalar integer COUNT can be admitted')
    pin={'documentId':intake.document_id,'revision':intake.document_revision,'sha256':intake.source_sha256,'umfVersion':'0.7.0'}
    if artifact.get('status')!='compiled' or artifact.get('interfaceVersion')!='weft-compile/0.2.0' or artifact.get('dialect')!='weft-sql/0.2.0' or artifact.get('compilerVersion')!='weft/0.1.0' or artifact.get('modelPins')!=[pin]:
        raise ResolutionError('Exact original compiler/model count artifact required')
    plan=artifact.get('logicalPlan')
    expected_keys={'aggregate','filters','groups','irVersion','joins','limit','modulePins','order','outputs','pageKey','readProfile','requiredCapabilities','source','typeGraph'}
    if type(plan) is not dict or set(plan)!=expected_keys or plan['irVersion']!='weft-ir/0.2.0' or plan['aggregate'] is not True or plan['modulePins']!=[pin]:
        raise ResolutionError('Original public aggregate IR required')
    if any(plan[k]!=[] for k in ['filters','groups','joins','order','typeGraph']) or any(plan[k] is not None for k in ['limit','pageKey','readProfile']) or plan['requiredCapabilities']!=['aggregate','aggregate.count','project','scan']:
        raise ResolutionError('Only unfiltered single-record global COUNT admitted')
    source=plan['source']
    if type(source) is not dict or set(source)!= {'occurrence','pin','record'} or source['occurrence']!='s0' or source['pin']!=pin:
        raise ResolutionError('Exact original count source required')
    identity=source['record']
    admitted=[{'documentId':intake.document_id,'revision':intake.document_revision,'module':module,'element':element} for module,element in policy.record_identities.values()]
    if identity not in admitted or column.get('sourceIdentities')!=[identity]:
        raise ResolutionError('Count source must correspond to admitted Record identity')
    logical={'family':'integer','facets':{},'nullable':False}
    if len(columns)!=1 or set(column)!= {'nullable','outputName','position','representation','sourceIdentities'} or column['nullable'] is not False or type(column['outputName']) is not str or not column['outputName']:
        raise ResolutionError('One complete non-null global count column required')
    if plan['outputs']!=[{'name':column['outputName'],'expression':{'op':'count','type':logical}}]:
        raise ResolutionError('Original output expression must prove COUNT')
    if representation != {'kind':'scalar','logicalType':logical,'carrier':'text','decoder':'exact-integer'} or logical != {'family':'integer','facets':{},'nullable':False}:
        raise ResolutionError('Exact non-null facetless COUNT representation required')
    return MappingProxyType(dict(identity))


def decode_compiled_column(column,value,artifact,policy,intake):
    """Decode admitted COUNT with original carrier custody, or existing strings.

    Host must admit/recompile the original artifact and execute every obligation.
    Metadata admission never manufactures a value observation.
    """
    if type(column) is not dict or type(column.get('representation')) is not dict:
        raise ResolutionError('Complete original compiler column required')
    representation=column['representation']
    if representation.get('kind')=='scalar':
        logical=representation.get('logicalType')
        if type(logical) is not dict:raise ResolutionError('Complete scalar logical type required')
        if logical.get('family')=='integer':
            admit_compiled_count(column,artifact,policy,intake)
            return decode_exact_scalar(representation,value,count=True)
    columns=artifact.get('columns') if type(artifact) is dict else None
    if type(columns) is not list or any(type(c) is not dict or type(c.get('position')) is not int or c['position']!=i for i,c in enumerate(columns,1)) or type(column.get('position')) is not int or not 1<=column['position']<=len(columns) or columns[column['position']-1]!=column:
        raise ResolutionError('Complete ordered original compiler columns required')
    return decode_string_column(column,value,policy,intake)


def admit_count_rows(rows):
    """Require the single exact global COUNT cell, including an empty-input zero."""
    if type(rows) is not tuple or len(rows)!=1 or type(rows[0]) is not tuple or len(rows[0])!=1 or not isinstance(rows[0][0],ExactScalar):
        raise ResolutionError('Global COUNT requires exactly one decoded result cell')
    cell=rows[0][0]
    representation={'kind':'scalar','logicalType':{'family':'integer','facets':{},'nullable':False},'carrier':'text','decoder':'exact-integer'}
    if type(cell.value) is not int or cell!=decode_exact_scalar(representation,cell.original,count=True):
        raise ResolutionError('Exact original COUNT cell required')
