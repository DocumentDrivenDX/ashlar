"""Typed singleton source adapter using original public UMF Record evidence.

Transport token translation and custody checks only; UMF owns value/presence
constraints. No native catalog IDs, dataset keys, relationships or ACK authority.
"""
import base64
from dataclasses import dataclass
import hashlib
import json
from types import MappingProxyType
from .schema import _json,_pairs

RECORD_CHECK_PIN='c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3'

class TypedSourceError(ValueError):pass

@dataclass(frozen=True)
class NumericToken:
    text:str


def typed_values(props_json,fields):
    """Translate exact JSON tokens through explicit admitted property-ID mappings.

    fields maps property ID to (module,element,scalar family). Numeric lexical
    tokens are passed intact to UMF, including exponents/signed zero; their native
    query compatibility must be admitted separately. Missing fields stay absent.
    """
    if type(props_json) is not str or len(props_json.encode('utf-8'))>1048576:
        raise TypedSourceError('Bounded original property JSON required')
    try:
        values=json.loads(props_json,object_pairs_hook=_pairs,parse_int=NumericToken,parse_float=NumericToken,
                          parse_constant=lambda _:(_ for _ in ()).throw(TypedSourceError('Nonfinite token')))
    except (ValueError,UnicodeError,RecursionError) as error:raise TypedSourceError('Invalid original property JSON') from error
    if type(values) is not dict or not set(values).issubset(fields):raise TypedSourceError('Unmapped executable property ID')
    result=[]
    for pid,value in values.items():
        module,element,family=fields[pid]
        if value is None:literal=None
        elif isinstance(value,NumericToken) and family in ('integer','decimal'):
            literal={family+'Token':value.text}
        elif type(value) is bool:literal={'boolean':value}
        elif type(value) is str:literal={'string':value}
        else:raise TypedSourceError('Unsupported scalar transport carrier')
        result.append({'field':{'module':module,'element':element},'state':'present','value':literal})
    return result


def _complete_validation(value):
    return (type(value) is dict and set(value)=={'valid','complete','diagnostics'}
            and value['valid'] is True and value['complete'] is True
            and type(value['diagnostics']) is list and value['diagnostics']==[])

class TypedSourcePolicy:
    """Bind complete original UMF checks to exact individual graph changes.

    bindings are independently admitted explicit IDs, not inferred allocations:
    type ID -> {'record':(module,element),'fields':{property ID:(module,element,family)}}.
    The host must authenticate the original producer and separately admit source,
    revisions, IDs and target homes. A report valid flag alone cannot do that.
    """
    def __init__(self,source,receipt,bindings,*,source_system,schema_revision,trusted_producer_revision):
        if trusted_producer_revision!=RECORD_CHECK_PIN or type(source) is not bytes or type(receipt) is not bytes:
            raise TypedSourceError('Pinned original public UMF evidence required')
        model=_json(source);report=_json(receipt)
        if model.get('umf')!='0.8.0' or report.get('format')!='ashlar-typed-record-check/0.1' or report.get('producerRevision')!=trusted_producer_revision:
            raise TypedSourceError('Authored core0.8 source and typed check profile required')
        try:original=base64.b64decode(report['sourceBase64'],validate=True);request=base64.b64decode(report['requestBase64'],validate=True)
        except (KeyError,ValueError,TypeError) as error:raise TypedSourceError('Original byte custody required') from error
        if original!=source or report.get('sourceSha256')!=hashlib.sha256(source).hexdigest() or report.get('requestSha256')!=hashlib.sha256(request).hexdigest():
            raise TypedSourceError('Original source/request correspondence differs')
        req=_json(request)
        if type(req) is not dict or set(req)!= {'records'} or type(req['records']) is not list or type(report.get('records')) is not list or len(req['records'])!=len(report['records']):
            raise TypedSourceError('Complete ordered Record custody required')
        if not source_system or not schema_revision:raise TypedSourceError('Explicit source/revision required')
        self.bindings={};seen=set()
        for tid,binding in bindings.items():
            if type(tid) is not int or not 1<=tid<2**31 or type(binding) is not dict or set(binding)!= {'record','fields'}:
                raise TypedSourceError('Explicit active target binding required')
            record=tuple(binding['record']);fields={}
            if len(record)!=2 or any(type(v) is not str or not v for v in record) or record in seen:raise TypedSourceError('Unambiguous Record identity required')
            seen.add(record)
            for pid,identity in binding['fields'].items():
                if type(pid) is not str or not pid.isascii() or not pid.isdecimal() or str(int(pid))!=pid or not 1<=int(pid)<2**31:
                    raise TypedSourceError('Exact positive property catalog ID required')
                ref=tuple(identity)
                if len(ref)!=3 or any(type(v) is not str or not v for v in ref) or ref[2] not in ('integer','decimal','boolean','string'):
                    raise TypedSourceError('Explicit supported scalar Field binding required')
                fields[pid]=ref
            self.bindings[tid]=MappingProxyType({'record':record,'fields':MappingProxyType(fields)})
        self.bindings=MappingProxyType(self.bindings)
        self.source=source;self.receipt=receipt;self.source_system=source_system;self.schema_revision=schema_revision
        self.source_sha256=hashlib.sha256(source).hexdigest();self.receipt_sha256=hashlib.sha256(receipt).hexdigest()
        self._checks={}
        for wanted,observed in zip(req['records'],report['records']):
            if type(wanted) is not dict or set(wanted)!= {'deliveryId','recordSha256','typeId','propsSha256','identity','values'} or type(observed) is not dict or set(observed)!= {'request','result'} or observed['request']!=wanted:
                raise TypedSourceError('Original ordered record request differs')
            result=observed['result']
            if result.get('operation')!='validate-core-record-values' or result.get('version')!='1.0.0' or result.get('source')!=model or result.get('identity')!=wanted['identity'] or result.get('values')!=wanted['values'] or result.get('documentValidation',{}).get('valid') is not True or not _complete_validation(result.get('validation')):
                raise TypedSourceError('Complete public Record validation required')
            # Verify original public per-member observation custody, not constraints.
            document_validation=result.get('documentValidation')
            if type(document_validation) is not dict or set(document_validation)!= {'valid','complete','diagnostics'} or type(document_validation['complete']) is not bool or type(document_validation['diagnostics']) is not list:
                raise TypedSourceError('Original complete document-validation shape required')
            identity=wanted['identity']
            owner=next((e for m in model.get('modules',[]) if m['id']==identity['module'] for e in m['elements'] if e['id']==identity['element']),None)
            if owner is None or owner.get('kind')!='record' or type(owner.get('members')) is not list:
                raise TypedSourceError('Original declared Record members required')
            fields=result.get('fields')
            if type(fields) is not list or len(fields)!=len(owner['members']):
                raise TypedSourceError('Complete ordered original member observations required')
            supplied={tuple(v['field'][k] for k in ('module','element')):v['state'] for v in wanted['values']}
            if len(supplied)!=len(wanted['values']):raise TypedSourceError('Duplicate original field-state request')
            for member,observation in zip(owner['members'],fields):
                state=supplied.get((member['module'],member['element']),'absent')
                if type(observation) is not dict or set(observation)!= {'field','state','validation'} or observation['field']!=member or observation['state']!=state or not _complete_validation(observation['validation']):
                    raise TypedSourceError('Original member identity/state/validation correspondence differs')
            key=(wanted['deliveryId'],wanted['recordSha256'])
            if key in self._checks:raise TypedSourceError('Duplicate original delivery evidence')
            self._checks[key]=wanted

    def request(self,change):
        state=change.state
        if state.key.kind!='object' or state.key.source!=self.source_system or state.schema_revision!=self.schema_revision or state.key.type_id not in self.bindings:
            raise TypedSourceError('Unsupported source/revision/Record binding')
        binding=self.bindings[state.key.type_id]
        module,element=binding['record']
        return {'deliveryId':change.delivery_id,'recordSha256':change.raw_digest,'typeId':state.key.type_id,
                'propsSha256':hashlib.sha256(state.props_json.encode('utf-8')).hexdigest(),
                'identity':{'module':module,'element':element},'values':typed_values(state.props_json,binding['fields'])}

    def __call__(self,change):
        request=self.request(change)
        if self._checks.get((change.delivery_id,change.raw_digest))!=request:
            raise TypedSourceError('No original complete UMF check for these exact properties')
