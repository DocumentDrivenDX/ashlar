"""Selected string-record binding planning from retained UMF interpretation.

This is a target consumer profile, not UMF validation or native acceptance.
Unknown semantics block executable admission; original evidence is retained.
"""
import base64
import hashlib
from .schema import _json

class BindingError(ValueError):
    pass


def plan_string_record_binding(raw: bytes, *, trusted_validator_revision: str):
    report = _json(raw)
    if not isinstance(report,dict) or report.get('format')!='ashlar-umf-interpretation/0.1' or report.get('validatorRevision')!=trusted_validator_revision:
        raise BindingError('Untrusted interpretation profile/source')
    try: source_bytes=base64.b64decode(report['sourceBase64'],validate=True)
    except (KeyError,ValueError,TypeError) as exc: raise BindingError('Invalid source custody') from exc
    if hashlib.sha256(source_bytes).hexdigest()!=report.get('sourceSha256'):
        raise BindingError('Source digest mismatch')
    source=_json(source_bytes)
    if report.get('validation',{}).get('valid') is not True:
        raise BindingError('No successful UMF structural validation')
    elements=report.get('elements')
    if not isinstance(elements,list) or not isinstance(source,dict): raise BindingError('Invalid complete interpretation inventory')
    if source.get('umf')!='0.7.0': raise BindingError('Selected binding requires UMF core 0.7.0')
    original={}
    for mi,module in enumerate(source['modules']):
        for ei,element in enumerate(module['elements']):
            key=(source['id'],module['id'],element['id'])
            if key in original: raise BindingError('Duplicate authored identity')
            original[key]=(element,f'/modules/{mi}/elements/{ei}')
    receipts={}
    for item in elements:
        key=tuple(item['identity'])
        if key not in original or key in receipts: raise BindingError('Foreign/duplicate interpretation')
        for category in ['kind','nullability','cardinality','facets','keys']:
            receipt=item.get(category,{})
            if receipt.get('source')!=source or receipt.get('identity')!={'module':key[1],'element':key[2]}:
                raise BindingError('Interpretation/source correspondence mismatch')
            expected_path=original[key][1]+'/'+category
            if receipt.get('path')!=expected_path: raise BindingError('Interpretation source pointer mismatch')
        receipts[key]=item
    if set(receipts)!=set(original): raise BindingError('Incomplete interpretation membership')
    blocked=[];types=[];properties=[];covered=set()
    def refuse(path,reason):blocked.append({'sourcePointer':path,'reason':reason,'enforcement':'none','retained':True})
    for key,(element,path) in original.items():
        kind=receipts[key]['kind']['meaning']
        if kind!={'state':'known','kind':'record'}:continue
        covered.add(key);types.append({'identity':list(key),'kind':'record','displayName':element.get('name'),'sourcePointer':path,'enforcement':'engine-unimplemented'})
        if receipts[key]['keys']['meaning']['state']!='missing':refuse(path+'/keys','Key execution requires a separate selected key binding')
        for k in element:
            if k not in ['id','name','description','kind','members','extensions']:refuse(path+'/'+k,'Record assertion has no selected binding')
        if element.get('extensions'):refuse(path+'/extensions','Extension meaning has no selected binding')
        seen=set()
        for i,member in enumerate(element.get('members',[])):
            target=(key[0],member.get('module'),member.get('element'))
            member_path=path+'/members/'+str(i)
            if set(member)!= {'module','element'} or target not in receipts or target in seen:
                refuse(member_path,'Unknown, duplicate or unresolved member');continue
            seen.add(target);covered.add(target)
            field,field_path=original[target];meaning=receipts[target]
            if meaning['kind']['meaning']!={'state':'known','kind':'field'} or field.get('scalarType')!='string':
                refuse(field_path,'Selected profile requires an interpreted scalar string Field');continue
            nullability=meaning['nullability']['meaning']
            if nullability.get('state')!='known' or nullability.get('nullability') not in ['required','absent-allowed']:
                refuse(field_path+'/nullability','Unknown or unsupported availability; no token substitution')
            if meaning['cardinality']['meaning']!={'state':'known','cardinality':'one'}:
                refuse(field_path+'/cardinality','Selected profile requires interpreted singleton shape')
            if meaning['facets']['meaning']['state']!='missing':refuse(field_path+'/facets','Facet execution requires a separate selected binding')
            for k in field:
                if k not in ['id','name','description','kind','scalarType','nullability','cardinality','extensions']:
                    refuse(field_path+'/'+k,'Field assertion has no selected binding')
            if field.get('extensions'):refuse(field_path+'/extensions','Extension meaning has no selected binding')
            properties.append({'identity':[key[0],key[1],key[2],target[1],target[2]],'sourcePointer':field_path,'displayName':field.get('name'),
                'scalarType':'string','nullability':nullability,'cardinality':meaning['cardinality']['meaning'],
                'home':'props','trussCarrier':'JSONB property-ID member','ashlarCarrier':'retained property-ID JSON member',
                'enforcement':'engine-unimplemented'})
    if not types:refuse('/modules','No interpreted Record in selected closure')
    for key in sorted(set(original)-covered):refuse(original[key][1],'Element lies outside selected record/member closure')
    for k in source:
        if k not in ['umf','id','vocabularies','modules']:refuse('/'+k,'Document assertion has no selected binding')
    if source.get('vocabularies'):refuse('/vocabularies','Vocabulary meaning has no selected binding')
    for mi,module in enumerate(source['modules']):
        for k in module:
            if k not in ['id','namespace','elements']:refuse(f'/modules/{mi}/{k}','Module assertion has no selected binding')
    return {'format':'ashlar-string-record-binding-plan/0.1','status':'blocked' if blocked else 'candidate',
            'sourceSha256':report['sourceSha256'],'interpretationSha256':hashlib.sha256(raw).hexdigest(),
            'validatorRevision':trusted_validator_revision,'types':types,'properties':properties,'blocked':blocked,
            'originalInterpretationBase64':base64.b64encode(raw).decode('ascii'),
            'qualification':'Selected string-record target plan only; engine enforcement unimplemented, no IDs allocated, native acceptance or data ingestion authority'}
