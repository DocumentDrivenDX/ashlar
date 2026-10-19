# SPDX-License-Identifier: MIT OR Apache-2.0
"""Transport original Core full ordering state; never compare or rewrite SQL."""
import copy
import hashlib
from time import monotonic

from umf_access.codec_support import revalidate_model
from umf_access.engine_catalog import EngineRead
from umf_access.native_program import canonical_json,program_json
from umf_access.pages import ReadContinuation,ReadPage,StaleContinuation
from umf_access.types import MISSING

from .execution_scope import preserve_body_failure
from .publication import ResolutionError
from .schema import _json
from .sdk_presence import DecodedPresence,SDK_BACKENDS,descriptor

REQUIREMENTS=[
 'authorize every hidden original sort Field before native execution',
 'retain exact state/value tuple and selected Key with original immutable publication and actor context',
 'missing and null share ordering class but keep distinct transported states',
 'native Unicode binary and exact numeric comparisons; no tuple comparison or cursor value rewriting',
]


def admit_metadata(artifact,guard):
    if type(artifact)is not dict or artifact.get('backend')!=SDK_BACKENDS[1]:
        raise ResolutionError('Original ordered native profile required')
    plan=artifact.get('logicalPlan')
    if (type(plan)is not dict or plan.get('version')!='weft-ir/engine-ordered-page/0.1'
            or type(guard)is not dict or set(guard)!={'version','selectedKey','sortSlots','keySlots','pageSize','requirements'}
            or guard.get('version')!='ashlar.ordered-page/0.1' or guard.get('requirements')!=REQUIREMENTS
            or any(guard.get(g)!=plan.get(p)for g,p in [('selectedKey','selectedKey'),('sortSlots','sortSlots'),('keySlots','keySlots'),('pageSize','pageSize')])):
        raise ResolutionError('Complete original ordering obligation correspondence required')
    slots=plan.get('sortSlots');indices=plan.get('keySlots');key=plan.get('selectedKey');base=plan.get('base')
    if (type(slots)is not list or not 1<=len(slots)<=1024 or type(indices)is not list
            or type(key)is not dict or set(key)!={'id','fields','types'} or not key['fields']
            or len(key['fields'])!=len(key['types']) or len(indices)!=len(key['fields'])
            or type(base)is not dict or base.get('pageKey')!=key or type(base.get('outputs'))is not list
            or type(plan.get('pageSize'))is not int or not 1<=plan['pageSize']<=999):
        raise ResolutionError('Original complete selected Key/order inventory required')
    columns=artifact.get('columns');visible=len(base['outputs'])
    if type(columns)is not list or len(columns)!=visible+2*len(slots):
        raise ResolutionError('Complete hidden original state/value columns required')
    for i,slot in enumerate(slots):
        if (type(slot)is not dict or set(slot)!={'identity','logicalType','descending','nullsFirst','nullable','missing','valueOutput','stateOutput'}
                or any(type(slot[k])is not bool for k in ('descending','nullsFirst','nullable','missing'))
                or type(slot['valueOutput'])is not int or slot['valueOutput']!=visible+2*i
                or type(slot['stateOutput'])is not int or slot['stateOutput']!=visible+2*i+1):
            raise ResolutionError('Original closed ordering slots required')
        selected=descriptor(columns[slot['valueOutput']],artifact)
        if (selected['identity']!=slot['identity'] or selected['type']!=slot['logicalType']
                or slot['missing']!=(selected['availability']=='absent-allowed')
                or slot['nullable']!=(selected['type']['nullable']or slot['missing'])):
            raise ResolutionError('Original sort Field domain differs')
        state=columns[slot['stateOutput']]
        if (state.get('outputName')!=f'__weft_sort_state_{i}' or state.get('position')!=slot['stateOutput']+1
                or state.get('sourceIdentities')!=[slot['identity']]or state.get('nullable')is not False
                or state.get('representation')!={'kind':'scalar','logicalType':{'family':'string','facets':{},'nullable':False},'carrier':'text','decoder':'text'}):
            raise ResolutionError('Original hidden state descriptor differs')
    for ordinal,index in enumerate(indices):
        if type(index)is not int or not 0<=index<len(slots):raise ResolutionError('Original selected Key slot index required')
        slot=slots[index]
        if (slot['identity']!=key['fields'][ordinal] or slot['logicalType']!=key['types'][ordinal]
                or slot['descending']or slot['nullable']or slot['missing']):
            raise ResolutionError('Original selected Key ASC suffix differs')
    if indices!=list(range(len(slots)-len(indices),len(slots))):
        raise ResolutionError('Original explicit complete Key suffix positions required')
    return slots


def _transport(value,carrier):
    from .access import _parameter
    if value is MISSING:return {'state':'missing'}
    if value is None:return {'state':'null'}
    token=_parameter(value)
    if token['family']!=carrier:raise ResolutionError('Original cursor scalar carrier differs')
    return {'state':'present','carrier':carrier,'value':token['value']}


def decoded_cursor(row,metadata,artifact):
    plan=artifact['logicalPlan'];guards=[o['parameters']for o in artifact['obligations']if o['id']=='ashlar.candidate.orderedPage']
    if len(guards)!=1:raise ResolutionError('Unique original ordered obligation required')
    slots=admit_metadata(artifact,guards[0])
    if (metadata.get('version')!='weft.engine-cursor/0.2' or metadata.get('pageSize')!=plan['pageSize']
            or metadata.get('visibleOutputs')!=len(plan['base']['outputs']) or metadata.get('keySlots')!=plan['keySlots']
            or metadata.get('key')!=plan['selectedKey'] or type(metadata.get('sortSlots'))is not list
            or len(metadata['sortSlots'])!=len(slots)):
        raise ResolutionError('Original public cursor metadata differs')
    values=[];states=[]
    for i,(slot,public) in enumerate(zip(slots,metadata['sortSlots'],strict=True)):
        expected={'field':slot['identity'],'carrier':slot['logicalType']['family'],'facets':slot['logicalType']['facets'],
                  'direction':'desc'if slot['descending']else'asc','nulls':'first'if slot['nullsFirst']else'last',
                  'nullable':slot['nullable'],'missing':slot['missing'],'valueOutput':slot['valueOutput'],'stateOutput':slot['stateOutput']}
        if public!=expected:raise ResolutionError('Original sort transport correspondence differs')
        value=row[slot['valueOutput']];state=row[slot['stateOutput']]
        if type(value)is not DecodedPresence or type(state)is not str or state!=value.state:
            raise ResolutionError('Original hidden state and value disagree')
        actual=MISSING if state=='missing'else None if state=='null'else value.value
        token=_transport(actual,public['carrier'])
        values.append(actual);states.append(token)
    key=[states[index]for index in metadata['keySlots']]
    if any(item['state']!='present'for item in key):raise ResolutionError('Original selected Key cannot be missing or null')
    return tuple(values),{'key':key,'sortValues':states}


def page(target,entry,parameters,*,after=None,timeout=None):
    from .access import _Policy,_parameter
    from .weft_query import read_weft,PageReadRows,PageContextRefused
    if type(entry)is not EngineRead or target.native_target_json is None:
        raise ResolutionError('Original immutable ordered engine page catalog required')
    program=entry.artifact();request=copy.deepcopy(program['request'])
    if (program['version']!='weft.engine-page/0.2' or program['compiler']!=target.compiler_method
            or request['target']!=program_json(target.native_target_json)):
        raise ResolutionError('Original ordered target differs')
    values=revalidate_model(entry.parameters,parameters)
    request['parameters']={name:_parameter(value)for name,value in values.model_dump(mode='python').items()}
    identity=hashlib.sha256(canonical_json([target.identity().digest(),entry.artifact_json,entry.context_json,request['parameters']]).encode()).hexdigest()
    core_after=None;native_context=None
    if after is not None:
        if type(after)is not ReadContinuation or after.identity_digest!=identity or after.owner_cursor is None:
            raise StaleContinuation('Original ordering/catalog/actor/arguments continuation required')
        try:
            token=_json(after.owner_cursor)
            if type(token)is not dict or set(token)!={'version','after','context'}or token['version']!='ashlar.ordered-continuation/0.1':
                raise ValueError('original closed owner cursor required')
            native_context=bytes.fromhex(token['context'])
            if not 1<=len(native_context)<=65536 or len(after.values)!=len(program['page']['sortSlots']):
                raise ValueError('original full cursor state required')
            states=[_transport(value,slot['carrier'])for value,slot in zip(after.values,program['page']['sortSlots'],strict=True)]
            if token['after']!={'sortValues':states,'key':[states[i]for i in program['page']['keySlots']]}:
                raise ValueError('public sort tuple differs from original owner cursor')
            core_after=token['after']
        except (ValueError,TypeError,KeyError,ResolutionError)as error:
            raise StaleContinuation('Complete original state/value cursor correspondence required')from error
    after_json=canonical_json(core_after)
    budget=target._budget(timeout);until=monotonic()+budget.total_seconds();original=entry.originals()
    with preserve_body_failure(target.deadline(budget)):
        checked=program_json(target.compiler.check_engine_page_json(original,canonical_json(request),after_json))
        if checked.get('status')!='compiled':raise ResolutionError('Original ordered page composition refused')
        artifact,metadata=checked['artifact'],checked['page'];size=program['pageSize']
        if metadata['visibleOutputs']!=len(entry.result.model_fields)or metadata['pageSize']!=size:
            raise ResolutionError('Original visible page signature/bound differs')
        policy=_Policy(target.policy,target.compiler,original,entry.result,until,page_after_json=after_json,
                       page_metadata=metadata,total_outputs=len(artifact['columns']))
        try:
            native=read_weft(target.executor,target.backend,target.pins,target.vector,policy,request=request,artifact=artifact,
                             context=target.context,supported_profiles=target.supported_profiles,supported_revisions=target.supported_revisions,
                             continuation_context=native_context)
        except PageContextRefused as error:
            if after is not None:raise StaleContinuation('Original page publication/actor context changed')from error
            raise
        policy._time()
        if (type(native)is not PageReadRows or policy.validated is None or policy.ordered_rows is None
                or len(native.rows)!=len(policy.validated)or len(native.rows)>size+1):
            raise ResolutionError('Complete original bounded ordered page required')
        next_after=None
        if len(native.rows)>size:
            values,cursor=policy.ordered_rows[size-1]
            owner=canonical_json({'version':'ashlar.ordered-continuation/0.1','after':cursor,'context':native.owner_context.hex()}).encode()
            next_after=ReadContinuation(values,identity,owner)
        result=ReadPage(tuple(policy.validated[:size]),next_after)
    if monotonic()>=until:raise TimeoutError('Ashlar deadline expired during final page release')
    return result
