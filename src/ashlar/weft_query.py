# SPDX-License-Identifier: MIT OR Apache-2.0
"""Buffered Weft execution within one resolver/pin interval; no permissive policies."""
import copy
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from types import AsyncGeneratorType, CoroutineType, GeneratorType

from .execution_scope import preserve_body_failure as _preserve_body_failure
from .pins import PinError
from .publication import ResolutionError, resolve_publication
from .weft_decode import ExactScalar
from .weft_related_keys_decode import DecodedRelatedKeys
from .sdk_presence import SDK_BACKENDS,CURRENT_SDK_PAGES_BACKEND,DecodedPresence

OBLIGATIONS={'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity','ashlar.nativeProfile'}
PAGE_BACKENDS=(
 {'backendId':'ashlar.databricks','backendVersion':'0.1.0-local-spark4-pages-candidate','interfaceVersion':'weft-backend/0.2.2','targetProfile':'spark4.0.1-delta4.0.0-candidate'},
 {'backendId':'ashlar.databricks','backendVersion':'0.1.0-managed-20261016-pages-candidate','interfaceVersion':'weft-backend/0.2.2','targetProfile':'dbsql2026.39-build-20261016-candidate'},
 CURRENT_SDK_PAGES_BACKEND,
)
MANAGED_CURRENT_BACKEND03={
 'backendId':'ashlar.databricks.managed-uc-spark',
 'backendVersion':'0.1.0-managed-current-core-candidate',
 'interfaceVersion':'weft-backend/0.3.0',
 'targetProfile':'ashlar-managed-current-core/0.1',
}
MANAGED_CURRENT_BACKEND02={
 'backendId':'ashlar.databricks.managed-uc-spark',
 'backendVersion':'0.1.0-managed-current-core-pages-candidate',
 'interfaceVersion':'weft-backend/0.2.7',
 'targetProfile':'ashlar-managed-current-core/0.1',
}
MANAGED_CURRENT_XXHASH64_BACKEND03={
 'backendId':'ashlar.databricks.managed-uc-spark-xxhash64',
 'backendVersion':'0.2.0-managed-current-core-xxhash64-candidate',
 'interfaceVersion':'weft-backend/0.3.0',
 'targetProfile':'ashlar-managed-current-core/0.2-xxhash64',
}
MANAGED_CURRENT_XXHASH64_BACKEND02={
 'backendId':'ashlar.databricks.managed-uc-spark-xxhash64',
 'backendVersion':'0.2.0-managed-current-core-xxhash64-pages-candidate',
 'interfaceVersion':'weft-backend/0.2.7',
 'targetProfile':'ashlar-managed-current-core/0.2-xxhash64',
}
class PageContextRefused(ResolutionError):
    """The original publication/current admission failed before page data SQL."""

@dataclass(frozen=True)
class PageReadRows:
    rows: tuple
    owner_context: bytes=field(repr=False)
    def __post_init__(self):
        if type(self.rows)is not tuple or type(self.owner_context)is not bytes or not 1<=len(self.owner_context)<=65536:
            raise ResolutionError('Complete immutable native page rows/context required')


class _SnapshotBudget:
    __slots__ = ('nodes', 'copied_bytes')
    def __init__(self):
        self.nodes = 100000
        self.copied_bytes = 8 * 1024 * 1024


def _completed_decode(value, *, _budget=None):
    """Bounded detached data snapshot; never retain arbitrary executable carriers.

    Scalar domain interpretation remains the qualified decoder's responsibility.
    Only exact built-in containers/scalars and named immutable native scalars are
    supported. Opaque objects, cycles and deferred values refuse while held.
    """
    budget = _SnapshotBudget() if _budget is None else _budget
    active = set()
    deferred = {}
    invalid = False
    def visit(item, depth):
        nonlocal invalid
        budget.nodes -= 1
        if budget.nodes < 0 or depth > 64:
            invalid = True
            return None
        kind = type(item)
        if kind in (GeneratorType, CoroutineType):
            deferred[id(item)] = item
            invalid = True
            return None
        if kind is AsyncGeneratorType:
            invalid = True
            return None
        if kind is ExactScalar:
            # Only the original decoder class; retain its original token while
            # held. Semantic/source admission remains the owner's obligation.
            try:
                token_bytes=len(item.original.encode('utf-8')) if type(item.original) is str else 0
            except UnicodeError:
                invalid = True
                return None
            if (type(item.value) not in (str, bool, int, Decimal)
                    or type(item.original) not in (str, bool)
                    or type(item.value) is Decimal and not item.value.is_finite()
                    or token_bytes > budget.copied_bytes):
                invalid = True
                return None
            if type(item.original) is str:
                budget.copied_bytes -= token_bytes
            return ExactScalar(visit(item.value, depth + 1), visit(item.original, depth + 1))
        if kind is DecodedPresence:
            if (item.state not in ('present','null','missing') or type(item.original)is not str
                    or item.state!='present' and item.value is not None
                    or item.state=='present' and type(item.value)not in (str,bool,int,Decimal)):
                invalid=True
                return None
            token=visit(item.original,depth+1)
            value=visit(item.value,depth+1)
            return DecodedPresence(item.state,value,token)
        if kind is DecodedRelatedKeys:
            # Only the original named decoder carrier crosses this boundary.
            # This snapshot grants no bag/authorization authority of its own.
            if (type(item.original) is not bytes or type(item.items) is not tuple
                    or type(item.truncated) is not bool or len(item.items) > 1000
                    or len(item.original) > budget.copied_bytes
                    or any(type(row) is not tuple or not 1 <= len(row) <= 1024
                           or any(type(component) is not str for component in row)
                           for row in item.items)):
                invalid = True
                return None
            budget.copied_bytes -= len(item.original)
            copied = visit(item.items, depth + 1)
            return DecodedRelatedKeys(item.original, copied, item.truncated)
        if kind in (type(None), bool, int, float, str, bytes, Decimal, date, timedelta):
            return item
        if kind in (datetime, time):
            if item.tzinfo is not None and item.tzinfo is not timezone.utc:
                invalid = True
                return None
            return item
        if kind is bytearray:
            if len(item) > budget.copied_bytes:
                invalid = True
                return None
            budget.copied_bytes -= len(item)
            return bytearray(item)
        if kind not in (list, tuple, dict) or id(item) in active:
            invalid = True
            return None
        active.add(id(item))
        try:
            if kind is dict:
                result = {}
                for key, child in item.items():
                    copied_key = visit(key, depth + 1)
                    copied_child = visit(child, depth + 1)
                    if type(key) is not str:
                        invalid = True
                    elif copied_key is not None:
                        result[copied_key] = copied_child
                    if budget.nodes < 0:
                        break
                return result
            result = []
            for child in item:
                result.append(visit(child, depth + 1))
                if budget.nodes < 0:
                    break
            return tuple(result) if kind is tuple else result
        finally:
            active.remove(id(item))
    result = visit(value, 0)
    closing_failure = None
    for item in deferred.values():
        try:
            item.close()
        except BaseException as error:
            if closing_failure is None:
                closing_failure = error
    if invalid:
        raise ResolutionError('Completed bounded data-only Weft decoding required') from closing_failure
    return result


def read_weft(executor,backend,pins,vector,policy,*,request,artifact,context,supported_profiles,supported_revisions,continuation_context=None):
    """Policy owns exact compiler custody, native profile, source visibility and decoding.

    No policy callback has a successful default. Unknown obligations refuse here;
    wider integrity families need explicit versioned handlers, not ignored checks.
    Returned rows remain buffered through final context closure.
    """
    request=copy.deepcopy(request);artifact=copy.deepcopy(artifact)
    current_page_backend=CURRENT_SDK_PAGES_BACKEND
    managed_request_target=request.get('target')if type(request)is dict else None
    managed_request_version=(managed_request_target.get('backendVersion')
        if type(managed_request_target)is dict else None)
    managed_expected=next((candidate for candidate in (
        MANAGED_CURRENT_BACKEND02,MANAGED_CURRENT_BACKEND03,
        MANAGED_CURRENT_XXHASH64_BACKEND02,MANAGED_CURRENT_XXHASH64_BACKEND03)
        if type(managed_request_target)is dict
        and managed_request_target.get('backendId')==candidate['backendId']
        and managed_request_target.get('backendVersion')==candidate['backendVersion']
        and managed_request_target.get('targetProfile')==candidate['targetProfile']),None)
    managed_request=(managed_expected is not None and type(managed_request_target)is dict
        and {key:managed_request_target.get(key) for key in ('backendId','backendVersion','targetProfile')}==
            {key:managed_expected[key] for key in ('backendId','backendVersion','targetProfile')})
    managed_artifact=(managed_expected is not None and artifact.get('backend')==managed_expected)
    if (managed_request or managed_artifact) and (not managed_request or not managed_artifact):
        raise ResolutionError('Exact managed current compiler target and artifact required')
    managed_backends=(MANAGED_CURRENT_BACKEND03,MANAGED_CURRENT_BACKEND02,
                      MANAGED_CURRENT_XXHASH64_BACKEND03,MANAGED_CURRENT_XXHASH64_BACKEND02)
    if artifact.get('backend') in managed_backends and not managed_request:
        raise ResolutionError('Managed current artifact must match its exact request target')
    request_target=request.get('target')if type(request)is dict else None
    current_request=(type(request_target)is dict and
        request_target.get('backendId')==current_page_backend['backendId'] and
        request_target.get('backendVersion')==current_page_backend['backendVersion'] and
        request_target.get('targetProfile')==current_page_backend['targetProfile'])
    current_artifact=artifact.get('backend')==current_page_backend
    if ((current_request or current_artifact) and
            (not current_request or not current_artifact or artifact.get('interfaceVersion')!='weft-compile/0.2.7')):
        raise ResolutionError('Exact current native Weft page target required')
    if artifact.get('status')!='compiled' or artifact.get('backend') not in SDK_BACKENDS+PAGE_BACKENDS+(MANAGED_CURRENT_BACKEND03,MANAGED_CURRENT_BACKEND02,MANAGED_CURRENT_XXHASH64_BACKEND03,MANAGED_CURRENT_XXHASH64_BACKEND02,{'backendId':'ashlar.databricks','backendVersion':'0.1.0-local-spark4-fields-candidate','interfaceVersion':'weft-backend/0.2.0','targetProfile':'spark4.0.1-delta4.0.0-fields-candidate'}, {'backendId':'ashlar.databricks','backendVersion':'0.1.0-qualified','interfaceVersion':'weft-backend/0.2.0','targetProfile':'dbsql2026.39-qualified'}, {'backendId':'ashlar.databricks','backendVersion':'0.1.0-local-spark4-candidate','interfaceVersion':'weft-backend/0.2.0','targetProfile':'spark4.0.1-delta4.0.0-candidate'}, {'backendId':'ashlar.databricks','backendVersion':'0.1.0-managed-20261016-candidate','interfaceVersion':'weft-backend/0.2.0','targetProfile':'dbsql2026.39-build-20261016-candidate'}):
        raise ResolutionError('Explicit selected native Weft artifact required')
    plan=artifact.get('logicalPlan',{})
    ordered=type(plan)is dict and plan.get('version')=='weft-ir/engine-ordered-page/0.1'
    if ordered:plan=plan.get('base',{})
    ordered_read=plan.get('readOrder')is not None
    key_integrity=plan.get('pageKey')is not None
    page=key_integrity and not ordered_read
    source_key=type(plan.get('navigation'))is dict and plan['navigation'].get('sourceSelectedKey')is not None
    if ordered and artifact.get('backend')!=SDK_BACKENDS[1]:raise ResolutionError('Explicit original ordered backend required')
    if page and artifact.get('backend') not in PAGE_BACKENDS+(SDK_BACKENDS[1],MANAGED_CURRENT_BACKEND02,MANAGED_CURRENT_XXHASH64_BACKEND02):raise ResolutionError('Explicit original page backend required')
    if managed_expected in (MANAGED_CURRENT_BACKEND03,MANAGED_CURRENT_XXHASH64_BACKEND03) and page:
        raise ResolutionError('Managed Backend03 current scalar route cannot be used as an SDK page')
    if managed_expected in (MANAGED_CURRENT_BACKEND02,MANAGED_CURRENT_XXHASH64_BACKEND02) and not page:
        raise ResolutionError('Managed Backend02 current target requires its typed page artifact')
    if continuation_context is not None and (not page or type(continuation_context)is not bytes or not 1<=len(continuation_context)<=65536):raise ResolutionError('Original bounded page context required')
    page_contexts=[]
    relationship=any(item.get('id')=='ashlar.candidate.relationshipIntegrity' for item in artifact.get('obligations',[]) if type(item)is dict)
    required=OBLIGATIONS|({'ashlar.candidate.orderedPage'}if ordered else set())|({'ashlar.candidate.keyIntegrity'}if key_integrity else set())|({'ashlar.candidate.orderedRead'}if ordered_read else set())|({'ashlar.candidate.sourceKeyIntegrity'}if source_key else set())|({'ashlar.candidate.relationshipIntegrity'}if relationship else set())
    obligations={}
    for item in artifact.get('obligations',[]):
        if set(item)!= {'id','owner','parameters','failureCode'} or item['id'] not in required or item['id'] in obligations or item['owner']!='host':
            raise ResolutionError('Unknown or duplicate Weft host obligation')
        obligations[item['id']]=item
    if set(obligations)!=required:raise ResolutionError('Complete supported Weft obligation inventory required')
    guards=obligations['ashlar.candidate.scalarIntegrity']['parameters']
    expected={'noPartialPublication':True,'parameters':'same emitted ordered slots; values never interpolated','phase':'before-user-query','samePublicationRequired':True,'success':'one exact STRING count equal to 0 per check'}
    if set(guards)!=set(expected)|{'checks'} or any(guards[k]!=v for k,v in expected.items()) or not isinstance(guards['checks'],list):
        raise ResolutionError('Unknown integrity execution meaning')
    checks=[];record_checks=[]
    for check in guards['checks']:
        if type(check) is not dict or not isinstance(check.get('sql'),str) or not check['sql']:
            raise ResolutionError('Unknown integrity check meaning')
        if set(check)=={'sql','failureCode','field','record'} and check['failureCode']=='WFT-NUMERIC-DOMAIN':
            pass
        elif set(check)=={'sql','failureCode','record'} and check['failureCode']=='WFT-BINDING':
            identity=check['record']
            if type(identity) is not dict or set(identity)!= {'documentId','revision','module','element'} or any(type(v) is not str or not v for v in identity.values()):
                raise ResolutionError('Exact record integrity identity required')
            record_checks.append(check)
        else:raise ResolutionError('Unknown integrity check meaning')
        checks.append(check['sql'])
    relationship_checks=[]
    relationship_guard=None
    if relationship:
        obligation=obligations['ashlar.candidate.relationshipIntegrity']
        relationship_guard=obligation['parameters']
        expected_relationship={
            'lifecycle':'host verifies the authored lifecycle and projection coverage against original publication',
            'multiplicity':'parallel edges retained; min/max checked in authored orientation',
            'phase':'before-user-query',
            'policy':'complete authorized source and target inputs; inverse traversal cannot broaden authority',
            'samePublicationRequired':True,
            'success':'one exact STRING count equal to 0 per check',
        }
        if (obligation['failureCode']!='WFT-BINDING' or type(relationship_guard)is not dict
                or set(relationship_guard)!=set(expected_relationship)|{'checks'}
                or any(relationship_guard[k]!=v for k,v in expected_relationship.items())
                or type(relationship_guard['checks'])is not list
                or not 1<=len(relationship_guard['checks'])<=4096):
            raise ResolutionError('Complete original relationship integrity meaning required')
        for check in relationship_guard['checks']:
            if (type(check)is not dict or set(check)!={'sql','failureCode','relationship'}
                    or check['failureCode']!='WFT-BINDING' or type(check['sql'])is not str or not check['sql']
                    or type(check['relationship'])is not dict
                    or set(check['relationship'])!={'documentId','revision','module','relationship'}
                    or any(type(v)is not str or not v for v in check['relationship'].values())):
                raise ResolutionError('Original complete relationship check required')
            relationship_checks.append(check['sql'])
    page_checks=[]
    if key_integrity:
        key_guard=obligations['ashlar.candidate.keyIntegrity']
        guard=key_guard['parameters']
        expected_page={'phase':'before-user-query','success':'one exact STRING count equal to 0','continuation':'same immutable publication, original binding and effective policy across pages; ORDER BY alone proves no continuity'}
        key=plan['pageKey']
        if (key_guard['failureCode']!='WFT-BINDING' or type(guard)is not dict or set(guard)!=set(expected_page)|{'key','checks'}
            or any(guard.get(name)!=value for name,value in expected_page.items()) or guard['key']!=key
            or type(key)is not dict or set(key)!={'id','fields','types'} or type(key['id'])is not str or not key['id']
            or type(key['fields'])is not list or not 1<=len(key['fields'])<=1024 or type(key['types'])is not list or len(key['types'])!=len(key['fields'])
            or type(guard['checks'])is not list or len(guard['checks'])!=1):
            raise ResolutionError('Complete original page Key integrity meaning required')
        for check in guard['checks']:
            if type(check)is not dict or set(check)!={'sql','failureCode'} or check['failureCode']!='WFT-BINDING' or type(check['sql'])is not str or not check['sql']:
                raise ResolutionError('Original complete page Key check required')
            page_checks.append(check['sql'])
    ordered_read_guard=None;source_key_guard=None
    if ordered_read:
        ordered_read_guard=obligations['ashlar.candidate.orderedRead']['parameters']
        expected_order={'version':'ashlar.ordered-read/0.1','selectedKey':plan['readOrder']['selectedKey'],
            'sortSlots':plan['readOrder']['sortSlots'],'literalLimit':plan.get('limit'),
            'authority':'original selected Key integrity and every ordering Field under the same immutable publication'}
        if ordered_read_guard!=expected_order or plan['readOrder']['selectedKey']!=plan.get('pageKey'):
            raise ResolutionError('Original ordered read Key/order correspondence required')
    if source_key:
        source_key_guard=obligations['ashlar.candidate.sourceKeyIntegrity']['parameters']
        navigation=plan['navigation'];source=navigation['source']
        expected_source={'version':'ashlar.navigation-source-key/0.1','phase':'before-user-query',
          'sourceRecord':source['source']['record'],'selectedKey':navigation['sourceSelectedKey'],
          'key':source['pageKey'],'success':'one exact STRING count equal to 0',
          'samePublicationRequired':True,'authority':'complete authorized original source Record and hidden ordering Fields'}
        if (type(source_key_guard)is not dict or set(source_key_guard)!=set(expected_source)|{'checks'}
                or any(source_key_guard[k]!=v for k,v in expected_source.items())
                or type(source_key_guard['checks'])is not list or len(source_key_guard['checks'])!=1):
            raise ResolutionError('Complete original source Key obligation required')
        for check in source_key_guard['checks']:
            if type(check)is not dict or set(check)!={'sql','failureCode'} or check['failureCode']!='WFT-BINDING' or type(check['sql'])is not str or not check['sql']:
                raise ResolutionError('Original source Key check required')
            page_checks.append(check['sql'])
    ordered_guard=None
    if ordered:
        from .sdk_ordered_page import admit_metadata
        ordered_guard=obligations['ashlar.candidate.orderedPage']['parameters']
        admit_metadata(artifact,ordered_guard)
    parameters={}
    for ordinal,slot in enumerate(artifact.get('parameters',[]),1):
        if type(slot.get('position')) is not int or slot['position']!=ordinal or type(slot.get('value')) is not str:
            raise ResolutionError('Complete ordered exact Weft parameter slots required')
        parameters['p'+str(ordinal)]=slot['value']
    columns=artifact.get('columns')
    if not isinstance(columns,list) or not columns or any(c.get('position')!=i for i,c in enumerate(columns,1)):
        raise ResolutionError('Complete ordered Weft result descriptors required')
    publication=obligations['ashlar.candidate.publication']['parameters']['publication']
    holder=getattr(pins,'hold_page' if page else 'hold',None)
    if not callable(holder):raise PageContextRefused('Original native page pin provider required')
    admitted=False
    try:
        with _preserve_body_failure(holder(vector,context=context)):
            resolved=resolve_publication(backend,publication['id'],{t:p[0] for t,p in vector.targets.items()},context=context,supported_profiles=supported_profiles,supported_revisions=supported_revisions)
            if any(resolved.snapshots[t].version!=pair[1] for t,pair in vector.targets.items()):
                raise ResolutionError('Weft read differs from complete held publication')
            def admit():
                for call in [lambda:policy.bind_descriptor(resolved.descriptor,vector,context),
                             lambda:policy.admit_artifact(request,artifact,resolved.descriptor,context),
                             lambda:policy.verify_native_profile(obligations['ashlar.nativeProfile']['parameters'],context),
                             lambda:policy.authorize_query(request,resolved.descriptor,context)]:
                    if call() is not None:raise ResolutionError('Incomplete Weft host admission')
                for check in record_checks:
                    callback=getattr(policy,'admit_record_integrity',None)
                    if not callable(callback) or callback(check,artifact,context) is not None:
                        raise ResolutionError('Explicit record integrity admission required')
                if relationship:
                    callback=getattr(policy,'admit_relationship_integrity',None)
                    if not callable(callback) or callback(relationship_guard,artifact,context) is not None:
                        raise ResolutionError('Explicit original relationship and full-input authority admission required')
                for guard_value,method in [(ordered_read_guard,'admit_ordered_read'),(source_key_guard,'admit_source_key_integrity')]:
                    if guard_value is not None:
                        callback=getattr(policy,method,None)
                        if not callable(callback)or callback(guard_value,artifact,context)is not None:
                            raise ResolutionError('Explicit original ordering Key authority required')
                if ordered:
                    callback=getattr(policy,'admit_ordered_page',None)
                    if not callable(callback)or callback(ordered_guard,artifact,context)is not None:
                        raise ResolutionError('Explicit original ordered hidden-field authority required')
                if page:
                    callback=getattr(policy,'admit_page_integrity',None)
                    if not callable(callback) or callback(guard,artifact,context) is not None:
                        raise ResolutionError('Explicit original page Key admission required')
                    callback=getattr(policy,'page_context',None)
                    if not callable(callback):raise ResolutionError('Original current page context provider required')
                    token=_completed_decode(callback(resolved.descriptor,vector,context))
                    if type(token)is not bytes or not 1<=len(token)<=65536:raise ResolutionError('Bounded immutable original page context required')
                    if not page_contexts:
                        if continuation_context is not None and continuation_context!=token:raise PageContextRefused('Original page caller or policy context changed')
                        page_contexts.append(token)
                    elif token!=page_contexts[0]:raise ResolutionError('Original page context changed before release')
            admit()
            admitted=True
            for sql in checks+relationship_checks+page_checks:
                result=executor.query(sql,parameters)
                if len(result.columns)!=1 or result.columns[0][1]!='STRING' or list(result.rows)!=[{result.columns[0][0]:'0'}]:
                    raise ResolutionError('Weft integrity check failed')
            result=executor.query(artifact['sql'],parameters)
            names=[c['outputName'] for c in columns]
            if list(result.columns)!=[(name,'STRING') for name in names] or len(set(names))!=len(names):
                raise ResolutionError('Exact complete Weft result carriers required')
            buffered=[]
            decode_budget=_SnapshotBudget()
            for row in result.rows:
                if set(row)!=set(names) or any(value is not None and type(value) is not str for value in row.values()):
                    raise ResolutionError('Exact Weft row inventory required')
                if any(row[c['outputName']] is None and c.get('nullable') is not True for c in columns):
                    raise ResolutionError('Unexpected null Weft carrier')
                buffered.append(tuple(_completed_decode(policy.decode_result(column,row[column['outputName']],request,context),_budget=decode_budget) for column in columns))
            if backend.validate_descriptor(resolved.descriptor,context) is not None or backend.authorize(context,publication['id'],tuple(vector.targets)) is not None:
                raise ResolutionError('Weft publication or authority expired')
            admit()
            if policy.authorize_result(tuple(buffered),resolved.descriptor,context) is not None:
                raise ResolutionError('Weft result release refused')
            # Re-resolve the original authoritative manifest and every consumed
            # UUID/version/schema after fetch and result authorization. Comparing
            # the previously loaded descriptor alone misses alias replacement.
            closing=resolve_publication(backend,publication['id'],{table:pair[0]for table,pair in vector.targets.items()},context=context,supported_profiles=supported_profiles,supported_revisions=supported_revisions)
            if closing!=resolved:raise ResolutionError('Original complete publication changed before result release')
            admit()
            result=_completed_decode(tuple(buffered),_budget=decode_budget)
    except Exception as error:
        if page and not admitted and (isinstance(error,(ResolutionError,PinError,PermissionError)) or getattr(error,'sqlstate',None)=='55000'):
            raise PageContextRefused('Original publication/current page admission refused')from error
        raise

    return PageReadRows(result,page_contexts[0])if page else result
