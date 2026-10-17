"""Buffered Weft execution within one resolver/pin interval; no permissive policies."""
import copy
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from types import AsyncGeneratorType, CoroutineType, GeneratorType

from .execution_scope import preserve_body_failure as _preserve_body_failure
from .pins import PinError
from .publication import ResolutionError, resolve_publication
from .weft_related_keys_decode import DecodedRelatedKeys

OBLIGATIONS={'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity','ashlar.nativeProfile'}
PAGE_BACKENDS=(
 {'backendId':'ashlar.databricks','backendVersion':'0.1.0-local-spark4-pages-candidate','interfaceVersion':'weft-backend/0.2.2','targetProfile':'spark4.0.1-delta4.0.0-candidate'},
 {'backendId':'ashlar.databricks','backendVersion':'0.1.0-managed-20261016-pages-candidate','interfaceVersion':'weft-backend/0.2.2','targetProfile':'dbsql2026.39-build-20261016-candidate'},
)
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
    if artifact.get('status')!='compiled' or artifact.get('backend') not in PAGE_BACKENDS+({'backendId':'ashlar.databricks','backendVersion':'0.1.0-qualified','interfaceVersion':'weft-backend/0.2.0','targetProfile':'dbsql2026.39-qualified'}, {'backendId':'ashlar.databricks','backendVersion':'0.1.0-local-spark4-candidate','interfaceVersion':'weft-backend/0.2.0','targetProfile':'spark4.0.1-delta4.0.0-candidate'}, {'backendId':'ashlar.databricks','backendVersion':'0.1.0-managed-20261016-candidate','interfaceVersion':'weft-backend/0.2.0','targetProfile':'dbsql2026.39-build-20261016-candidate'}):
        raise ResolutionError('Explicit selected native Weft artifact required')
    page=artifact.get('logicalPlan',{}).get('pageKey') is not None
    if page and artifact.get('backend') not in PAGE_BACKENDS:raise ResolutionError('Explicit original page backend required')
    if continuation_context is not None and (not page or type(continuation_context)is not bytes or not 1<=len(continuation_context)<=65536):raise ResolutionError('Original bounded page context required')
    page_contexts=[]
    relationship=any(item.get('id')=='ashlar.candidate.relationshipIntegrity' for item in artifact.get('obligations',[]) if type(item)is dict)
    required=OBLIGATIONS|({'ashlar.candidate.keyIntegrity'}if page else set())|({'ashlar.candidate.relationshipIntegrity'}if relationship else set())
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
    if page:
        key_guard=obligations['ashlar.candidate.keyIntegrity']
        guard=key_guard['parameters']
        expected_page={'phase':'before-user-query','success':'one exact STRING count equal to 0','continuation':'same immutable publication, original binding and effective policy across pages; ORDER BY alone proves no continuity'}
        key=artifact['logicalPlan']['pageKey']
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
