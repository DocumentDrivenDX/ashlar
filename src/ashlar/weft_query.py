"""Buffered Weft execution within one resolver/pin interval; no permissive policies."""
import copy
from .publication import resolve_publication,ResolutionError

OBLIGATIONS={'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity','ashlar.nativeProfile'}

def read_weft(executor,backend,pins,vector,policy,*,request,artifact,context,supported_profiles,supported_revisions):
    """Policy owns exact compiler custody, native profile, source visibility and decoding.

    No policy callback has a successful default. Unknown obligations refuse here;
    wider integrity families need explicit versioned handlers, not ignored checks.
    Returned rows remain buffered through final context closure.
    """
    request=copy.deepcopy(request);artifact=copy.deepcopy(artifact)
    if artifact.get('status')!='compiled' or artifact.get('backend')!={'backendId':'ashlar.databricks','backendVersion':'0.1.0-qualified','interfaceVersion':'weft-backend/0.2.0','targetProfile':'dbsql2026.39-qualified'}:
        raise ResolutionError('Explicit qualified Weft artifact required')
    obligations={}
    for item in artifact.get('obligations',[]):
        if set(item)!= {'id','owner','parameters','failureCode'} or item['id'] not in OBLIGATIONS or item['id'] in obligations or item['owner']!='host':
            raise ResolutionError('Unknown or duplicate Weft host obligation')
        obligations[item['id']]=item
    if set(obligations)!=OBLIGATIONS:raise ResolutionError('Complete supported Weft obligation inventory required')
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
    parameters={}
    for ordinal,slot in enumerate(artifact.get('parameters',[]),1):
        if type(slot.get('position')) is not int or slot['position']!=ordinal or type(slot.get('value')) is not str:
            raise ResolutionError('Complete ordered exact Weft parameter slots required')
        parameters['p'+str(ordinal)]=slot['value']
    columns=artifact.get('columns')
    if not isinstance(columns,list) or not columns or any(c.get('position')!=i for i,c in enumerate(columns,1)):
        raise ResolutionError('Complete ordered Weft result descriptors required')
    publication=obligations['ashlar.candidate.publication']['parameters']['publication']
    with pins.hold(vector,context=context):
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
        admit()
        for sql in checks:
            result=executor.query(sql,parameters)
            if len(result.columns)!=1 or result.columns[0][1]!='STRING' or list(result.rows)!=[{result.columns[0][0]:'0'}]:
                raise ResolutionError('Weft integrity check failed')
        result=executor.query(artifact['sql'],parameters)
        names=[c['outputName'] for c in columns]
        if list(result.columns)!=[(name,'STRING') for name in names] or len(set(names))!=len(names):
            raise ResolutionError('Exact complete Weft result carriers required')
        buffered=[]
        for row in result.rows:
            if set(row)!=set(names) or any(value is not None and type(value) is not str for value in row.values()):
                raise ResolutionError('Exact Weft row inventory required')
            if any(row[c['outputName']] is None and c.get('nullable') is not True for c in columns):
                raise ResolutionError('Unexpected null Weft carrier')
            buffered.append(tuple(policy.decode_result(column,row[column['outputName']],request,context) for column in columns))
        if backend.validate_descriptor(resolved.descriptor,context) is not None or backend.authorize(context,publication['id'],tuple(vector.targets)) is not None:
            raise ResolutionError('Weft publication or authority expired')
        admit()
        if policy.authorize_result(tuple(buffered),resolved.descriptor,context) is not None:
            raise ResolutionError('Weft result release refused')
        result=tuple(buffered)
    return result
