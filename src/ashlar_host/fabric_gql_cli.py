# SPDX-License-Identifier: MIT OR Apache-2.0
"""Installed GQL command using an explicitly selected trusted operator lifecycle."""
from dataclasses import dataclass
import base64,hashlib,json,re
from types import ModuleType,MappingProxyType
from .evolution_cli import InvocationFile,MAX_PROVIDER
from .fabric_gql import FabricGraphBinding,execute_gql
from .lifecycle import finish,owned_context

PROFILE='ashlar-fabric-gql-invocation/0.1'
LIMITS={'maximum_response_bytes':16777216,'maximum_total_bytes':67108864,
        'maximum_rows':10000,'maximum_requests':128,'deadline_seconds':60}

@dataclass(frozen=True)
class FabricGqlInvocation:
    binding:FabricGraphBinding
    query:str
    limits:object

@dataclass(frozen=True)
class FabricGqlSession:
    """Operator-supplied ordinary HTTP and actual release policy, never a grant."""
    binding:FabricGraphBinding
    transport:object
    policy:object
    context:object


def parse_fabric_gql_invocation(raw):
    if type(raw)is not bytes or not 0<len(raw)<=65536:raise ValueError('Bounded Fabric query configuration required')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate Fabric configuration member')
            result[key]=value
        return result
    def refuse(_):raise ValueError('Exact Fabric configuration scalars required')
    value=json.loads(raw,object_pairs_hook=pairs,parse_float=refuse,parse_constant=refuse)
    if type(value)is not dict or set(value)!={'profile','binding','query','limits'}or value['profile']!=PROFILE:
        raise ValueError('Closed Fabric GQL configuration required')
    binding=value['binding'];limits=value['limits'];query=value['query']
    if type(binding)is not dict or set(binding)!={'workspace_id','graph_model_id','release_sha256'}:
        raise ValueError('Exact Fabric graph binding required')
    selected=FabricGraphBinding(**binding)
    if type(query)is not str or not 0<len(query.encode('utf8'))<=65536:raise ValueError('Bounded original GQL query required')
    if type(limits)is not dict or set(limits)!=set(LIMITS)or any(type(limits[k])is not int or not 0<limits[k]<=cap for k,cap in LIMITS.items()):
        raise ValueError('Explicit finite Fabric GQL limits required')
    return FabricGqlInvocation(selected,query,MappingProxyType(limits))


def run_fabric_gql_command(configuration,provider_path,provider_sha256):
    if type(provider_sha256)is not str or not re.fullmatch('[a-f0-9]{64}',provider_sha256):raise ValueError('Explicit provider SHA256 required')
    config=InvocationFile.read(configuration,65536);invocation=parse_fabric_gql_invocation(config.raw)
    provider=InvocationFile.read(provider_path,MAX_PROVIDER)
    if hashlib.sha256(provider.raw).hexdigest()!=provider_sha256:raise ValueError('Selected Fabric provider bytes changed')
    def renew():finish(None,[lambda:config.renew(65536),lambda:provider.renew(MAX_PROVIDER)])
    result=None;primary=None
    try:
        module=ModuleType('ashlar_operator_fabric_'+provider_sha256);module.__file__=str(provider.path)
        exec(compile(provider.raw,str(provider.path),'exec'),module.__dict__)
        renew()
        if not callable(getattr(module,'open_fabric_gql',None)):raise ValueError('Provider must define open_fabric_gql(invocation)')
        with owned_context(module.open_fabric_gql(invocation))as session:
            if type(session)is not FabricGqlSession or type(session.binding)is not FabricGraphBinding or session.binding!=invocation.binding:
                raise ValueError('Exact selected Fabric session binding required')
            result=execute_gql(invocation.binding,invocation.query,transport=session.transport,
                policy=session.policy,context=session.context,**dict(invocation.limits))
            renew()
    except BaseException as error:primary=error
    finish(primary,[renew])
    return {'profile':'ashlar-fabric-gql-result/0.1','request_sha256':result.request_sha256,
        'columns':[{'name':name,'gqlType':kind}for name,kind in result.columns],
        'rows':[list(row)for row in result.rows],
        'responses_base64':[base64.b64encode(raw).decode('ascii')for raw in result.responses],
        'scope':'Complete supported scalar GQL read after selected operator cleanup; authority belongs to the actual publication/release policy and ordinary HTTP owner.'}
