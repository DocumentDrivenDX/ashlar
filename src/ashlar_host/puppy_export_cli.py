# SPDX-License-Identifier: MIT OR Apache-2.0
"""Public bounded PuppyGraph corpus over an admitted immutable release pair."""
from collections import Counter
from dataclasses import replace
import hashlib,json
from pathlib import Path
from .evolution_cli import InvocationFile
from .local_graph_export import LocalGraphExportSelection,open_local_graph_export
from .lifecycle import owned_context,finish
from .puppy_carrier import prepare_private_carrier
from .puppy_finite_local import ImmutableExportPreparationPolicy,PuppyFiniteLoopbackConfig,PuppyGremlinLoopbackConfig,open_puppy_export_local
from .puppy_native import PuppyNativeReleaseAdapter
from .puppy_release import PuppyReleaseOwner,PuppyReleaseHandle
from .truss_preparation import json_bytes
from .truss_example import private_file

PROFILE='ashlar-local-immutable-puppy-corpus/0.1'
GREMLIN_PROFILE='ashlar-local-immutable-puppy-corpus/0.2'

def parse_puppy_export_configuration(raw):
    from .private_graph_custody import decode_custody_bytes,require_custody_digest
    value=decode_custody_bytes(raw)
    keys={'profile','release_path','custody_path','release_sha256','custody_sha256','output','docker','docker_host','image','name','username','password','http_port','bolt_port','deadline_seconds'}
    if type(value)is not dict:raise ValueError('Closed explicit immutable PuppyGraph configuration required')
    gremlin=value.get('profile')==GREMLIN_PROFILE
    if set(value)!=keys|({'language','gremlin_port'}if gremlin else set())or value.get('profile')not in (PROFILE,GREMLIN_PROFILE)or(gremlin and value['language']!='Gremlin'):
        raise ValueError('Closed explicit immutable PuppyGraph configuration required')
    def path(name):
        v=value[name]
        if type(v)is not str:raise ValueError('Explicit canonical path required')
        p=Path(v)
        if not p.is_absolute()or p.resolve()!=p:raise ValueError('Explicit canonical path required')
        return p
    for name in ('release_sha256','custody_sha256'):require_custody_digest(value[name])
    selection=LocalGraphExportSelection(path('release_path'),path('custody_path'),value['release_sha256'],value['custody_sha256'])
    root=path('output')
    if root.exists()or not root.parent.is_dir()or any(root==p or root in p.parents or p in root.parents for p in (selection.release_path,selection.custody_path)):raise ValueError('Fresh separate output required')
    # Validate runtime selection without requiring the not-yet-created prepared directory.
    # Multicall Docker distributions require the operator-selected argv0 name;
    # resolving their executable symlink can select an unsupported command.
    docker=value['docker']
    if type(docker)is not str or not Path(docker).is_absolute()or not Path(docker).is_file():raise ValueError('Explicit existing Docker CLI executable required')
    cls=PuppyGremlinLoopbackConfig if gremlin else PuppyFiniteLoopbackConfig
    config=cls(Path(docker),value['docker_host'],value['image'],value['name'],root.parent,value['username'],value['password'],value['http_port'],value['bolt_port'],value['deadline_seconds'],**({'gremlin_port':value['gremlin_port']}if gremlin else{}))
    config.__post_init__()
    return selection,root,config

def immutable_corpus(release):
    """Independent complete original edge occurrence oracle, including isolates."""
    value=json.loads(release.payload);handle=PuppyReleaseHandle(release);nl=handle.node_label;el=handle.edge_label
    nodes=value['nodes'];edges=value['edges'];keys=sorted(n['graph_id']for n in nodes)
    if not keys or len(keys)>100 or len(edges)>100:raise ValueError('Explicit small nonempty graph corpus required')
    node=lambda k:{'key':k};one=[{'src':e['src'],'edge':e['graph_id'],'dst':e['dst']}for e in edges]
    paths=[{'src':a['src'],'edge1':a['graph_id'],'middle':a['dst'],'edge2':b['graph_id'],'dst':b['dst']}for a in edges for b in edges if a['dst']==b['src']]
    degrees=[{'key':k,'degree':sum(e['src']==k for e in edges)}for k in keys]
    counts=[{'key':k,'paths':sum(p['src']==k for p in paths),'targets':len({p['dst']for p in paths if p['src']==k})}for k in keys]
    literal=json.dumps(keys[0]);n='(n:'+nl+')';e='[e:'+el+']';a='[a:'+el+']';b='[b:'+el+']'
    return (
      ('nodes','MATCH '+n+' RETURN n.carrier_key AS key',[node(k)for k in keys]),
      ('edges','MATCH (s)-'+e+'->(t) RETURN s.carrier_key AS src,e.carrier_key AS edge,t.carrier_key AS dst',one),
      ('singleton','MATCH '+n+' WHERE n.carrier_key='+literal+' RETURN n.carrier_key AS key',[node(keys[0])]),
      ('bounded-list','MATCH '+n+' RETURN n.carrier_key AS key ORDER BY key LIMIT 1',[node(keys[0])]),
      ('degrees','MATCH '+n+' OPTIONAL MATCH (n)-'+e+'->(t) RETURN n.carrier_key AS key,count(e) AS degree',degrees),
      ('two-hop','MATCH (s)-'+a+'->(m) MATCH (m)-'+b+'->(t) RETURN s.carrier_key AS src,a.carrier_key AS edge1,m.carrier_key AS middle,b.carrier_key AS edge2,t.carrier_key AS dst',paths),
      ('two-hop-counts','MATCH '+n+' OPTIONAL MATCH (n)-'+a+'->(m) OPTIONAL MATCH (m)-'+b+'->(t) RETURN n.carrier_key AS key,count(b) AS paths,count(DISTINCT t.carrier_key) AS targets',counts))


def immutable_gremlin_corpus(release):
    """Same complete source oracle; only terminal DISTINCT uses deduplication."""
    cases=immutable_corpus(release);handle=PuppyReleaseHandle(release)
    nl=json.dumps(handle.node_label);el=json.dumps(handle.edge_label)
    key=json.dumps(cases[2][2][0]['key']);vertices='g.V().hasLabel('+nl+')'
    two='__.outE('+el+').inV().outE('+el+').inV()'
    scripts=(
      vertices+".project('key').by('carrier_key')",
      'g.E().hasLabel('+el+").project('src','edge','dst').by(__.outV().values('carrier_key')).by('carrier_key').by(__.inV().values('carrier_key'))",
      vertices+".has('carrier_key',"+key+").project('key').by('carrier_key')",
      vertices+".order().by('carrier_key').limit(1).project('key').by('carrier_key')",
      vertices+".project('key','degree').by('carrier_key').by(__.outE("+el+').count())',
      vertices+".as('src').outE("+el+").as('edge1').inV().as('middle').outE("+el+").as('edge2').inV().as('dst').select('src','edge1','middle','edge2','dst').by('carrier_key')",
      vertices+".project('key','paths','targets').by('carrier_key').by("+two+'.count()).by('+two+".values('carrier_key').dedup().count())")
    return tuple((name,script,expected)for(name,_,expected),script in zip(cases,scripts))

def run_puppy_export_command(configuration):
    selected=InvocationFile.read(configuration,65536);selection,root,runtime=parse_puppy_export_configuration(selected.raw)
    result=None;primary=None
    try:
        # Mixed/altered custody refuses before preparing output or Docker effects.
        with owned_context(open_local_graph_export(selection))as export:
            gremlin=type(runtime)is PuppyGremlinLoopbackConfig
            if gremlin:
                from .puppy_gremlin_transport import require_gremlin_runtime
                require_gremlin_runtime()
            corpus=immutable_gremlin_corpus(export.release)if gremlin else immutable_corpus(export.release);root.mkdir(mode=0o700)
            policy=ImmutableExportPreparationPolicy(export)
            with owned_context(policy.writer(export.context)):
                receipt=prepare_private_carrier(export,root/'prepared',policy=policy,context=export.context)
            receipt_path=private_file(root/'prepared/receipt.json',receipt);receipt_raw=receipt_path.read_bytes()
            runtime=replace(runtime,prepared=root/'prepared')
            def failure(value):
                private_file(root/'failure.json',{k:(v.hex()if type(v)is bytes else [(n,b.hex())for n,b in v]if k=='model_observations'else v)for k,v in value.items()})
            with owned_context(open_puppy_export_local(runtime,export=export,receipt=receipt,failure_observer=failure))as native:
                adapter=PuppyNativeReleaseAdapter(prepared={export.release.sha256:(native.database,root/'prepared/model.json',receipt_path,hashlib.sha256(receipt_raw).hexdigest())},prior_model=json_bytes({'catalog':[],'node':[],'edge':[]},1048576),http=native.schema_http,runtime=native.native_runtime,cypher=lambda q,c:native.query('Cypher',q,c),evidence=root/'native')
                owner=PuppyReleaseOwner(adapter,native);handle=owner.refresh(export.release,context=export.context)
                gremlin_owner=None;gremlin_handle=None
                if gremlin:
                    from .puppy_gremlin import GremlinReleaseProjection
                    original_model=(root/'prepared/model.json').read_bytes()
                    gremlin_adapter=PuppyNativeReleaseAdapter(prepared={export.release.sha256:(native.database,root/'prepared/model.json',receipt_path,hashlib.sha256(receipt_raw).hexdigest())},prior_model=original_model,http=native.schema_http,runtime=native.native_runtime,projection=GremlinReleaseProjection(lambda q,c:native.query_gremlin(q,c)),evidence=root/'gremlin-native')
                    gremlin_owner=PuppyReleaseOwner(gremlin_adapter,native);gremlin_handle=gremlin_owner.refresh(export.release,context=export.context)
                results=[]
                with owned_context(native.writer(export.context)):
                    for name,sql,expected in corpus:
                        fields=('degree',)if name=='degrees'else('paths','targets')if name=='two-hop-counts'else()
                        rows=native.query_gremlin(sql,export.context,count_fields=fields)if gremlin else native.query('Cypher',sql,export.context)
                        if Counter(json.dumps(r,sort_keys=True,separators=(',',':'))for r in rows)!=Counter(json.dumps(r,sort_keys=True,separators=(',',':'))for r in expected):raise ValueError('Complete original PuppyGraph corpus differs: '+name)
                        result_item={'case':name,'query':sql,'rows':rows,'oracle':expected};private_file(root/(name+'.json'),result_item);results.append(result_item)
                owner.read(handle,context=export.context)
                if gremlin:gremlin_owner.read(gremlin_handle,context=export.context)
                if gremlin:private_file(root/'gremlin-projections.json',[json.loads(raw)for name,raw in native.observations if name=='gremlin-projection'])
                private_file(root/'readiness.json',[json.loads(raw)for name,raw in native.observations if name=='backend-readiness'])
                result={'profile':GREMLIN_PROFILE if gremlin else PROFILE,'release_sha256':export.release.sha256,'custody_sha256':export.custody_sha256,'runtime_image':runtime.image,'nodes':len(json.loads(export.release.payload)['nodes']),'edges':len(json.loads(export.release.payload)['edges']),'cases':results,'qualification':'Actual bounded local PuppyGraph complete canonical carrier/identity and seven original immutable-release '+('Gremlin'if gremlin else'Cypher')+' corpus comparisons. Historical source metadata only; no source access, mutation, ACK, numeric property filtering, cloud or production qualification.'}
    except BaseException as error:primary=error
    finish(primary,[lambda:selected.renew(65536)])
    if gremlin:result['language']='Gremlin'
    private_file(root/'result.json',result);return result
