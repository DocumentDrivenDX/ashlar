"""Indexed original-commerce one-hop consumer, experimental private host only.

This module consumes an already opened authoritative reader. It provisions no
engine and never borrows fixture ACKs. Caller must close the outer reader and
stop Spark successfully before persisting the returned provisional evidence.
"""
import hashlib,json
from ashlar.weft_distribution import compile_distribution
from ashlar.commerce_source import GRAPH_SHA
from indexed_weft_commerce import decode_compile_artifact
from local_delta_custody import encoded
from weft_relationship_plan import relationship_request,admit_relationship_artifact,decode_relationship_rows


def original_oracle(graph_bytes: bytes) -> list:
    graph=json.loads(graph_bytes);objects={o['key']:o for o in graph['objects']}
    result=[]
    for o in graph['objects']:
        if o['type']!={'document':'urn:umf:domain:commerce','module':'domain','element':'products'}:continue
        keys=[]
        for edge in graph['edges']:
            if edge['relationship']=={'document':'urn:umf:domain:commerce','module':'domain','id':'products.supplier_id'} and edge['source']==o['key']:
                target=objects[edge['target']]
                if target['type']!={'document':'urn:umf:domain:commerce','module':'domain','element':'suppliers'}:raise ValueError('Original endpoint type differs')
                keys.append([target['values']['suppliers.id']])
        keys.sort(key=lambda k:k[0].encode('utf-8'))
        result.append({'id':o['values']['products.id'],'suppliers':{'items':keys[:2],'truncated':len(keys)>2}})
    if not result:raise ValueError('Original commerce product witness required')
    return sorted(result,key=lambda r:r['id'].encode('utf-8'))


def run_indexed_relationship(opened, paths) -> dict:
    provider=opened.provider;context=opened.context
    if type(opened.graph)is not bytes or hashlib.sha256(opened.graph).hexdigest()!=GRAPH_SHA:raise ValueError('Exact original admitted commerce graph required')
    if any(not callable(getattr(provider,name,None))for name in ('interval','resolve','admit_binding','runtime','native_table_schema','sql','closed_interval_custody')):raise ValueError('Complete held provider callbacks required before acquisition')
    request=relationship_request(opened.model,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases)
    raw=(encoded(request)+'\n').encode();artifact=decode_compile_artifact(compile_distribution(paths,raw))
    # Independent second public compilation retains exact slot/check custody.
    binding=admit_relationship_artifact(request,artifact,decode_compile_artifact(compile_distribution(paths,raw)))
    expected=original_oracle(opened.graph);native=dict(opened.original_native_files)
    if opened.native_files()!=native:raise ValueError('Opening full native vector differs')
    provider.expected_binding=request['target']['bindingJson'];params={'p'+str(s['position']):s['value']for s in artifact['parameters']}
    checks=[];schemas=[];completed=False
    with provider.interval(context):
        resolved=provider.resolve(context);provider.admit_binding(binding,resolved,context);provider.runtime(context)
        # Metadata proves physical types independently of rows, including empty sources.
        for table in binding['publication']['tables']:
            if table['name'][-1] not in ('object_current','edge_current'):continue
            observed=provider.native_table_schema(table,resolved,context)
            if observed.get('table')!=table:raise ValueError('Observed schema table pin differs')
            fields=observed['schema']['fields'];native_types=dict(observed['nativeTypes'])
            required={'id':'BIGINT','source_system':'STRING','schema_revision':'STRING'}
            if table['name'][-1]=='edge_current':required.update(rel_type_id='BIGINT',source_type='BIGINT',source_id='BIGINT',target_type='BIGINT',target_id='BIGINT')
            else:required.update(type_id='BIGINT',props_json='STRING')
            byname={f['name']:f for f in fields}
            if len(native_types)!=len(observed['nativeTypes']) or set(native_types)!=set(byname):raise ValueError('Complete unambiguous observed schema types required')
            if len(byname)!=len(fields) or any(name not in byname or native_types.get(name)!=kind or type(byname[name]['nullable'])is not bool for name,kind in required.items()):raise ValueError('Exact native physical carrier schema required')
            schemas.append(observed)
        for family in ('ashlar.candidate.scalarIntegrity','ashlar.candidate.relationshipIntegrity'):
            obligation=next(o for o in artifact['obligations']if o['id']==family)
            for check in obligation['parameters']['checks']:
                rows=provider.sql(check['sql'],params)
                if rows!=[{'violations':'0'}]:raise ValueError('Complete source/relationship guard refused')
                checks.append({'family':family,'check':check,'rows':rows})
        rows=provider.sql(artifact['sql'],params);decoded=decode_relationship_rows(rows)
        if encoded(decoded)!=encoded(expected):raise ValueError('Independent original ordered occurrence bags differ')
        provider.runtime(context);closing=provider.resolve(context);provider.admit_binding(binding,closing,context)
        if dict(closing.descriptor.raw)!=dict(resolved.descriptor.raw) or closing.snapshots!=resolved.snapshots:raise ValueError('Original full publication changed')
        completed=True
    if not completed or opened.native_files()!=native:raise ValueError('Closing native interval differs')
    custody=provider.closed_interval_custody(context)
    return {'format':'ashlar-indexed-commerce-relationship/0.1','request':request,'artifact':artifact,'native_rows':rows,'decoded':decoded,'independent_original_expected':expected,'checks':checks,'schemas':schemas,'closed_interval':custody,'qualification':__doc__,'open_upstream_gap':'General parallel-edge multiplicity versus distinct associated Record semantics remains unqualified; no two-hop capability claim.'}
