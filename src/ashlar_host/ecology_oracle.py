"""Independent original ecology graph oracle; no UMF/native admission claim."""
import hashlib,json
SOURCE_SHA='299e6f3e96bbfb149e593484e1ded655378aa9adf272cdc1ffc9d8706d3244ee'
GRAPH_SHA='cd601e5eff6868def9e5ad06d53e6a067cc140eadbc29855a46bba548b78d668'
def original_oracle(model_bytes,graph_bytes):
    if hashlib.sha256(model_bytes).hexdigest()!=SOURCE_SHA or hashlib.sha256(graph_bytes).hexdigest()!=GRAPH_SHA:raise ValueError('Exact original ecology bytes required')
    graph=json.loads(graph_bytes);model=json.loads(model_bytes)
    objects={o['key']:o for o in graph['objects']}
    if len(objects)!=len(graph['objects']) or len({e['key'] for e in graph['edges']})!=len(graph['edges']):raise ValueError('Original occurrence identities must be distinct')
    if any(e[end] not in objects for e in graph['edges'] for end in ('source','target')):raise ValueError('Original endpoint missing')
    def rows(name):return [o['values'] for o in graph['objects'] if o['type']=={'document':model['id'],'module':'domain','element':name}]
    def f(row,name):return row[next(k for k in row if k.endswith('.'+name))]
    def matching(name,key,value):return [r for r in rows(name) if f(r,key)==value]
    cases={}
    cases['effort-event']=[[f(o,'id')] for o in rows('occurrences') for e in matching('effort','id',f(o,'effort_id')) if f(e,'event_id')!=f(o,'event_id')]
    groups={}
    for o in rows('observations'):
     for p in matching('observed_properties','id',f(o,'property_id')):
      if f(p,'name') not in ('temperature','dissolved-oxygen'):continue
      for s in matching('samples','id',f(o,'sample_id')):
       for event in matching('sampling_events','id',f(s,'event_id')):
        for site in matching('monitoring_sites','id',f(event,'site_id')):
         for reach in matching('reaches','id',f(site,'reach_id')):
          for method in matching('methods','id',f(event,'method_id')):
           if f(method,'fraction')=='in-situ':groups.setdefault(f(p,'name'),set()).add(f(reach,'id'))
    cases['connected-measurements']=[[k,str(len(v))] for k,v in sorted(groups.items())]
    cases['censor']=[[f(o,'id'),f(o,'threshold'),f(o,'qualifier')] for o in rows('observations') if f(o,'result_kind')=='censored' and f(o,'value') is None]
    cases['match']=[[f(o,'id')] for o in rows('site_matches') if f(o,'resolution')=='unresolved']
    cases['effort']=[[f(o,'id')] for o in rows('occurrences') for e in matching('effort','id',f(o,'effort_id')) if f(e,'amount') is None]
    cases['zero']=[[f(o,'id')] for o in rows('occurrences') for e in matching('effort','id',f(o,'effort_id')) if int(f(o,'count'))==0 and f(o,'detection')=='not-detected' and f(e,'amount') is not None]
    cases['network']=[[f(o,'upstream_id'),f(o,'downstream_id')] for o in rows('network_links')]
    comparable=set()
    for o in rows('occurrences'):
     for e in matching('sampling_events','id',f(o,'event_id')):
      for m in matching('methods','id',f(e,'method_id')):
       for i in matching('identifications','id',f(o,'identification_id')):
        for t in matching('taxa','id',f(i,'taxon_id')):comparable.add((f(m,'matrix'),f(m,'fraction'),f(t,'rank')))
    cases['comparability']=[list(v) for v in sorted(comparable)]
    cases['fishing']=[[f(o,'effort_unit')] for o in rows('fishing_events')]
    return {'format':'ashlar-original-ecology-graph-oracle/0.1','source_sha256':SOURCE_SHA,'graph_sha256':GRAPH_SHA,'objects':graph['objects'],'edges':graph['edges'],'source_metadata':graph['source_metadata'],'qualification':graph.get('qualification'),'scenarios':cases,'oracle_scope':'Direct original graph lexical field joins; exact seeded graph identifiers retained. No ID decoding, event/table/SQL/receipt inputs or semantic/native execution claim.'}
