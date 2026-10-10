"""Independent original archaeology graph oracle; no UMF/native admission claim."""
import hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/archaeology/upstream'
SOURCE_SHA='dc0e075208922c928c6144d3efcc23235735f01aeb70bcf66f96080f1301ca5a'
GRAPH_SHA='d30006199d1a464827194a24e5cd87f8d5b2f073917a3e8253fc95a778adcc52'
def original_oracle(model_bytes,graph_bytes):
    if hashlib.sha256(model_bytes).hexdigest()!=SOURCE_SHA or hashlib.sha256(graph_bytes).hexdigest()!=GRAPH_SHA:raise ValueError('Exact original archaeology bytes required')
    graph=json.loads(graph_bytes);model=json.loads(model_bytes)
    objects={o['key']:o for o in graph['objects']}
    if len(objects)!=len(graph['objects']) or len({e['key'] for e in graph['edges']})!=len(graph['edges']):raise ValueError('Original occurrence identities must be distinct')
    if any(e[end] not in objects for e in graph['edges'] for end in ('source','target')):raise ValueError('Original endpoint missing')
    def rows(name):return [o['values'] for o in graph['objects'] if o['type']=={'document':model['id'],'module':'domain','element':name}]
    def f(row,name):return row[next(k for k in row if k.endswith('.'+name))]
    def matching(name,key,value):return [r for r in rows(name) if f(r,key)==value]
    cases={}
    a=rows('stratigraphic_assertions')
    cases['cycle']=[[f(x,'id'),f(y,'id')] for x in a for y in a if f(x,'id')<f(y,'id') and f(x,'source_context_id')==f(y,'target_context_id') and f(x,'target_context_id')==f(y,'source_context_id')]
    i=rows('interpretations')
    cases['dating']=[[f(x,'author'),f(y,'author')] for x in i for y in i if f(x,'id')<f(y,'id') and f(x,'context_id')==f(y,'context_id') and int(f(x,'earliest'))<=int(f(y,'latest')) and int(f(y,'earliest'))<=int(f(x,'latest'))]
    groups={}
    for r in rows('asset_subjects'):
        if f(r,'context_id') is not None:groups.setdefault(f(r,'asset_id'),set()).add(f(r,'context_id'))
    cases['media']=[[k,str(len(v))] for k,v in sorted(groups.items()) if len(v)>1]
    cases['missing-media']=[[f(r,'id')] for r in rows('assets') if f(r,'availability')=='external']
    cases['specialists']=[]
    for fauna in rows('fauna_results'):
      for analysis in matching('analyses','id',f(fauna,'analysis_id')):
       for sample in matching('samples','id',f(analysis,'sample_id')):
        for lot in matching('find_lots','context_id',f(sample,'context_id')):
         for obj in matching('objects','lot_id',f(lot,'id')):
          for pottery in matching('pottery_results','object_id',f(obj,'id')):
           cases['specialists'].append([f(fauna,'nisp'),f(fauna,'mni'),f(pottery,'sherd_count'),f(pottery,'estimated_vessels')])
    cases['lineage']=[[f(o,'id'),f(c,'native_locus')] for o in rows('objects') for l in matching('find_lots','id',f(o,'lot_id')) for c in matching('contexts','id',f(l,'context_id'))]
    cases['evidence-links']=[]
    for interpretation in sorted(i,key=lambda r:f(r,'id')):
      for evidence in matching('interpretation_evidence','interpretation_id',f(interpretation,'id')):
       pottery=matching('pottery_results','id',f(evidence,'pottery_result_id')) or [None]
       fauna=matching('fauna_results','id',f(evidence,'fauna_result_id')) or [None]
       for p in pottery:
        for a in fauna:cases['evidence-links'].append([f(interpretation,'author'),None if p is None else f(p,'form'),None if a is None else f(a,'taxon')])
    cases['sample']=[[f(s,'parent_id'),f(r,'preparation')] for s in rows('samples') if f(s,'parent_id') is not None for a in matching('analyses','sample_id',f(s,'parent_id')) for r in matching('soil_results','analysis_id',f(a,'id'))]
    return {'format':'ashlar-original-archaeology-graph-oracle/0.1','source_sha256':SOURCE_SHA,'graph_sha256':GRAPH_SHA,'objects':graph['objects'],'edges':graph['edges'],'source_metadata':graph['source_metadata'],'qualification':graph.get('qualification'),'scenarios':cases,'oracle_scope':'Direct original graph lexical field joins; exact seeded graph identifiers retained. No ID decoding, event/table/SQL/receipt inputs or semantic/native execution claim.'}
if __name__=='__main__':print(json.dumps(original_oracle((ROOT/'ontology.json').read_bytes(),(ROOT/'graph/fixture.json').read_bytes()),ensure_ascii=False,indent=2))
