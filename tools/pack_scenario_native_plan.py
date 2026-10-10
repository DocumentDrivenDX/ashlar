"""Closed original17 property-equality analytic plan, independent of graph edges.

This is an explicit native translation profile, not a SQL parser or source model.
Authored SQL remains the intent. Independent original source bags remain oracle.
"""
def plans(pack):
    # scans, equality joins, predicates, logical outputs, grouped count distinct.
    if pack=='archaeology':
        return {
        'cycle':(['a:stratigraphic_assertions','b:stratigraphic_assertions'],['a.source_context_id=b.target_context_id','a.target_context_id=b.source_context_id'],['a.id<b.id'],['a.id','b.id'],None),
        'dating':(['a:interpretations','b:interpretations'],['a.context_id=b.context_id'],['a.id<b.id','a.earliest<=b.latest','b.earliest<=a.latest'],['a.author','b.author'],None),
        'media':(['s:asset_subjects'],[],[],['s.asset_id','COUNT_DISTINCT:s.context_id'],('s.asset_id',1)),
        'missing-media':(['a:assets'],[],["a.availability=STRING:external"],['a.id'],None),
        'specialists':(['f:fauna_results','a:analyses','s:samples','l:find_lots','o:objects','p:pottery_results'],['a.id=f.analysis_id','s.id=a.sample_id','l.context_id=s.context_id','o.lot_id=l.id','p.object_id=o.id'],[],['f.nisp','f.mni','p.sherd_count','p.estimated_vessels'],None),
        'lineage':(['o:objects','l:find_lots','c:contexts'],['l.id=o.lot_id','c.id=l.context_id'],[],['o.id','c.native_locus'],None),
        'evidence-links':(['i:interpretations','e:interpretation_evidence','p:pottery_results','f:fauna_results'],['e.interpretation_id=i.id','OPTIONAL:p.id=e.pottery_result_id','OPTIONAL:f.id=e.fauna_result_id'],[],['i.author','p.form','f.taxon'],None),
        'sample':(['s:samples','a:analyses','r:soil_results'],['a.sample_id=s.parent_id','r.analysis_id=a.id'],['NOT_NULL:s.parent_id'],['s.parent_id','r.preparation'],None)}
    if pack=='ecology':
        common=['o:occurrences','e:effort'];join=['e.id=o.effort_id']
        return {
        'effort-event':(common,join,['e.event_id<>o.event_id'],['o.id'],None),
        'effort':(common,join,['NULL:e.amount'],['o.id'],None),
        'zero':(common,join,['o.count=INTEGER:0','o.detection=STRING:not-detected','NOT_NULL:e.amount'],['o.id'],None),
        'connected-measurements':(['o:observations','p:observed_properties','s:samples','e:sampling_events','site:monitoring_sites','r:reaches','m:methods'],['p.id=o.property_id','s.id=o.sample_id','e.id=s.event_id','site.id=e.site_id','r.id=site.reach_id','m.id=e.method_id'],['IN:p.name:temperature:dissolved-oxygen','m.fraction=STRING:in-situ'],['p.name','COUNT_DISTINCT:r.id'],('p.name',None)),
        'censor':(['o:observations'],[],['o.result_kind=STRING:censored','NULL:o.value'],['o.id','o.threshold','o.qualifier'],None),
        'match':(['s:site_matches'],[],['s.resolution=STRING:unresolved'],['s.id'],None),
        'network':(['n:network_links'],[],[],['n.upstream_id','n.downstream_id'],None),
        'comparability':(['o:occurrences','e:sampling_events','m:methods','i:identifications','t:taxa'],['e.id=o.event_id','m.id=e.method_id','i.id=o.identification_id','t.id=i.taxon_id'],[],['m.matrix','m.fraction','t.rank'],None),
        'fishing':(['f:fishing_events'],[],[],['f.effort_unit'],None)}
    raise ValueError('Closed original scenario pack required')
