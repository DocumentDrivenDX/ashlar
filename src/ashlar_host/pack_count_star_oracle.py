"""Independent complete original17 bags and selected Spark041 output carriers.

Original graph oracles preserve lexical source tokens and opaque identities.
This projection consumes no compiler SQL, observed rows, table data or reports.
"""
import json,re
from .finite_pack import FinitePackDefinition

_OUTPUTS={
 'archaeology':{
  'cycle':('stratigraphic_assertions.id','stratigraphic_assertions.id'),
  'dating':('interpretations.author','interpretations.author'),
  'media':('asset_subjects.asset_id',None),
  'missing-media':('assets.id',),
  'specialists':('fauna_results.nisp','fauna_results.mni','pottery_results.sherd_count','pottery_results.estimated_vessels'),
  'lineage':('objects.id','contexts.native_locus'),
  'evidence-links':('interpretations.author','pottery_results.form','fauna_results.taxon'),
  'sample':('samples.parent_id','soil_results.preparation')},
 'ecology':{
  'effort-event':('occurrences.id',),
  'connected-measurements':('observed_properties.name',None),
  'censor':('observations.id','observations.threshold','observations.qualifier'),
  'match':('site_matches.id',),
  'effort':('occurrences.id',),
  'zero':('occurrences.id',),
  'network':('network_links.upstream_id','network_links.downstream_id'),
  'comparability':('methods.matrix','methods.fraction','taxa.rank'),
  'fishing':('fishing_events.effort_unit',)}}


def _decimal(value,facets):
    precision,scale=facets.get('precision'),facets.get('scale')
    if (set(facets)!={'precision','scale'}or type(precision)is not int or type(scale)is not int
            or not 1<=precision<=38 or not 0<=scale<=precision or type(value)is not str
            or len(value)>precision+3 or re.fullmatch('-?(0|[1-9][0-9]*)(?:\.[0-9]+)?',value)is None):
        raise ValueError('Closed original fixed decimal required')
    sign='-'if value.startswith('-')else '';whole,_,fraction=value.lstrip('-').partition('.')
    if len(fraction)>scale or (whole!='0'and len(whole)>precision-scale):raise ValueError('Original decimal cannot be represented without loss')
    if sign and not any(c!='0'for c in whole+fraction):raise ValueError('Negative zero target spelling is not qualified')
    return sign+whole+('.'+fraction.ljust(scale,'0')if scale else '')


def pack_count_star_result_oracle(definition,case_id,model,graph):
    """Complete original bag, with explicit selected optional and LEFT envelopes."""
    if type(definition)is not FinitePackDefinition or definition.name not in _OUTPUTS or type(case_id)is not str or case_id not in _OUTPUTS[definition.name]:
        raise ValueError('Exact original archaeology/ecology case required')
    original=definition.oracle(model,graph);source=json.loads(model)
    fields={(m['id'],f['id']):f for m in source['modules']for f in m['elements']if f['kind']=='field'}
    left_witness=[]
    if definition.name=='archaeology'and case_id=='evidence-links':
        for obj in original['objects']:
            field={'pottery_results':'pottery_results.form','fauna_results':'fauna_results.taxon'}.get(obj['type']['element'])
            if field is not None:
                if obj['type']['module']!='domain' or type(obj['values'].get(field))is not str:
                    raise ValueError('Matched original LEFT payload must be proven nonnull String')
                left_witness.append({'key':obj['key'],'field':field,'value':obj['values'][field]})
        if not left_witness:raise ValueError('Complete original matched LEFT witness required')
    rows=[]
    for original_row in original['scenarios'][case_id]:
        if type(original_row)is not list or len(original_row)!=len(_OUTPUTS[definition.name][case_id]):raise ValueError('Complete original output occurrence required')
        row=[]
        for index,(value,field_id)in enumerate(zip(original_row,_OUTPUTS[definition.name][case_id])):
            left=definition.name=='archaeology'and case_id=='evidence-links'and index in (1,2)
            if field_id is None:
                if type(value)is not str or re.fullmatch('0|[1-9][0-9]*',value)is None:raise ValueError('Exact independent distinct-count text required')
                row.append(value);continue
            field=fields[('domain',field_id)];optional=field['nullability']=='absent-allowed'
            if value is None:
                if left:row.append('{"state":"absent"}')
                elif optional:row.append('{"state":"null"}')
                else:raise ValueError('Required original field cannot be null')
                continue
            if field['scalarType']=='decimal':value=_decimal(value,field.get('facets',{}))
            elif field['scalarType']=='integer':
                if type(value)is not str or len(value.lstrip('-'))>38 or re.fullmatch('-?(0|[1-9][0-9]*)',value)is None:raise ValueError('Exact original integer text required')
            elif field['scalarType']=='string':
                if type(value)is not str:raise ValueError('Exact original String required')
            else:raise ValueError('Unselected original scalar output family')
            row.append(json.dumps({'state':'value','value':value},separators=(',',':'),ensure_ascii=False)if optional or left else value)
        rows.append(row)
    return {'rows':rows,'witnesses':{'pack':definition.name,'case':case_id,'complete_result_occurrences':len(rows),
        'original_objects':len(original['objects']),'original_edges':len(original['edges']),
        'matched_left_nonnull_source':left_witness},
        'scope':'Complete independently derived original graph bag; selected Spark041 fixed decimal/nativeNull/LEFT carrier projection only. No CSV ID decoding, SQL/result repair or source/publication authority.'}
