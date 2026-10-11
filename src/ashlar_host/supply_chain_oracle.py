"""Independent original lexical supply-chain graph oracle, not an UMF validator.

Original candidate row identities/values are preserved. Public finite-dataset
admission remains separate; no canonical storage IDs or engine acceptance.
"""
import hashlib,json,re
from decimal import Decimal
MODEL_SHA='53ae68617d313548bf6c4b3c507c5fc33632c2e8527f88c310927652f635fe9a'
GRAPH_SHA='2c79e403545766dc7c221d6a6c8ff184ea852a8ecf8ef7f00e26c7f2ffc68e13'

def original_graph_oracle(model_bytes,graph_bytes):
    if hashlib.sha256(model_bytes).hexdigest()!=MODEL_SHA or hashlib.sha256(graph_bytes).hexdigest()!=GRAPH_SHA:raise ValueError('Exact original source bytes required')
    model=json.loads(model_bytes);graph=json.loads(graph_bytes)
    nodes={o['key']:o for o in graph['objects']}
    if len(nodes)!=len(graph['objects']) or len({e['key'] for e in graph['edges']})!=len(graph['edges']) or any(e['source'] not in nodes or e['target'] not in nodes for e in graph['edges']):raise ValueError('Closed injective original source carrier inventory required')
    # Read each original candidate field lexical token directly. No event,
    # generated SQL, public receipt or materialized table supplies this oracle.
    tables={}
    for row in graph['objects']:tables.setdefault(row['type']['element'],[]).append(row['values'])
    by_id={table:{r[table+'.id']:r for r in rows} for table,rows in tables.items()}
    lineage=[]
    for item in tables['items']:
        lot=by_id['lots'][item['items.lot_id']];product=by_id['products'][lot['lots.product_id']]
        for containment in tables['containment']:
            if containment['containment.item_id']==item['items.id']:
                container=by_id['containers'][containment['containment.container_id']]
                lineage.append({'serial':item['items.serial'],'lot_code':lot['lots.lot_code'],'sku':product['products.sku'],'parent_id':container['containers.parent_id']})
    split=[]
    for lot in tables['lots']:
        items={r['items.id'] for r in tables['items'] if r['items.lot_id']==lot['lots.id']}
        shipments={r['shipment_items.shipment_id'] for r in tables['shipment_items'] if r['shipment_items.item_id'] in items}
        split.append({'lot_code':lot['lots.lot_code'],'shipment_count':str(len(shipments))})
    counts={}
    for event in tables['events']:counts[event['events.upstream_event_id']]=counts.get(event['events.upstream_event_id'],0)+1
    replay=[{'upstream_event_id':key,'count':str(count)} for key,count in counts.items() if count>1]
    excursion=[{'id':r['sensor_readings.id'],'value':r['sensor_readings.value'],'unit':r['sensor_readings.unit']} for r in tables['sensor_readings'] if Decimal(r['sensor_readings.value'])>Decimal(r['sensor_readings.upper_limit'])]
    sensors=[{'unit':unit,'method':method} for unit,method in sorted({(r['sensor_readings.unit'],r['sensor_readings.method']) for r in tables['sensor_readings']})]
    return {'profile':'ashlar-original-supply-chain-graph-oracle/0.1','qualification':__doc__,'model_sha256':MODEL_SHA,'graph_sha256':GRAPH_SHA,'objects':graph['objects'],'edges':graph['edges'],'original_notices':graph['qualification'],'original_source_metadata':graph['source_metadata'],
            'cases':{'split-excursion':split,'excursion':excursion,'replay':replay,'lineage':lineage,'sensor':sensors}}



def _spark041_decimal_text(value,precision,scale):
    """Independent fixed-scale expected carrier for this selected Spark profile.

    No actual cell is repaired. Original graph lexical values remain unchanged.
    Negative-zero source spelling is outside this helper's qualified subset.
    """
    if (type(precision)is not int or type(scale)is not int or not 1<=precision<=38
            or not 0<=scale<=precision or type(value)is not str
            or len(value)>precision+3 or re.fullmatch('-?(0|[1-9][0-9]*)(?:\.[0-9]+)?',value)is None):
        raise ValueError('Closed original fixed decimal required')
    negative=value.startswith('-');whole,separator,fraction=value.lstrip('-').partition('.')
    if len(fraction)>scale or (whole!='0'and len(whole)>precision-scale):
        raise ValueError('Original decimal cannot be represented without loss')
    if negative and not any(char!='0'for char in whole+fraction):
        raise ValueError('Negative zero target spelling is not qualified')
    return ('-'if negative else '')+whole+('.'+fraction.ljust(scale,'0')if scale else '')

def supply_chain_result_oracle(case_id, model_bytes, graph_bytes):
    """Complete original graph result bag; no compiler SQL/native rows consumed."""
    outputs = {
        'split-excursion': ('lot_code', 'shipment_count'),
        'excursion': ('id', 'value', 'unit'),
        'replay': ('upstream_event_id', 'count'),
        'lineage': ('serial', 'lot_code', 'sku', 'parent_id'),
        'sensor': ('unit', 'method'),
    }
    if type(case_id) is not str or case_id not in outputs:
        raise ValueError('Exact original supply-chain case required')
    if type(model_bytes) is not bytes or type(graph_bytes) is not bytes:
        raise ValueError('Immutable original source bytes required')
    original = original_graph_oracle(model_bytes, graph_bytes)
    rows = [[row[name] for name in outputs[case_id]] for row in original['cases'][case_id]]
    if case_id=='excursion':
        model=json.loads(model_bytes)
        fields=[field for module in model['modules']for field in module['elements']
                if field['id']=='sensor_readings.value']
        if len(fields)!=1 or fields[0].get('scalarType')!='decimal' or set(fields[0].get('facets',{}))!={'precision','scale'}:
            raise ValueError('Original selected fixed decimal definition required')
        facets=fields[0]['facets']
        rows=[[row[0],_spark041_decimal_text(row[1],facets['precision'],facets['scale']),row[2]]for row in rows]
    if case_id=='lineage':
        model=json.loads(model_bytes)
        fields=[field for module in model['modules']for field in module['elements']
                if field['id']=='containers.parent_id']
        if len(fields)!=1 or fields[0].get('scalarType')!='string' or fields[0].get('nullability')!='absent-allowed':
            raise ValueError('Original selected optional String definition required')
        rows=[[row[0],row[1],row[2],json.dumps({'state':'null'}if row[3]is None else {'state':'value','value':row[3]},separators=(',',':'),ensure_ascii=False)]for row in rows]
    return {'rows': rows, 'witnesses': {'case': case_id, 'complete_result_occurrences': len(rows),
            'original_objects': len(original['objects']), 'original_edges': len(original['edges'])},
            'scope': 'Complete independent original supply-chain graph bag for selected Spark041 fixed-scale decimal and optional tagged String output; original graph lexical tokens unchanged. No ORDER BY or source/native authority'}
