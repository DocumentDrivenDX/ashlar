"""Independent original lexical supply-chain graph oracle, not an UMF validator.

Original candidate row identities/values are preserved. Public finite-dataset
admission remains separate; no canonical storage IDs or engine acceptance.
"""
import hashlib,json
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
    return {'rows': rows, 'witnesses': {'case': case_id, 'complete_result_occurrences': len(rows),
            'original_objects': len(original['objects']), 'original_edges': len(original['edges'])},
            'scope': 'Complete independent original supply-chain graph bag; no ORDER BY or source/native authority'}
