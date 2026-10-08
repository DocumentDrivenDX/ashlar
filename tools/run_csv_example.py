"""Run the additional CSV source through the actual UMF-backed apply pipeline."""
import base64,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from ashlar.csv_source import csv_batches
from ashlar.apply import empty_state,plan_apply
from ashlar.whole_entity import changes_from_batch
from ashlar.staging import batch_row,batch_from_row
from run_local_example import fixture_inputs

def run():
    intake,policy,_=fixture_inputs()
    raw=(ROOT/'examples/end-to-end/string-source.csv').read_bytes()
    originals=raw.splitlines(keepends=True)
    batches=tuple(csv_batches(originals,feed='csv-example',epoch='immutable-example-1',source_system='local-example',schema_revision='3',type_id='17',properties={'label':'23','caption':'24'}))
    state=empty_state()
    for ordinal,batch in enumerate(batches,1):
        retained=json.loads(batch.records[0].delivery_id)
        if base64.b64decode(retained['header_base64'])!=originals[0] or base64.b64decode(retained['row_base64'])!=originals[ordinal]:raise ValueError('CSV original custody changed')
        state=plan_apply(state,changes_from_batch(batch),schema_policy=policy)
    expected=state
    for batch in batches:state=plan_apply(state,changes_from_batch(batch_from_row(batch_row(batch))),schema_policy=policy)
    if state!=expected:raise ValueError('Replay changed CSV-derived state')
    if len(state.current)!=1 or len(state.tombstones)!=1 or len(state.history)!=4:raise ValueError('Unexpected CSV graph')
    live=next(iter(state.current.values()))
    return {'source_profile':'ashlar-single-line-csv/0.1','batches':len(batches),'events':len(state.history),'objects':len(state.current),'tombstones':len(state.tombstones),'replay_unchanged':True,'props_json':live.props_json,'complete_interpretation':intake.complete_interpretation,'published':False,'acknowledged':False,'qualification':'Local additional source; exact CSV custody and UMF-backed selected string constraints. Fixture IDs only; not a Truss catalog or native publication.'}
if __name__=='__main__':print(json.dumps(run(),indent=2,ensure_ascii=False))
