"""Preflight a caller-configured immutable JSONL source without database application."""
import argparse,hashlib,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from jsonl_source_configuration import load_jsonl_configuration
from check_bound_umf_records import check_bound_records
from ashlar.source import jsonl_batches
from ashlar.apply import empty_state,plan_apply
from ashlar.whole_entity import changes_from_batch
from ashlar.staging import batch_row,batch_from_row


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('source-config','umf-source','output'):parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists():parser.error('Fresh output directory required')
    config=load_jsonl_configuration(args.source_config)
    batches=tuple(jsonl_batches(config.source.read_bytes().splitlines(keepends=True),feed=config.feed,epoch=config.epoch))
    if not batches:raise ValueError('Nonempty complete source required')
    args.output.mkdir(parents=True)
    receipts=check_bound_records(args.umf_source,config.intake,config.policy,batches,schema_path=config.schema,output_dir=args.output/'records')
    state=empty_state()
    for batch in batches:
        if batch_from_row(batch_row(batch))!=batch:raise ValueError('Original source staging custody changed')
        state=plan_apply(state,changes_from_batch(batch),schema_policy=config.policy)
    original=state
    for batch in batches:state=plan_apply(state,changes_from_batch(batch),schema_policy=config.policy)
    if state!=original:raise ValueError('Exact source replay changed state')
    config.verify()
    result={'state':'preflight-passed','configuration_sha256':hashlib.sha256(config.original).hexdigest(),
        'source_sha256':hashlib.sha256(config.source.read_bytes()).hexdigest(),'model_sha256':config.intake.source_sha256,
        'feed':config.feed,'epoch':config.epoch,'schema_alias':config.schema_alias,'batches':len(batches),
        'records':sum(len(b.records) for b in batches),'current_entities':len(state.current),'history_records':len(state.history),'tombstones':len(state.tombstones),
        'final_cursor':batches[-1].cursor_after,'exact_replay_unchanged':True,'upstream_record_results':sum(len(r['records']) for r in receipts),
        'qualification':'Configured original model/binding/source, actual UMF logical checks, local complete transaction staging/apply/delete/history/exact replay. No native application, publication, accepted Truss IDs, source ACK or automatic evolution.'}
    (args.output/'summary.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))

if __name__=='__main__':main()
