"""Preflight a caller-configured immutable JSONL source without database application."""
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from jsonl_source_configuration import load_jsonl_configuration,inspect_configured_source
from check_bound_umf_records import check_bound_records
from ashlar.source import jsonl_batches


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
    result=inspect_configured_source(config)
    result.update(state='preflight-passed',upstream_record_results=sum(len(r['records']) for r in receipts),
        qualification='Configured original model/binding/source, actual UMF logical checks, local complete transaction staging/apply/delete/history/exact replay. No native application, publication, accepted Truss IDs, source ACK or automatic evolution.')
    (args.output/'summary.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))

if __name__=='__main__':main()
