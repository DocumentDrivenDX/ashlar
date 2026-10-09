"""Installed bounded source-custody command; native deployment is a separate host."""
import argparse
import base64
from dataclasses import asdict
import json
import sys
from .source import jsonl_batches


def main():
    parser = argparse.ArgumentParser(prog='ashlar')
    commands = parser.add_subparsers(dest='command', required=True)
    inspect = commands.add_parser('inspect-source', help='Verify committed JSONL batches from stdin; no ACK')
    inspect.add_argument('--feed', required=True)
    inspect.add_argument('--epoch', required=True)
    inspect.add_argument('--cursor-before', default='0')
    csv = commands.add_parser('inspect-csv', help='Adapt bounded UTF-8 CSV to retained source batches/checkpoints; no ACK')
    for name in ('feed','epoch','source-system','schema-revision','type-id','properties-json'):
        csv.add_argument('--'+name,required=True)
    configured=commands.add_parser('inspect-configured-source',help='Inspect pinned model/ID/source configuration and local replay; no database I/O')
    configured.add_argument('configuration')
    commerce=commands.add_parser('commerce-source',help='Build an exact original commerce candidate with development bindings')
    for name in ('ontology','graph','output'):
        commerce.add_argument('--'+name,required=True)
    commerce.add_argument('--source-system',required=True)
    commerce.add_argument('--binding-profile',choices=['ashlar-commerce-development-bindings/0.1','ashlar-commerce-development-bindings/0.2'],default='ashlar-commerce-development-bindings/0.2')
    for pack in ('archaeology','ecology'):
        command=commands.add_parser(pack+'-source',help='Build an exact original '+pack+' candidate with development bindings')
        for name in ('ontology','graph','output','source-system'):
            command.add_argument('--'+name,required=True)
    medical=commands.add_parser('medical-source',help='Build an exact historical medical candidate using explicit public admission')
    for name in ('ontology','graph','public-admission','output','source-system'):
        medical.add_argument('--'+name,required=True)
    medical.add_argument('--binding-profile',required=True,choices=['ashlar-medical-development-bindings/0.2'])
    args = parser.parse_args()
    if args.command=='medical-source':
        from .medical_source import write_candidate
        batch,_=write_candidate(args.ontology,args.graph,args.public_admission,args.output,source_system=args.source_system,binding_profile=args.binding_profile)
        print('Created candidate historical medical transaction: '+str(len(batch.records))+' events; development bindings only; native host must recompute public admission')
        return
    if args.command in ('archaeology-source','ecology-source'):
        if args.command=='archaeology-source':
            from .archaeology_source import write_candidate
        else:
            from .ecology_source import write_candidate
        batch,_=write_candidate(args.ontology,args.graph,args.output,source_system=args.source_system)
        print('Created candidate '+args.command[:-7]+' transaction: '+str(len(batch.records))+' events; development bindings only')
        return
    if args.command=='commerce-source':
        from .commerce_source import write_candidate
        write_candidate(args.ontology,args.graph,args.output,source_system=args.source_system,binding_profile=args.binding_profile)
        print('Created candidate commerce transaction: 11 objects, 10 edges; development bindings only')
        return
    if args.command=='inspect-configured-source':
        from .source_config import load_jsonl_configuration,inspect_configured_source
        print(json.dumps(inspect_configured_source(load_jsonl_configuration(args.configuration)),separators=(',',':')))
        return
    if args.command == 'inspect-csv':
        from .csv_source import csv_batches,validate_csv_batch
        from .schema import _json
        from .staging import batch_row
        from .source_checkpoint import csv_checkpoint
        mapping=_json(args.properties_json.encode('utf-8'))
        lines=iter(lambda:sys.stdin.buffer.readline(65537),b'')
        for batch in csv_batches(lines,feed=args.feed,epoch=args.epoch,
                source_system=args.source_system,schema_revision=args.schema_revision,
                type_id=args.type_id,properties=mapping):
            validate_csv_batch(batch,feed=args.feed,epoch=args.epoch,source_system=args.source_system,
                schema_revision=args.schema_revision,type_id=args.type_id,properties=mapping)
            print(json.dumps({'format':'ashlar-csv-source-custody/0.1',
                'batch_row':batch_row(batch),'source_checkpoint_json':csv_checkpoint(batch)},
                separators=(',',':')),flush=True)
        return

    lines = iter(lambda: sys.stdin.buffer.readline(1024 * 1024 + 1), b'')
    for batch in jsonl_batches(lines, feed=args.feed, epoch=args.epoch,
                              cursor_before=args.cursor_before):
        value = asdict(batch)
        value['begin_base64'] = base64.b64encode(value.pop('begin')).decode('ascii')
        value['commit_base64'] = base64.b64encode(value.pop('commit')).decode('ascii')
        for record in value['records']:
            record['raw_base64'] = base64.b64encode(record.pop('raw')).decode('ascii')
        print(json.dumps(value, separators=(',', ':')), flush=True)
