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
    args = parser.parse_args()
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
