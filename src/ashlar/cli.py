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
    args = parser.parse_args()
    lines = iter(lambda: sys.stdin.buffer.readline(1024 * 1024 + 1), b'')
    for batch in jsonl_batches(lines, feed=args.feed, epoch=args.epoch,
                              cursor_before=args.cursor_before):
        value = asdict(batch)
        value['begin_base64'] = base64.b64encode(value.pop('begin')).decode('ascii')
        value['commit_base64'] = base64.b64encode(value.pop('commit')).decode('ascii')
        for record in value['records']:
            record['raw_base64'] = base64.b64encode(record.pop('raw')).decode('ascii')
        print(json.dumps(value, separators=(',', ':')), flush=True)
