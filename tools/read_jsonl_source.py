"""Read complete source batches from stdin; emits custody, never acknowledges."""
import base64
from dataclasses import asdict
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from ashlar.source import jsonl_batches
if len(sys.argv)!=3:raise SystemExit('Usage: python3 tools/read_jsonl_source.py FEED EPOCH < source.jsonl')
for batch in jsonl_batches(iter(lambda:sys.stdin.buffer.readline(1024*1024+1),b''),feed=sys.argv[1],epoch=sys.argv[2]):
    value=asdict(batch)
    value['begin_base64']=base64.b64encode(value.pop('begin')).decode()
    value['commit_base64']=base64.b64encode(value.pop('commit')).decode()
    for record in value['records']:record['raw_base64']=base64.b64encode(record.pop('raw')).decode()
    print(json.dumps(value),flush=True)
