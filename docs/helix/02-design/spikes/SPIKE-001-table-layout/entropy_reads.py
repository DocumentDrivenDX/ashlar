"""Uncached native singleton measurements against the immutable r86 baseline."""
import argparse
import concurrent.futures
import json
from pathlib import Path
from driver_sql import DriverClient

B = Path(__file__).resolve().parent
parser = argparse.ArgumentParser()
parser.add_argument('--version', type=int, choices=(0, 1), default=0)
args = parser.parse_args()
version = args.version
O = B / ('out/native/ashlar_entropy_reads_20261006_r88' if version == 0 else 'out/native/ashlar_entropy_post_reads_20261006_r90')
assert not (O / 'summary.json').exists(), 'Completed run exists; audit it instead of repeating measurements'
F = 'client_dev.ashlar_entropy_20261006_r86.edge_current'
clients = [DriverClient(O / f'client-{i}') for i in range(4)]

def read(client, j, label):
    ordinal = 1 + (j * 15485863) % 20000000
    if label == 'changed-row-':
        ordinal = 1 + (((j * 9973) % 200000) * 104729) % 20000000
    key = 4000000 + ordinal
    source = 'other' if key % 10 == 0 else 'pilot'
    src = (key - 1) % 4000000 + 1
    hop = (ordinal - 1) // 4000000 + 1
    typ = 1000 + (src % 32 + 1) * 5 + hop - 1
    rows = client.sql(label + str(j), f"SELECT * FROM {F} VERSION AS OF {version} WHERE lookup_hash=sha2(to_json(named_struct('source_system','{source}','rel_type_id',cast({typ} AS BIGINT),'id',cast({key} AS BIGINT))),256) AND source_system='{source}' AND rel_type_id={typ} AND id={key}")
    assert len(rows) == 1 and rows[0][2] == str(key)
    assert rows[0][0] == source and rows[0][1] == str(typ)
    updated = version == 1 and ((ordinal - 1) * pow(104729, -1, 20000000)) % 20000000 < 200000
    assert rows[0][8] == ('1' if updated else '0')
    if updated:
        assert rows[0][9].startswith('{"107":true')

try:
    for label in ('first-touch-', 'repeat-'):
        for j in range(50):
            read(clients[0], j, label)
    def worker(i):
        for j in range(i, 50, 4):
            read(clients[i], j, 'four-client-')
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(worker, range(4)))
    if version == 1:
        for j in range(50):
            read(clients[0], j, 'changed-row-')
    for client in clients:
        client.history()
    (O / 'summary.json').write_text(json.dumps({
        'state': 'singleton identity/version checks passed; metrics require history audit',
        'table': F, 'version': version, 'counts': {'first_touch': 50, 'repeat': 50, 'four_clients': 50, 'changed_rows': 50 if version == 1 else 0},
        'result_cache': False, 'scope': 'synthetic 20M-edge baseline; first touch is not controlled cold data'
    }, indent=2) + '\n')
finally:
    for client in clients:
        client.close()
