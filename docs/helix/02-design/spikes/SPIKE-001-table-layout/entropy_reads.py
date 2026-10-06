"""Uncached native singleton measurements against the immutable r86 baseline."""
import concurrent.futures
import json
from pathlib import Path
from driver_sql import DriverClient

B = Path(__file__).resolve().parent
O = B / 'out/native/ashlar_entropy_reads_20261006_r88'
F = 'client_dev.ashlar_entropy_20261006_r86.edge_current'
clients = [DriverClient(O / f'client-{i}') for i in range(4)]

def read(client, j, label):
    ordinal = 1 + (j * 15485863) % 20000000
    key = 4000000 + ordinal
    source = 'other' if key % 10 == 0 else 'pilot'
    src = (key - 1) % 4000000 + 1
    hop = (ordinal - 1) // 4000000 + 1
    typ = 1000 + (src % 32 + 1) * 5 + hop - 1
    rows = client.sql(label + str(j), f"SELECT * FROM {F} VERSION AS OF 0 WHERE lookup_hash=sha2(to_json(named_struct('source_system','{source}','rel_type_id',cast({typ} AS BIGINT),'id',cast({key} AS BIGINT))),256) AND source_system='{source}' AND rel_type_id={typ} AND id={key}")
    assert len(rows) == 1 and rows[0][2] == str(key)
    assert rows[0][0] == source and rows[0][1] == str(typ)

try:
    for label in ('first-touch-', 'repeat-'):
        for j in range(50):
            read(clients[0], j, label)
    def worker(i):
        for j in range(i, 50, 4):
            read(clients[i], j, 'four-client-')
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(worker, range(4)))
    for client in clients:
        client.history()
    (O / 'summary.json').write_text(json.dumps({
        'state': '150 singleton identity checks passed; metrics require history audit',
        'table': F, 'version': 0, 'counts': {'first_touch': 50, 'repeat': 50, 'four_clients': 50},
        'result_cache': False, 'scope': 'synthetic 20M-edge baseline; first touch is not controlled cold data'
    }, indent=2) + '\n')
finally:
    for client in clients:
        client.close()
