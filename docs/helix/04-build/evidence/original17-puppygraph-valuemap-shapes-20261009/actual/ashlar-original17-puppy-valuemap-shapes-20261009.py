import json, os
from pathlib import Path
from gremlin_python.driver import client, serializer
from check_pack_scenario_puppygraph import observe_six, source_watcher, admit_guarded, carrier_chunks, raw_projection

base = Path('/private/tmp/ashlar-original17-puppy-activation-inputs-20261009-a')
prep = Path('/private/tmp/ashlar-original17-puppy-preparation-20261009-a')
model = json.loads((base / 'model.json').read_bytes())
databases = json.loads((base / 'databases.json').read_bytes())
trusted = json.loads((base / 'trusted-prepared.json').read_bytes())
candidates = json.load(open('/private/tmp/ashlar-original17-graph-candidates-20261009.json'))
prepared = json.loads((prep / 'receipt.json').read_bytes())
watch = source_watcher(prepared, prep, base / 'model.json', base / 'databases.json', base / 'trusted-prepared.json', candidates)
prepared, original = admit_guarded(prep, trusted, candidates, Path('/private/tmp/ashlar-umf-dataset-compact-fc78'), watch)
user = os.environ['ASHLAR_PUPPY_USER']
password = os.environ['ASHLAR_PUPPY_PASSWORD']
endpoint = 'ws://127.0.0.1:18182/gremlin'
observe = lambda: observe_six('Gremlin', endpoint, model, databases, user, password)
opening = observe()
remote = client.Client(endpoint, 'g', username=user, password=password, message_serializer=serializer.GraphSONSerializersV3d0())
records = []
try:
    profile = prepared['reports'][0]['profile']
    for kind in ('node', 'edge'):
        names = carrier_chunks(profile, kind, 'Gremlin')[0]
        script, expected, names, label = raw_projection(profile, kind, 'Gremlin', names)
        before = observe()
        rows = remote.submit(script, bindings={}).all().result(timeout=20)
        after = observe()
        assert before == after
        types = [{k: type(v).__module__ + '.' + type(v).__name__ for k, v in row['cells'].items()} for row in rows]
        records.append({'kind': kind, 'script': script, 'columns': names, 'native_rows': rows, 'property_types': types, 'opening': before, 'closing': after})
        print(kind, 'native rows', len(rows), 'property types', sorted({v for t in types for v in t.values()}))
finally:
    remote.close()
assert watch() == original and observe() == opening
out = Path('/private/tmp/ashlar-original17-puppy-valuemap-shapes-20261009-a.json')
assert not out.exists()
out.write_text(json.dumps({'format': 'ashlar-native-property-map-shape-diagnostic/0.1', 'qualification': 'Original admitted archaeology carrier-map representation only; no authored scenario or result repair', 'records': records, 'source_files': original, 'opening': opening, 'closing': observe()}, sort_keys=True, ensure_ascii=False, indent=2) + '\n')
