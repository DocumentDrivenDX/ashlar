"""Inert exact-source gate for installed receiver D. No candidate imports."""
import ast
import hashlib
import json
from pathlib import Path
import stat

BASE = Path('/private/tmp/ashlar-otel-installed-receiver-probe-20261010-')
C = Path(str(BASE) + 'c')
D = Path(str(BASE) + 'd')
PREP = Path('/private/tmp/ashlar-otel-installed-preparation-20261010-b')
SITE = PREP / 'environment/lib/python3.11/site-packages'
OUT = Path('/private/tmp/astra-otel-installed-receiver-delta-source-review-20261010-d.json')
pins = {}
checks = []

def check(value, label):
    if not value:
        raise AssertionError(label)
    checks.append(label)

def read(path, size=None, digest=None):
    info = path.lstat()
    check(stat.S_ISREG(info.st_mode) and not path.is_symlink() and info.st_size <= 1048576, 'bounded regular input: ' + path.name)
    raw = path.read_bytes()
    pin = {'path': str(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}
    if size is not None: check(len(raw) == size, 'exact size: ' + path.name)
    if digest is not None: check(pin['sha256'] == digest, 'exact SHA256: ' + path.name)
    if str(path) in pins: check(pin == pins[str(path)], 'unchanged closing bytes: ' + path.name)
    pins[str(path)] = pin
    return raw

c = read(C / 'candidate.py', 13465, 'b1da715f60001fb8f650167d074daf66eb469cf16b0bfaccfe6b8f170ee4857c').decode()
d = read(D / 'candidate.py', 14036, 'a5b2d63ccf5b2fb7dd0b699976728057881e5f56ee40e0008434eb0c45f67ab9').decode()
command = json.loads(read(D / 'command.json', 2112, '90c25a9d8ca5ad77880828f4ff7ef9baf6d6ca51cc22ca055f97ebfe1ce266b9'))
read(PREP / 'freeze.json', digest='42aa7c1f88287b1d4704c8da10eee8f4c100da5287dbe2536d631047735b42aa')
custody = json.loads(read(PREP / 'installed-custody.json'))
check(custody['sourceCommit'] == command['sourceCommit'] == 'ea4b4ff433edfe096ea40290f19cf1439c5fae33', 'exact committed spanless source identity')
record = {item['path']: item for item in custody['recordFiles']}
sources = {}
for name in ('otel.py', '_otel_worker.py', 'diagnostics.py'):
    path = SITE / 'ashlar_host' / name
    item = record[str(path)]
    sources[name] = read(path, item['bytes'], item['sha256']).decode()
contract = read(Path('/Users/erik/Projects/ashlar/docs/helix/02-design/contracts/CONTRACT-006-diagnostics.md'),
     digest='06a8919aaecdfbf6f83b53bfbfa0cb09574cc654b2dcb827226cb55c6f8607cd').decode()
check('*, create_span=True)' in contract and 'same log and completion-counter semantics, submits no span' in contract,
      'current C006 explicitly governs genuine spanless invocation semantics')
metadata_path = SITE / 'ashlar_graph_toolkit-0.1.0.dev0.dist-info/METADATA'
metadata = read(metadata_path, record[str(metadata_path)]['bytes'], record[str(metadata_path)]['sha256']).decode()
check('\nName: ashlar-graph-toolkit\nVersion: 0.1.0.dev0\n' in metadata, 'actual installed service version remains 0.1.0.dev0')
ct, dt = ast.parse(c), ast.parse(d)
def top(tree):
    return {node.name: ast.dump(node, include_attributes=False) for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef))}
cf, df = top(ct), top(dt)
check(cf.keys() == df.keys() and all(cf[key] == df[key] for key in cf if key != 'main'), 'all helper, receiver, cleanup and attribute/scope functions unchanged from actual C')

insert = """        with owner.operation_context(create_span=False):
            third = run.begin_attempt('source-admission')
        run.phase(third, 'admission', 'completed')
        run.finish_attempt(third, 'succeeded')
"""
check(d.count(insert) == 1, 'third attempt starts spanless; phase/finish are outside the binding')
main = next(node for node in dt.body if isinstance(node, ast.FunctionDef) and node.name == 'main')
calls = [node for node in ast.walk(main) if isinstance(node, ast.Call)]
begin = [node for node in calls if isinstance(node.func, ast.Attribute) and node.func.attr == 'begin_attempt']
check([ast.literal_eval(node.args[0]) for node in sorted(begin, key=lambda n: n.lineno)] == ['held-read', 'publication', 'source-admission'], 'exact three sequential synthetic invocation operations')
bindings = [node for node in ast.walk(main) if isinstance(node, ast.With) and any(isinstance(i.context_expr, ast.Call) and isinstance(i.context_expr.func, ast.Attribute) and i.context_expr.func.attr == 'operation_context' for i in node.items)]
check(len(bindings) == 3 and len(bindings[2].body) == 1 and isinstance(bindings[2].body[0], ast.Assign), 'third explicit binding contains only begin_attempt')
check(ast.unparse(bindings[2].items[0].context_expr) == 'owner.operation_context(create_span=False)', 'third binding carries no parent or retry context')
requirements = [
    "assert len(received) == 12 and [sum(path == route for path, _ in received) for route in ('/v1/logs', '/v1/traces', '/v1/metrics')] == [9, 2, 1]",
    "assert snapshot['capture_complete'] and snapshot['matched'] == 9 and len(events) == 9",
    "assert resource['service.version'] == metadata.version('ashlar-graph-toolkit')",
    "assert len(logs) == 9 and len(spans) == 2 and len(points) == 3",
    "assert log.time_unix_nano == 0 and log.observed_time_unix_nano == int(event['observed_timestamp_unix_nano'])",
    "assert attributes(log.attributes) == {**event['attributes'], 'ashlar.diagnostic.schema_version': event['schema_version']}",
    "assert event['attributes']['ashlar.operation'] == 'source-admission'",
    "assert not log.trace_id and not log.span_id and log.flags == 0",
    "assert all('trace' not in event for event in events[6:])",
    "for signal, count in (('logs', 9), ('spans', 2), ('metrics', 3)):",
    "assert manifest['loss'][signal] == {'submitted': count, 'handed_off': count, 'dropped': 0, 'unknown': False, 'flush': 'complete'}",
    "{'ashlar.operation': 'source-admission', 'ashlar.outcome': 'succeeded'}",
]
for item in requirements: check(item in d, 'required new/retained oracle: ' + item)
check(command['bounds']['attempts'] == 3 and command['bounds']['expectedHttpRequests'] == 12
      and command['logicalUnits'] == {'logs': 9, 'spans': 2, 'metrics': 3}
      and command['expectedEndpointRequests'] == {'/v1/logs': 9, '/v1/traces': 2, '/v1/metrics': 1}, 'command agrees with separate request/logical-unit counts')

# Undo only the listed reviewed delta and require all remaining source to equal
# historical C exactly, including original traced-span and metric oracles.
normal = d.replace(str(D), str(C)).replace(insert, '')
normal = normal.replace("assert len(received) == 12", "assert len(received) == 9")
normal = normal.replace("== [9, 2, 1]", "== [6, 2, 1]")
normal = normal.replace("snapshot['matched'] == 9 and len(events) == 9", "snapshot['matched'] == 6 and len(events) == 6")
normal = normal.replace('assert len(logs) == 9 and len(spans) == 2 and len(points) == 3', 'assert len(logs) == 6 and len(spans) == 2 and len(points) == 2')
new_context = """        if 'trace' in event:
            trace = event['trace']
            assert log.trace_id.hex() == trace['trace_id'] and log.span_id.hex() == trace['span_id'] and log.flags == trace['trace_flags']
        else:
            assert event['attributes']['ashlar.operation'] == 'source-admission'
            assert not log.trace_id and not log.span_id and log.flags == 0
"""
old_context = """        trace = event['trace']
        assert log.trace_id.hex() == trace['trace_id'] and log.span_id.hex() == trace['span_id'] and log.flags == trace['trace_flags']
"""
check(normal.count(new_context) == 1, 'exact new trace presence/absence branch')
normal = normal.replace(new_context, old_context).replace("    assert all('trace' not in event for event in events[6:])\n", '')
normal = normal.replace("{'ashlar.operation': 'publication', 'ashlar.outcome': 'refused'},\n        {'ashlar.operation': 'source-admission', 'ashlar.outcome': 'succeeded'}]", "{'ashlar.operation': 'publication', 'ashlar.outcome': 'refused'}]")
normal = normal.replace("(('logs', 9), ('spans', 2), ('metrics', 3))", "(('logs', 6), ('spans', 2), ('metrics', 2))")
normal = normal.replace('three sequential synthetic attempts, explicit parent and external retry-link context plus genuinely spanless third invocation', 'two sequential synthetic attempts, explicit parent and external retry-link context')
normal = normal.replace("'No concurrent attempt test', 'No real publication/query/source/ACK workflow'", "'No concurrent attempt test', 'No untraced event support claim', 'No real publication/query/source/ACK workflow'")
check(normal == c, 'candidate D is precisely the reviewed delta; all other actual C source bytes unchanged')

for fragment in ("if started:\n            request['create_span'] = create_span", 'self._operation_binding.reset(token)'):
    check(fragment in sources['otel.py'], 'facade started-only selection and binding restoration: ' + fragment)
for fragment in ("set_status_on_exception=False) if request['create_span'] else None",
                 "'span': span, 'operation': request['operation']",
                 "actual = attempt['span'].get_span_context() if attempt['span'] is not None else None",
                 "result = None if actual is None else",
                 "(actual is None and 'trace' not in event)",
                 'token = attach(Context()) if actual is None else None',
                 'trace_id=actual.trace_id if actual is not None else 0, span_id=actual.span_id if actual is not None else 0',
                 'trace_flags=actual.trace_flags if actual is not None else 0',
                 "if attempt['span'] is not None:\n                attempt['span'].set_attribute('ashlar.outcome', outcome)",
                 "self.counter.add(1, {'ashlar.operation': key[0], 'ashlar.outcome': key[1]}, context=Context())"):
    check(fragment in sources['_otel_worker.py'], 'installed spanless correspondence: ' + fragment)

opening = list(pins.values())
for pin in opening: read(Path(pin['path']), pin['bytes'], pin['sha256'])
report = {
    'format': 'astra-independent-unexecuted-receiver-source-gate/0.1',
    'verdict': 'approved-source-oracle-only', 'candidate': pins[str(D / 'candidate.py')],
    'method': 'Read-only source comparison, inert stdlib AST inspection and exact installed-source custody checks; no candidate/module execution, SDK/provider construction, receiver/worker/network/native run.',
    'checks': checks,
    'scope': 'Three sequential synthetic public attempts; third source-admission starts with create_span=False and leaves binding before phase/finish; expected nine logs, two spans, one cumulative integer metric request carrying three points.',
    'oracleAssessment': [
        'The first two traced invocations retain every C parent/link/span/flags/status/time oracle unchanged.',
        'All nine local events must be captured; all nine OTLP logs retain exact original timestamp/severity/body/EventName/attributes/schema version/resource/scope mapping.',
        'The final three local events must omit trace. Their logs must have empty trace ID/span ID and zero flags; trace-less events must identify source-admission.',
        'Exactly two ended spans remain, so no span is exported for the third invocation.',
        'One cumulative monotonic integer Sum contains three points, each value one, with only operation/outcome dimensions; the source-admission/succeeded point is required.',
        'Manifest export loss checks use logical units 9/2/3 independently of HTTP requests 9/2/1.',
        'Resource service.version remains actual installed metadata, not the fixture SDK version.'
    ],
    'conditions': ['Parent owns command/input/fresh-path/cleanup review and opening/closing verification. Actual receiver qualification requires retained D execution and wire review; this receipt makes no actual outcome claim.'],
    'limitations': ['No concurrent attempts, native workflow, privacy sentinel, outage/deadline/cancellation or full C006 qualification is implied.', 'No original C evidence was edited.'],
    'reviewerCorrection': 'Initial inert harness stopped on historical C006 hash; reviewed current explicit spanless contract and corrected pin before continuing. Historical-hash failure log retained; not a product or candidate failure.',
    'openingClosingPins': opening,
    'script': {'path': __file__, 'bytes': Path(__file__).stat().st_size, 'sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
    'findings': []
}
raw = (json.dumps(report, indent=2, sort_keys=True) + '\n').encode()
with OUT.open('xb') as stream: stream.write(raw)
print(json.dumps({'receipt': str(OUT), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(), 'checks': len(checks), 'findings': 0}))
