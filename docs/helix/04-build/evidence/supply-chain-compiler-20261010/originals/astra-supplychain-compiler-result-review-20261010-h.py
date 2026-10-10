import hashlib
import json
from pathlib import Path

ROOT = Path('/private/tmp/ashlar-supply-chain-compile-candidate-20261010-h')
OUT = Path('/private/tmp/astra-supplychain-compiler-result-review-20261010-h.json')

def pin(path):
    path = Path(path)
    data = path.read_bytes()
    return dict(path=str(path), bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

def check(item):
    actual = pin(item['path'])
    assert actual['sha256'] == item['sha256'], item['path']
    if 'bytes' in item:
        assert actual['bytes'] == item['bytes'], item['path']

command = json.loads((ROOT / 'command.json').read_text())
assert pin(ROOT / 'command.json')['sha256'] == '649789f68102d390a6136c313047d468151e196b74c186eb4adcb5197171ebbf'
process = json.loads((ROOT / 'process.json').read_text())
assert process['exitCode'] == 0 and process['terminalChunk'] == 'd255e2'
for key in ('command', 'opening', 'closing', 'stdout', 'stderr', 'observations'):
    check(process[key])
assert (ROOT / 'opening-root.json').read_bytes() == (ROOT / 'closing-root.json').read_bytes()
opening = json.loads((ROOT / 'opening-root.json').read_text())
assert opening['commandSha256'] == pin(ROOT / 'command.json')['sha256']
assert len(command['resources']) == len(opening['files']) == 125
assert {x['path']: (x['bytes'], x['sha256']) for x in opening['files']} == {x['path']: (x['bytes'], x['sha256']) for x in command['resources']}
for item in command['resources']:
    check(item)
assert str(Path(command['interpreter_resolution']['argv_path']).resolve()) == command['interpreter_resolution']['resolved_path']
assert process['stdout']['bytes'] == process['stderr']['bytes'] == 0

child_path = Path('/private/tmp/astra-supplychain-compiler-evidence-review-20261010-h.receipt.json')
assert pin(child_path)['sha256'] == '176cf322d9fc8dc99ec78e7b18115c41d460659d6d0b998fdf8562bfccea390e'
child = json.loads(child_path.read_text())
for artifact in child['artifacts']:
    check(artifact)
assert child['exitCode'] == 0 and child['terminalChunk'] == '7b2cf2'
schema_correspondence = []
for schema in child['schemaPins']:
    check(schema)
    installed = Path(command['installation']) / 'schemas' / Path(schema['path']).name
    assert pin(installed)['sha256'] == schema['sha256']
    schema_correspondence.append(pin(installed))

observations = json.loads((ROOT / 'observations.json').read_text())
assert observations['selectedResourcesUnchanged'] == 125
actual_cases = []
for expected, child_case in zip(observations['cases'], child['observations']):
    assert expected['case'] == child_case['case']
    name = expected['case']
    request = ROOT / (name + '.request.json')
    response = ROOT / 'candidate-output' / (name + '.response.json')
    rp = pin(response)
    assert rp['bytes'] == expected['bytes']
    assert rp['sha256'] == expected['sha256'] == child_case['responseSha256']
    value = json.loads(response.read_text())
    assert value['status'] == expected['status'] == child_case['status']
    assert value['interfaceVersion'] == 'weft-compile/0.4.0'
    actual_cases.append(dict(case=name, request=pin(request), response=rp, status=value['status']))
assert [x['status'] for x in actual_cases] == ['compiled', 'compiled', 'blocked', 'compiled', 'compiled']
replay = json.loads((ROOT / 'candidate-output/replay.response.json').read_text())
assert replay['diagnostics'] == [{'code':'WFT-UNSUPPORTED', 'message':'Expected dialect keyword', 'phase':'parse', 'recoverability':'correct-input', 'severity':'error', 'sourceSpan':{'end':87, 'start':86}}]
request = json.loads((ROOT / 'replay.request.json').read_text())
assert request['sql'][86:87] == '*'

receipt = dict(
    status='approved-retained-compiler-observations-for-evidence-landing',
    scope='Actual installed public compiler transport, exact input/output custody, offline public response/IR schemas and request-pin/capability/obligation/frame correspondence only.',
    command=pin(ROOT / 'command.json'), process=pin(ROOT / 'process.json'),
    actual_terminal=dict(chunk='d255e2', exitCode=0, toolWallSeconds=process['toolWallSeconds']),
    selected_resources=dict(count=125, opening=pin(ROOT / 'opening-root.json'), closing=pin(ROOT / 'closing-root.json'), current_hashes_match=True, interpreter_resolution_matches=True),
    streams=[pin(ROOT / 'process.stdout.log'), pin(ROOT / 'process.stderr.log')],
    cases=actual_cases, schema_correspondence=schema_correspondence,
    independent_payload_review=pin(child_path), payload_review_artifacts=[pin(x['path']) for x in child['artifacts']],
    corrected_source_review=pin('/private/tmp/astra-supplychain-binding-source-review-20261010-h.json'),
    command_review=pin('/private/tmp/astra-supplychain-compile-command-review-20261010-h.json'),
    scope_corrections=[
        'E actual five WFT-INPUT responses exposed the wrapper extra target.interfaceVersion; E is retained as malformed-input evidence, not an upstream capability refusal.',
        'G actual four WFT-BINDING responses exposed two-part aliases; H retains the original physical namespace and uses required three-part spark_catalog aliases.',
        'The corrected wrapper and request pairs preserve original SQL, UMF0.8 source and complete binding/publication identities; prior source/input approvals missing those two constraints remain historical.',
        'F was unexecuted with stale copied outcomes; it is not an actual compiler run. H actual custody is opening-root/closing-root only.'
    ],
    limitations=[
        'Four cases compiled; original replay COUNT(*) HAVING remains WFT-UNSUPPORTED at parse span86:87. The complete original five-case task is not finished.',
        'No actual Spark/PostgreSQL query, native result-bag correctness, current publication hold, ACK, cleanup or complete installed workflow qualification.',
        'Schema/descriptor/inventory correspondence does not establish exact native lowering semantics or result acceptance.',
        'Selected resource custody is not full interpreter/OS closure, hermeticity, crash durability or abrupt-parent-death cleanup assurance.',
        'Review is pure retained-file inspection; no compiler, native engine or receiver was rerun.'
    ],
    reviewer_script=pin(__file__)
)
with OUT.open('x') as stream:
    json.dump(receipt, stream, indent=2)
    stream.write('\n')
print(json.dumps(dict(status=receipt['status'], resources=125, compiled=4, blocked=1, receipt=pin(OUT))))
