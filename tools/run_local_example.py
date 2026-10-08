"""Run the fixed, local UMF/string-record source example without external I/O.

Fixture mappings are explicit development authority, not accepted Truss IDs.
No publication, native singleton guarantee or source acknowledgement is issued.
"""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from ashlar.apply import empty_state, plan_apply
from ashlar.catalog import Identity, MappingEntry
from ashlar.schema import SchemaIntake
from ashlar.semantic_policy import StringRecordPolicy
from ashlar.source import jsonl_batches
from ashlar.whole_entity import changes_from_batch

PIN = '16c35e8d943769ccfa7bb57d16785aa7159abe65'


def run():
    example = ROOT / 'examples/end-to-end'
    intake = SchemaIntake.read((example / 'schema-v3.intake.json').read_bytes(),
                               '3', trusted_validator_revision=PIN)
    owner = ('truss-weft-source-review-fixture', 'fixture', 'item')
    entries = [MappingEntry(Identity('type', owner), 17, True)]
    entries += [MappingEntry(Identity('property', owner + ('fixture', field)), ident, True)
                for field, ident in [('label', 23), ('caption', 24)]]
    policy = StringRecordPolicy.from_intake(
        intake, (example / 'schema-v3.interpretation.json').read_bytes(),
        entries, source_system='local-example')
    with (example / 'local-string-source.jsonl').open('rb') as source:
        batches = tuple(jsonl_batches(source, feed='local-jsonl', epoch='example-1'))
    state = empty_state()
    for batch in batches:
        state = plan_apply(state, changes_from_batch(batch), schema_policy=policy)
    original = state
    for batch in batches:
        state = plan_apply(state, changes_from_batch(batch), schema_policy=policy)
    if state != original:
        raise ValueError('Exact replay changed complete graph state')
    live = sorted(state.current.values(), key=lambda value: value.key)
    return {
        'scope': 'local UMF-backed string-record example; fixture IDs only',
        'complete_interpretation': intake.complete_interpretation,
        'batches': len(batches), 'events': len(state.history),
        'objects': len(live), 'tombstones': len(state.tombstones),
        'replay_unchanged': True,
        'rows': [{'id': str(value.key.id), 'version': str(value.version),
                  'props_json': value.props_json, 'retained_json': value.retained_json}
                 for value in live],
        'published': False, 'acknowledged': False,
    }


if __name__ == '__main__':
    print(json.dumps(run(), indent=2, ensure_ascii=False))
