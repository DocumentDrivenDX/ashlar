"""Local v1-to-v3 UMF schema evolution; fixture authority only, no native writes."""
import json
import argparse
from pathlib import Path
from run_local_example import ROOT, PIN, fixture_inputs, check_original_records, check_existing_records
from ashlar.schema import _json
from ashlar.apply import empty_state, plan_apply
from ashlar.catalog import Identity, MappingEntry
from ashlar.schema import SchemaIntake
from ashlar.schema_policies import SchemaPolicies
from ashlar.semantic_policy import StringRecordPolicy
from ashlar.source import jsonl_batches
from ashlar.staging import batch_row, batch_from_row
from ashlar.whole_entity import changes_from_batch
from ashlar.recovery import recover_whole_entity_state


def inputs():
    v3, policy3, _ = fixture_inputs()
    example = ROOT / 'examples/end-to-end'
    v1 = SchemaIntake.read((example / 'schema-v1.intake.json').read_bytes(), '1',
                          trusted_validator_revision=PIN)
    owner = ('truss-weft-source-review-fixture', 'fixture', 'item')
    entries = [MappingEntry(Identity('type', owner), 17, True),
               MappingEntry(Identity('property', owner + ('fixture', 'label')), 23, True)]
    policy1 = StringRecordPolicy.from_intake(v1,
        (example / 'schema-v1.interpretation.json').read_bytes(), entries,
        source_system='local-example')
    policies = SchemaPolicies({('local-example', '1'): policy1,
                               ('local-example', '3'): policy3})
    # Explicit fixture authority for exactly the demonstrated additive transition;
    # no implicit version ordering or automatic compatibility claim.
    def transition(previous, change):
        if (previous.key.source, previous.key.kind, previous.key.type_id,
            previous.schema_revision, change.state.schema_revision, change.operation) != (
                'local-example', 'object', 17, '1', '3', 'replace'):
            raise PermissionError('Unadmitted fixture schema transition')
    with (example / 'schema-evolution-source.jsonl').open('rb') as source:
        batches = tuple(jsonl_batches(source, feed='local-evolution', epoch='example-1'))
    return (v1, v3), policies, transition, batches


def run(umf_source=None, umf_check_output_dir=None):
    intakes, policies, transition, batches = inputs()
    upstream = []
    if umf_source is not None:
        for intake in intakes:
            selected = []
            for batch in batches:
                revisions = {_json(record.raw)['schema_revision'] for record in batch.records}
                if len(revisions) != 1:
                    raise ValueError('This fixture requires explicit single-revision batches')
                if revisions == {intake.document_revision}:
                    selected.append(batch)
            output = None if umf_check_output_dir is None else umf_check_output_dir / ('schema-' + intake.document_revision + '.json')
            result = check_original_records(umf_source, intake, selected, output,
                ROOT / ('examples/end-to-end/schema-v' + intake.document_revision + '.umf.json'))
            upstream.append({'revision': intake.document_revision, 'records': len(result['records']),
                             'document_complete': result['originalValidation']['complete']})
    existing_checks = []
    state = empty_state()
    for batch in batches:
        changes = changes_from_batch(batch)
        if umf_source is not None and any(change.state.schema_revision == '3' and
            change.state.key in state.current and state.current[change.state.key].schema_revision == '1'
            for change in changes):
            output = None if umf_check_output_dir is None else umf_check_output_dir / 'existing-values-under-v3.json'
            result = check_existing_records(umf_source, intakes[1], state, output,
                                            ROOT / 'examples/end-to-end/schema-v3.umf.json')
            existing_checks.append({'target_revision': '3', 'existing_records': len(result['records']),
                                    'native_migration': False})
        state = plan_apply(state, changes_from_batch(batch), schema_policy=policies,
                           schema_transition_policy=transition)
    original = state
    rows = json.loads(json.dumps([batch_row(batch) for batch in batches]))
    recovered = recover_whole_entity_state(rows, prior=empty_state(), feed='local-evolution',
        epoch='example-1', cursor_before='0', cursor_after=batches[-1].cursor_after,
        schema_policy=policies, schema_transition_policy=transition)
    state = recovered.state
    if state != original:
        raise ValueError('Schema evolution replay changed retained state')
    return {'scope': 'local admitted fixture schema transition; not Truss acceptance',
            'document_revisions': [intake.document_revision for intake in intakes],
            'complete_interpretations': [intake.complete_interpretation for intake in intakes],
            'history_revisions': sorted({change.state.schema_revision for change in state.history.values()}),
            'live_schema_revisions': sorted({entity.schema_revision for entity in state.current.values()}),
            'events': len(state.history), 'tombstones': len(state.tombstones),
            'upstream_record_checks': upstream or None, 'existing_value_checks': existing_checks or None,
            'replay_unchanged': True, 'published': False, 'acknowledged': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--umf-source', type=Path)
    parser.add_argument('--umf-check-output-dir', type=Path)
    args = parser.parse_args()
    if args.umf_check_output_dir is not None and args.umf_source is None:
        parser.error('--umf-check-output-dir requires --umf-source')
    print(json.dumps(run(args.umf_source, args.umf_check_output_dir), indent=2))
