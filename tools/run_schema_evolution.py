"""Local v1-to-v3 UMF schema evolution; fixture authority only, no native writes."""
import json
from run_local_example import ROOT, PIN, fixture_inputs
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


def run():
    intakes, policies, transition, batches = inputs()
    state = empty_state()
    for batch in batches:
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
            'replay_unchanged': True, 'published': False, 'acknowledged': False}


if __name__ == '__main__':
    print(json.dumps(run(), indent=2))
