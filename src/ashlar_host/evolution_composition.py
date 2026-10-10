"""Original run correspondence; semantic agreement establishes no native authority."""
from dataclasses import dataclass
import hashlib
import json
from typing import Protocol
from ashlar.attempt_store import verify_request_digest
from ashlar.staging import batch_row
from .evolution_admission import EvolutionSourceSet
from .evolution_plan import EvolutionAttemptPlan, EvolutionPlanError
from .evolution_run import (EvolutionRunDefinition, EvolutionRunLedgerView, EvolutionRunPolicy,
                            encoded, request_from_document)


class EvolutionRunDriver(Protocol):
    def validate_evolution_run(self, original: EvolutionRunDefinition, *, context: object) -> None: ...
    def evolution_sources(self, *, context: object) -> EvolutionSourceSet: ...
    def admit_evolution_plan(self, plan: EvolutionAttemptPlan, *, context: object, fresh: bool) -> object: ...
    def admit_evolution_run_ledger(self, original: EvolutionRunDefinition, inventory: EvolutionRunLedgerView,
                                 *, context: object, attempt_admission: object) -> None: ...


@dataclass(frozen=True)
class NativeEvolutionRunPolicy:
    """Compose native observations with independently owned original reservation authority.

    Authority must bind the actual original ledger/location and current writer,
    not merely accept a descriptor or the caller's slot-state flags. Native
    observations below never manufacture a lost positive reservation.
    """
    driver: EvolutionRunDriver
    authority: EvolutionRunPolicy

    def __post_init__(self):
        if (any(not callable(getattr(self.authority, name, None)) for name in
                ('admit_run', 'admit_ledger', 'admit_attempt'))
                or any(not callable(getattr(self.driver, name, None)) for name in
                ('validate_evolution_run', 'evolution_sources', 'admit_evolution_plan', 'admit_evolution_run_ledger'))):
            raise EvolutionPlanError('Mandatory original reservation and native owners required')

    def admit_run(self, original, context):
        if self.authority.admit_run(original, context) is not None:
            raise EvolutionPlanError('Current original reservation admission incomplete')
        self.driver.validate_evolution_run(original, context=context)

    def admit_attempt(self, original, ordinal, plan, context):
        self.admit_run(original, context)
        OriginalRunCorrespondence(original, self.driver.evolution_sources(context=context)).admit_attempt(ordinal, plan)
        if self.authority.admit_attempt(original, ordinal, plan, context) is not None:
            raise EvolutionPlanError('Current original attempt authority incomplete')
        self.driver.admit_evolution_plan(plan, context=context, fresh=False)

    def admit_ledger(self, original, inventory, context):
        self.admit_run(original, context)
        if self.authority.admit_ledger(original, inventory, context) is not None:
            raise EvolutionPlanError('Current complete original reservation custody incomplete')
        owner = self
        class Admission:
            def admit(self, plan, supplied):
                selected = [index for index, _, raw, _, _ in inventory.slots if raw == plan.raw]
                if len(selected) != 1:
                    raise EvolutionPlanError('Exact retained original slot required')
                return owner.admit_attempt(original, selected[0], plan, supplied)
        self.driver.admit_evolution_run_ledger(original, inventory, context=context,
                                              attempt_admission=Admission())
        # Renew independent original custody after all acquired native/session ports close.
        if self.authority.admit_ledger(original, inventory, context) is not None:
            raise EvolutionPlanError('Closing original reservation custody incomplete')


@dataclass(frozen=True)
class OriginalRunCorrespondence:
    original: EvolutionRunDefinition
    sources: EvolutionSourceSet

    def __post_init__(self):
        if type(self.original) is not EvolutionRunDefinition or type(self.sources) is not EvolutionSourceSet:
            raise EvolutionPlanError('Owned original definition and source semantics required')
        definition = self.original.document()
        if encoded(self.sources.metadata()) != encoded(definition['installation']['source_admission']):
            raise EvolutionPlanError('Original complete source semantics differ')
        request = request_from_document(definition['request'])
        if set(request.source_order) != {item.prepared.source_system for item in self.sources.admissions}:
            raise EvolutionPlanError('Original two-source inventory differs')

    def at(self, ordinal):
        """Return copied expected original facts, independently of retained slot fields."""
        if type(ordinal) is not int or not 0 <= ordinal < 8:
            raise EvolutionPlanError('Exact original ordinal required')
        definition = self.original.document(); config = request_from_document(definition['request'])
        step = definition['schedule'][ordinal]
        previous = {source: sum(item['source'] == source for item in definition['schedule'][:ordinal])
                    for source in config.source_order}
        following = dict(previous); following[step['source']] += 1
        clocks = dict(zip(config.source_order, config.clocks))
        progress = {}; expected_request = None
        for index, selected in enumerate(definition['schedule'][:ordinal + 1]):
            admission = next(item for item in self.sources.admissions
                             if item.prepared.source_system == selected['source'])
            batch = admission.prepared.batches[selected['prefix'] - 1]
            raw = batch.begin + b''.join(record.raw for record in batch.records) + batch.commit
            checkpoint = {'profile': 'ashlar-postgresql-outbox/0.1', 'feed': batch.feed,
                'epoch': batch.epoch, 'previous': str(selected['prefix'] - 1),
                'position': str(selected['prefix']), 'payload_digest': hashlib.sha256(raw).hexdigest(),
                'batch_id': batch.batch_id}
            prior_progress = json.loads(encoded(progress))
            progress[batch.feed] = checkpoint
            revisions = {item.prepared.source_system: item.prepared.schema_revisions[
                int(progress[item.prepared.source_system]['position']) - 1]
                for item in self.sources.admissions if item.prepared.source_system in progress}
            row = batch_row(batch)
            expected_request = {'stream': config.stream, 'batch_id': batch.batch_id,
                'predecessor': config.predecessor if index == 0 else definition['schedule'][index - 1]['publication_id'],
                'schema_revisions_json': encoded(revisions).decode(), 'source_batch_json': row['batch_json'],
                'source_batch_digest': row['batch_digest'], 'source_checkpoint_json': encoded(checkpoint).decode()}
            expected_request['request_digest'] = hashlib.sha256(encoded(expected_request)).hexdigest()
            verify_request_digest(expected_request)
        return {'request': expected_request, 'publication_id': step['publication_id'],
            'materialized_at': step['materialized_at'], 'recorded_at': step['recorded_at'],
            'previous_progress': prior_progress, 'progress': progress,
            'schema_state': {'previous_prefixes': previous, 'prefixes': following, 'clocks': clocks},
            'previous_expected': self.sources.oracle(previous, materialized_at=clocks),
            'expected': self.sources.oracle(following, materialized_at=clocks),
            'resource_registry': definition['installation']['registry'], 'source_admission': self.sources.metadata()}

    def admit_attempt(self, ordinal, plan):
        if type(plan) is not EvolutionAttemptPlan:
            raise EvolutionPlanError('Complete original attempt bytes required')
        value = plan.document()
        for field, expected in self.at(ordinal).items():
            if encoded(value[field]) != encoded(expected):
                raise EvolutionPlanError('Original scheduled attempt correspondence differs')
        # Generated/elided operations, prior native anchors, current lineage,
        # writer/source authority and protected ACK remain mandatory native gates.
