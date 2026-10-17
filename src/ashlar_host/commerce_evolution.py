"""Owned one-transaction evolution composition; full eight-step run is separate."""
import os  # Standard-library alias retained for existing custody patch consumers.
from .evolution_plan import (EvolutionAttemptPlan, EvolutionPlanJournal, EvolutionPlanError,
    EvolutionPlanPolicy, PROFILE, MAXIMUM_BYTES, FIELDS, owned_fd)

def publish_evolution_transaction(config):
    """Publish one complete original transaction through the owned native driver.

    This is an internal transaction composition, not the eight-step installed run.
    Native registry, current authority, original plan and protected ACK gates remain
    mandatory; no initialization or client discovery takes place here.
    """
    from .config import FreshEvolutionTransactionConfig
    if type(config) is not FreshEvolutionTransactionConfig:
        raise EvolutionPlanError('Distinct fresh transaction configuration required')
    config.__post_init__()
    return config.driver.publish_evolution_attempt(config.journal, context=config.context,
        original_plan=EvolutionAttemptPlan(config.plan_bytes))


def resume_evolution_transaction(config):
    """Load only retained original bytes; never generate a replacement plan."""
    from .config import ResumeEvolutionTransactionConfig
    if type(config) is not ResumeEvolutionTransactionConfig:
        raise EvolutionPlanError('Distinct resume transaction configuration required')
    config.__post_init__()
    return config.driver.publish_evolution_attempt(config.journal, context=config.context,
        expected_sha256=config.expected_sha256)


def _run_commerce_evolution(config, fresh, *, through_ordinal=None):
    from .evolution_run import EvolutionRunLedger, request_from_document, encoded
    from .evolution_admission import EvolutionSourceSet, admit_commerce_evolution
    from .lifecycle import owned_context, finish
    from ashlar.source_checkpoint import bind_source_descriptor
    from .evolution_composition import NativeEvolutionRunPolicy
    import json
    config.__post_init__()
    driver = config.driver
    stream = config.request.stream if fresh else 'original-evolution-resume'
    answer = None
    timing = None
    if through_ordinal is not None and getattr(config, 'stage_observer', None) is not None:
        from .evolution_continuation_timing import EvolutionContinuationTiming
        timing = EvolutionContinuationTiming(config.stage_observer)
    def observed(stage, action):
        return action() if timing is None else timing.call(stage, action)
    manager = owned_context(driver.writer(stream, config.context))
    with (manager if timing is None else timing.context('writer-acquisition', manager)):
        original = driver.capture_evolution_run(config.request, context=config.context) if fresh else None
        definition = None
        def verify_producer():
            metadata = (definition['installation']['source_admission'] if definition is not None
                else driver.source_admission.metadata())
            admitted = tuple(admit_commerce_evolution(config.producer, source_system=source['source_system'],
                epoch=source['epoch']) for source in metadata['sources'])
            if driver.verify_evolution_sources(EvolutionSourceSet(admitted), context=config.context) is not None:
                raise EvolutionPlanError('Fresh public producer verification incomplete')
        ledger = None
        def open_ledger():
            nonlocal ledger
            ledger = EvolutionRunLedger.open(config.ledger_path, config.policy, context=config.context,
                original=original, expected_sha256=None if fresh else config.expected_sha256)
            return ledger
        try:
            observed('ledger-opening', open_ledger)
        except BaseException as error:
            if through_ordinal is not None:
                # No ledger owner was released by open; its own failure closes its connection.
                # Renew only actually acquired current source/native owners, never invent original custody.
                finish(error, ([lambda: observed('closing', ledger.close)] if ledger is not None else []) + [
                    lambda: observed('closing', verify_producer),
                    lambda: observed('closing', lambda: driver.evolution_run_identity(context=config.context))])
            raise
        primary = None
        def current_view():
            def custody(path, selected, context):
                if path != config.ledger_path or selected.raw != original.raw or context is not config.context:
                    raise EvolutionPlanError('Original continuation custody differs')
                return config.policy.admit_run(original, context)
            return EvolutionRunLedger.inspect(config.ledger_path, original, context=config.context, custody=custody)
        def current_slots():
            from .evolution_admission import original_equal
            view = current_view()
            if config.policy.admit_ledger(original, view, config.context) is not None:
                raise EvolutionPlanError('Current complete continuation admission incomplete')
            closing = current_view()
            if not original_equal(view, closing):
                raise EvolutionPlanError('Original continuation inventory changed')
            return view.slots
        try:
            original = ledger.original
            definition = original.document()
            observed('run-identity', lambda: driver.validate_evolution_run(original, context=config.context))
            request = request_from_document(definition['request'])
            observed('producer-admission', verify_producer)
            prefix_inventory = None; prefix_descriptors = ()
            if type(ledger.policy) is NativeEvolutionRunPolicy and ledger.policy.driver is driver:
                prefix_inventory, prefix_descriptors = observed('prefix-native-admission',
                    lambda: driver.observe_completed_evolution_prefix(ledger, context=config.context))
                from .evolution_run import EvolutionRunLedgerView
                from .evolution_admission import original_equal
                if type(prefix_inventory) is not EvolutionRunLedgerView or len(prefix_inventory.slots)!=8:
                    raise EvolutionPlanError('Current complete original prefix inventory required')
                if not original_equal(prefix_inventory,current_view()):
                    raise EvolutionPlanError('Original completed prefix inventory changed')
            if through_ordinal is not None:
                # The native observer already completed the full lineage and
                # source/ACK audit and renewed this exact held inventory. Reuse
                # only its ordinal preflight; later attempt/closing gates stay live.
                slots = prefix_inventory.slots if prefix_inventory is not None else current_slots()
                completed = [slot[0] for slot in slots if slot[1] == 'completed']
                last = completed[-1] if completed else -1
                if fresh and any(slot[1] != 'unprepared' for slot in slots):
                    raise EvolutionPlanError('Fresh bounded run requires all original slots unprepared')
                if not fresh and (through_ordinal < last or through_ordinal > last + 1):
                    raise EvolutionPlanError('Select current completed prefix or first incomplete original ordinal')
                if any(slot[1] == 'publication-started' for slot in slots[through_ordinal + 1:]):
                    raise EvolutionPlanError('Later unresolved original publication prevents settled prefix')
            progress = {}; descriptors = []
            for ordinal, step in enumerate(definition['schedule']):
                if through_ordinal is not None and ordinal > through_ordinal:
                    break
                state, raw, digest, retained_descriptor = (prefix_inventory.slots[ordinal][1:]
                    if ordinal < len(prefix_descriptors) else observed('selected-slot-admission', lambda: ledger.slot(ordinal)))
                if state == 'unprepared':
                    plan = driver.plan_evolution_transaction(original, ordinal, progress, context=config.context)
                    ledger.retain(ordinal, plan)
                    state, raw, digest, retained_descriptor = observed('selected-slot-admission', lambda: ledger.slot(ordinal))
                plan = EvolutionAttemptPlan(raw); value = plan.document()
                if (encoded(value['previous_progress']) != encoded(progress) or value['publication_id'] != step['publication_id']
                        or value['request']['stream'] != request.stream
                        or value['request']['predecessor'] != (request.predecessor if ordinal == 0 else descriptors[-1].publication_id)
                        or value['materialized_at'] != step['materialized_at'] or value['recorded_at'] != step['recorded_at']):
                    raise EvolutionPlanError('Complete original schedule/progress/ancestry differs')
                journal = ledger.attempt(ordinal)
                completed_replay = (state == 'completed' and type(ledger.policy) is NativeEvolutionRunPolicy
                    and ledger.policy.driver is driver)
                descriptor = (prefix_descriptors[ordinal] if completed_replay else
                    driver.publish_evolution_attempt_held(journal, context=config.context, expected_sha256=digest))
                bind_source_descriptor(value['request'], descriptor, expected_publication_id=step['publication_id'])
                actual_progress = json.loads(descriptor.raw['source_progress_json'])
                if encoded(actual_progress) != encoded(value['progress']):
                    raise EvolutionPlanError('Actual complete publication/source progress differs')
                actual = encoded(dict(descriptor.raw))
                if retained_descriptor is not None and retained_descriptor != actual:
                    raise EvolutionPlanError('Original native descriptor replay differs')
                if not completed_replay:
                    ledger.transition(ordinal, plan, 'completed', actual)
                descriptors.append(descriptor)
                progress = actual_progress
            verify_producer()
            driver.validate_evolution_run(original, context=config.context)
            ledger.admit()
            answer = {'profile': 'ashlar-commerce-evolution-run-result/0.1',
                'run_sha256': original.sha256, 'publications': tuple(descriptors)}
            if through_ordinal is not None:
                slots = current_slots()
                if any(slot[1] != 'completed' or slot[4] != encoded(dict(descriptors[index].raw))
                       for index, slot in enumerate(slots[:through_ordinal + 1])):
                    raise EvolutionPlanError('Actual complete original prefix differs')
                answer.update(profile='ashlar-commerce-evolution-prefix-result/0.1',
                    through_ordinal=through_ordinal, completed_count=through_ordinal + 1,
                    total_publications=8, run_complete=through_ordinal == 7,
                    source_progress=progress)
        except BaseException as error:
            primary = error
        callbacks = [ledger.close]
        if through_ordinal is not None:
            callbacks = [verify_producer,
                lambda: (driver.validate_evolution_run(original, context=config.context) if original is not None
                    else driver.evolution_run_identity(context=config.context)),
                ledger.admit, ledger.close]
        if timing is not None:
            callbacks = [lambda action=action: observed('closing', action) for action in callbacks]
        finish(primary, callbacks)
    return answer


def publish_commerce_evolution(config):
    """Publish the fixed original two-source R1→R4 run on supplied native ports.

    Requires an already empty independently authorized installation and ordinary
    source/ACK sessions; does not provision or initialize native resources.
    """
    from .config import FreshCommerceEvolutionConfig
    if type(config) is not FreshCommerceEvolutionConfig:
        raise EvolutionPlanError('Distinct fresh eight-step configuration required')
    return _run_commerce_evolution(config, True, through_ordinal=config.through_ordinal)


def resume_commerce_evolution(config):
    """Reconstruct the fixed run exclusively from retained original reservations."""
    from .config import ResumeCommerceEvolutionConfig
    if type(config) is not ResumeCommerceEvolutionConfig:
        raise EvolutionPlanError('Distinct retained eight-step configuration required')
    return _run_commerce_evolution(config, False)


def continue_commerce_evolution(config):
    """Release only a currently validated original prefix through one ordinal."""
    from .config import ContinueCommerceEvolutionConfig
    if type(config) is not ContinueCommerceEvolutionConfig:
        raise EvolutionPlanError('Distinct original continuation configuration required')
    return _run_commerce_evolution(config, False, through_ordinal=config.through_ordinal)
