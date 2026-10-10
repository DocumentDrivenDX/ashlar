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


def _run_commerce_evolution(config, fresh):
    from .evolution_run import EvolutionRunLedger, request_from_document, encoded
    from .evolution_admission import EvolutionSourceSet, admit_commerce_evolution
    from .lifecycle import owned_context, finish
    from ashlar.source_checkpoint import bind_source_descriptor
    import json
    config.__post_init__()
    driver = config.driver
    stream = config.request.stream if fresh else 'original-evolution-resume'
    answer = None
    with owned_context(driver.writer(stream, config.context)):
        original = driver.capture_evolution_run(config.request, context=config.context) if fresh else None
        ledger = EvolutionRunLedger.open(config.ledger_path, config.policy, context=config.context,
            original=original, expected_sha256=None if fresh else config.expected_sha256)
        primary = None
        try:
            original = ledger.original
            driver.validate_evolution_run(original, context=config.context)
            definition = original.document(); request = request_from_document(definition['request'])
            def verify_producer():
                admitted = tuple(admit_commerce_evolution(config.producer, source_system=source['source_system'],
                    epoch=source['epoch']) for source in definition['installation']['source_admission']['sources'])
                if driver.verify_evolution_sources(EvolutionSourceSet(admitted), context=config.context) is not None:
                    raise EvolutionPlanError('Fresh public producer verification incomplete')
            verify_producer()
            progress = {}; descriptors = []
            for ordinal, step in enumerate(definition['schedule']):
                state, raw, digest, retained_descriptor = ledger.slot(ordinal)
                if state == 'unprepared':
                    plan = driver.plan_evolution_transaction(original, ordinal, progress, context=config.context)
                    ledger.retain(ordinal, plan)
                    state, raw, digest, retained_descriptor = ledger.slot(ordinal)
                plan = EvolutionAttemptPlan(raw); value = plan.document()
                if (encoded(value['previous_progress']) != encoded(progress) or value['publication_id'] != step['publication_id']
                        or value['request']['stream'] != request.stream
                        or value['request']['predecessor'] != (request.predecessor if ordinal == 0 else descriptors[-1].publication_id)
                        or value['materialized_at'] != step['materialized_at'] or value['recorded_at'] != step['recorded_at']):
                    raise EvolutionPlanError('Complete original schedule/progress/ancestry differs')
                journal = ledger.attempt(ordinal)
                descriptor = driver.publish_evolution_attempt_held(journal, context=config.context, expected_sha256=digest)
                bind_source_descriptor(value['request'], descriptor, expected_publication_id=step['publication_id'])
                actual_progress = json.loads(descriptor.raw['source_progress_json'])
                if encoded(actual_progress) != encoded(value['progress']):
                    raise EvolutionPlanError('Actual complete publication/source progress differs')
                actual = encoded(dict(descriptor.raw))
                if retained_descriptor is not None and retained_descriptor != actual:
                    raise EvolutionPlanError('Original native descriptor replay differs')
                ledger.transition(ordinal, plan, 'completed', actual)
                descriptors.append(descriptor)
                progress = actual_progress
            verify_producer()
            driver.validate_evolution_run(original, context=config.context)
            ledger.admit()
            answer = {'profile': 'ashlar-commerce-evolution-run-result/0.1',
                'run_sha256': original.sha256, 'publications': tuple(descriptors)}
        except BaseException as error:
            primary = error
        finish(primary, [ledger.close])
    return answer


def publish_commerce_evolution(config):
    """Publish the fixed original two-source R1→R4 run on supplied native ports.

    Requires an already empty independently authorized installation and ordinary
    source/ACK sessions; does not provision or initialize native resources.
    """
    from .config import FreshCommerceEvolutionConfig
    if type(config) is not FreshCommerceEvolutionConfig:
        raise EvolutionPlanError('Distinct fresh eight-step configuration required')
    return _run_commerce_evolution(config, True)


def resume_commerce_evolution(config):
    """Reconstruct the fixed run exclusively from retained original reservations."""
    from .config import ResumeCommerceEvolutionConfig
    if type(config) is not ResumeCommerceEvolutionConfig:
        raise EvolutionPlanError('Distinct retained eight-step configuration required')
    return _run_commerce_evolution(config, False)
