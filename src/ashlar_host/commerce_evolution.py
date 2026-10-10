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
