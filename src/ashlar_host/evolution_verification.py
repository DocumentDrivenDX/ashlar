"""Existing-only selected publication verification, never publisher recovery."""
from .driver import NativeDriver
from .evolution_composition import NativeEvolutionRunPolicy
from .evolution_operator import ExistingEvolutionLease
from .evolution_run import EvolutionRunDefinition
from .evolution_verification_types import VerifyCommerceEvolutionConfig
from .lifecycle import finish, owned_context


def verify_commerce_evolution(config):
    if (type(config) is not VerifyCommerceEvolutionConfig or type(config.driver) is not NativeDriver
            or type(config.policy) is not NativeEvolutionRunPolicy or config.policy.driver is not config.driver
            or type(config.policy.authority) is not ExistingEvolutionLease
            or type(config.original) is not EvolutionRunDefinition
            or config.original.sha256 != config.expected_sha256
            or config.policy.authority.context is not config.context
            or config.policy.authority.invocation.ordinal != config.ordinal
            or config.policy.authority.invocation.bounds != config.bounds
            or config.policy.authority.invocation.supervision != config.supervision):
        raise PermissionError('Exact live original verification owners required')
    config.__post_init__()
    driver = config.driver; policy = config.policy; original = config.original
    result = None
    with owned_context(driver.writer('original-publication-verification', config.context)):
        opening = None; primary = None
        def renew_local():
            fresh = policy.authority.inspect_ledger(original, config.context)
            if opening is not None and fresh != opening:
                raise PermissionError('Complete original ledger changed during verification')
        def renew_native():
            fresh = policy.authority.inspect_ledger(original, config.context)
            policy.admit_ledger(original, fresh, config.context)
        try:
            opening = policy.authority.inspect_ledger(original, config.context)
            observed = driver.verify_evolution_publication(original, opening, config.ordinal,
                context=config.context, policy=policy, bounds=config.bounds)
            result = {'profile': 'ashlar-evolution-publication-verification/0.1',
                'original_sha256': original.sha256, 'ordinal': config.ordinal,
                'descriptor': observed['descriptor'], 'completed_prefix': observed['completed_prefix'],
                'run_size': 8, 'qualification': 'selected-completed-publication-only',
                'bags': observed['bags'], 'schemas': observed['schemas']}
        except BaseException as error:
            primary = error
        finally:
            finish(primary, [renew_local, renew_native, lambda: driver.require(config.context)])
    return result
