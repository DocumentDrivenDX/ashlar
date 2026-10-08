"""Narrow native automatic-maintenance exclusion check; not retention admission.

Observations must be fresh authenticated native metadata for independently bound
physical targets. Disabling scheduling does not terminate existing operations,
fence other operators, preserve data/log files or qualify active pins.
"""
class RetentionError(ValueError):pass


def validate_predictive_optimization_disabled(setting,effective_flag):
    """Refuse missing, enabled, contradictory or unrecognized native settings.

    INHERIT is admitted only when the complete effective native flag explicitly
    says DISABLE. The caller still needs target identity, all-operator/outstanding
    work containment, retained-file and pin admission. No TTL/default inference.
    """
    if setting not in ('DISABLE','INHERIT'):
        raise RetentionError('Explicit native predictive optimization exclusion required')
    if not isinstance(effective_flag,dict) or effective_flag.get('value')!='DISABLE':
        raise RetentionError('Native automatic maintenance remains enabled or unknown')
    if setting=='INHERIT' and (not isinstance(effective_flag.get('inherited_from_name'),str) or not effective_flag['inherited_from_name']):
        raise RetentionError('Original inherited exclusion source required')
