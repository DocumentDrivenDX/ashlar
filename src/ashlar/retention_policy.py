"""Compose finite retention with mandatory independent publication/read policies.

No permissive authorization, original timestamp custody, file availability,
protocol or source admission is supplied. Native configuration observations are
fresh on each check; an independently qualified provider may batch metadata.
"""
import time
from collections.abc import Mapping
from .publication import Descriptor,_decode,_freeze
from .retention import RetentionError,_micros,observe_retention_configuration,validate_publication_retention


class SQLRetentionProvider:
    """Bounded development SQL provider; no history scans or settings mutations.

    UUID allowlist and platform default profile must be independently admitted.
    Production providers may use qualified metadata APIs instead of serial SQL.
    The supplied span budget must fit the gate's reserved observation margin.
    """
    def __init__(self,executor,table_uuids,*,defaults,default_profile,max_observation_span_us):
        if type(max_observation_span_us) is not int or max_observation_span_us<=0:
            raise RetentionError('Explicit positive observation span budget required')
        self.executor=executor;self.table_uuids=dict(table_uuids)
        self.defaults=dict(defaults);self.default_profile=default_profile
        self.required_margin_us=max_observation_span_us

    def observe(self,descriptor,context):
        start=time.monotonic_ns()
        if not set(descriptor.versions)<=set(self.table_uuids):raise RetentionError('Unadmitted retention target')
        observations={table:observe_retention_configuration(self.executor,table,self.table_uuids[table],defaults=self.defaults,default_profile=self.default_profile)
                      for table in descriptor.versions}
        rows=self.executor.query('SELECT cast(unix_micros(current_timestamp()) AS STRING) AS now_us',{}).rows
        if len(rows)!=1 or set(rows[0])!={'now_us'}:raise RetentionError('Complete native clock required')
        _micros(rows[0]['now_us'])
        if time.monotonic_ns()-start>=self.required_margin_us*1000:
            raise RetentionError('Native retention observation span exceeded reserved budget')
        return {'configurations':{t:o['configuration'] for t,o in observations.items()},
                'table_uuids':{t:o['uuid'] for t,o in observations.items()},'now_us':rows[0]['now_us']}


class RetentionGate:
    """Use from both manifest admission and resolver descriptor admission."""
    def __init__(self,provider,*,minimum_margin_us):
        if type(minimum_margin_us) is not int or minimum_margin_us<0:
            raise RetentionError('Explicit nonnegative host margin required')
        if minimum_margin_us<getattr(provider,'required_margin_us',0):
            raise RetentionError('Host margin does not reserve native observation budget')
        self.provider=provider;self.minimum_margin_us=minimum_margin_us

    def check(self,descriptor,context):
        if not isinstance(descriptor,Descriptor):raise RetentionError('Original admitted descriptor required')
        report=descriptor.validation_report.get('retention')
        if not isinstance(report,Mapping) or _micros(report.get('margin_us'))<self.minimum_margin_us:
            raise RetentionError('Original retention margin below current host budget')
        observed=self.provider.observe(descriptor,context)
        if not isinstance(observed,Mapping) or set(observed)!={'configurations','table_uuids','now_us'}:
            raise RetentionError('Complete fresh native retention observation required')
        validate_publication_retention(descriptor,observed['configurations'],now_us=observed['now_us'],table_uuids=observed['table_uuids'])


class RetentionNativePolicy:
    """NativeBackend policy wrapper; original custody and snapshots stay mandatory."""
    def __init__(self,policy,gate):self.policy=policy;self.gate=gate
    def authorize(self,context,publication_id,tables):
        return self.policy.authorize(context,publication_id,tables)
    def validate_snapshot(self,table,uuid,version,columns):
        return self.policy.validate_snapshot(table,uuid,version,columns)
    def validate_descriptor(self,descriptor,context):
        if self.policy.validate_descriptor(descriptor,context) is not None:
            raise RetentionError('Independent descriptor custody admission incomplete')
        self.gate.check(descriptor,context)


class RetentionManifestPolicy:
    """DeltaManifestStore wrapper; does not replace writer/source/effect admission."""
    def __init__(self,policy,gate):self.policy=policy;self.gate=gate
    def writer(self,table,uuid,context):return self.policy.writer(table,uuid,context)
    def admit(self,row,context):
        original=dict(row)
        if self.policy.admit(row,context) is not None:
            raise RetentionError('Independent publication admission incomplete')
        if dict(row)!=original:raise RetentionError('Admission changed original manifest bytes')
        from .manifest import validate_manifest_row
        validate_manifest_row(original)
        descriptor=Descriptor(original['publication_id'],original['profile_version'],
            _freeze(_decode(original['table_versions_json'])),_freeze(_decode(original['schema_revisions_json'])),
            _freeze(_decode(original['source_progress_json'])),_freeze(_decode(original['validation_report_json'])),_freeze(original))
        self.gate.check(descriptor,context)
