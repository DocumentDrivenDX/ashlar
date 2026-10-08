"""Finite publication readability from verified native data/log retention.

Predictive optimization may remain enabled. Fresh native settings, snapshot
commit timestamps and real snapshot availability require independent host
admission; this arithmetic does not promise files exist or fence maintenance.
"""
import re
from collections.abc import Mapping
from .publication import _name
class RetentionError(ValueError):pass


def interval_microseconds(text):
    """Selected fixed-duration native interval subset; no calendar guesses."""
    if not isinstance(text,str):raise RetentionError('Explicit verified retention interval required')
    match=re.fullmatch(r'(?:interval )?([1-9][0-9]{0,9}) (seconds?|minutes?|hours?|days?|weeks?)',text.strip().lower())
    if not match:raise RetentionError('Unsupported retention interval')
    units={'second':1,'minute':60,'hour':3600,'day':86400,'week':604800}
    result=int(match[1])*units[match[2].rstrip('s')]*1000000
    if result>=2**63:raise RetentionError('Retention interval outside signed64')
    return result


def _micros(value):
    if not isinstance(value,str) or not value.isascii() or not value.isdecimal() or len(value)>19 or str(int(value))!=value or int(value)>=2**63:raise RetentionError('Canonical native microsecond text required')
    return int(value)


def _duration(configuration):
    if not isinstance(configuration,Mapping) or set(configuration)!={'data_retention','log_retention'}:raise RetentionError('Verified data and log retention configuration required')
    return min(interval_microseconds(configuration['data_retention']),interval_microseconds(configuration['log_retention']))


def publication_retention_report(snapshots,configurations,*,margin_us):
    """Immutable expiry ceiling anchored to each original native snapshot commit.

    snapshots: table -> {uuid, version, committed_at}, all independently verified.
    The margin reserves read/clock/configuration observation budget. Never anchor
    old snapshots to a new publication clock or extend expiry on replay.
    """
    if type(margin_us) is not int or margin_us<0:raise RetentionError('Explicit nonnegative retention margin required')
    if not isinstance(snapshots,Mapping) or not 1<=len(snapshots)<=128 or set(snapshots)!=set(configurations):raise RetentionError('Complete bounded retention vector required')
    targets={}
    for table,snapshot in snapshots.items():
        _name(table)
        if not isinstance(snapshot,Mapping) or set(snapshot)!={'uuid','version','committed_at'} or not isinstance(snapshot['uuid'],str) or not snapshot['uuid'] or type(snapshot['version']) is not int or not 0<=snapshot['version']<2**63:raise RetentionError('Exact original snapshot identity required')
        duration=_duration(configurations[table]);commit=_micros(snapshot['committed_at'])
        if duration<=margin_us or commit+duration>=2**63:raise RetentionError('No usable retention window')
        targets[table]=dict(snapshot,readable_until=str(commit+duration-margin_us))
    return {'profile':'ashlar-retention-window/0.1','margin_us':str(margin_us),'targets':targets}


def validate_publication_retention(descriptor,configurations,*,now_us,table_uuids):
    """Refuse expired/unknown retention before AND after native consumption.

    Use in mandatory validate_descriptor admission; native files/logs, schema,
    protocol, current permission and identity remain independent checks. Current
    shorter settings tighten the original expiry; longer settings never extend
    it. Expiry does not delete history, release pins or acknowledge a source.
    """
    report=descriptor.validation_report.get('retention')
    if not isinstance(report,Mapping) or set(report)!={'profile','margin_us','targets'} or report['profile']!='ashlar-retention-window/0.1':raise RetentionError('Explicit publication retention profile required')
    now=_micros(now_us);margin=_micros(report['margin_us']);targets=report['targets']
    if not isinstance(targets,Mapping) or not 1<=len(targets)<=128 or set(targets)!=set(descriptor.versions) or set(configurations)!=set(targets) or set(table_uuids)!=set(targets):raise RetentionError('Complete current retention configuration required')
    deadline=None
    for table,target in targets.items():
        if not isinstance(target,Mapping) or set(target)!={'uuid','version','committed_at','readable_until'} or not isinstance(target['uuid'],str) or not target['uuid'] or type(target['version']) is not int or target['version']!=descriptor.versions[table] or target['uuid']!=table_uuids[table]:raise RetentionError('Retention snapshot differs from original vector')
        commit=_micros(target['committed_at']);original=_micros(target['readable_until']);duration=_duration(configurations[table])
        if commit>now or original<=commit or duration<=margin:raise RetentionError('Invalid or unusable native retention window')
        effective=min(original,commit+duration-margin)
        if now>=effective:raise RetentionError('Publication snapshot retention window expired')
        deadline=effective if deadline is None else min(deadline,effective)
    return str(deadline)


def observe_retention_configuration(executor,table,uuid,*,defaults,default_profile):
    """Read exact UUID/properties with explicitly qualified native defaults.

    The host admits platform defaults/profile, authenticated executor and target.
    Original property rows are returned; an absent override is distinguished from
    an explicit value. This observation is not a lock or file-availability proof.
    """
    from .native import _quoted
    if not isinstance(default_profile,str) or not default_profile or not isinstance(uuid,str) or not uuid:raise RetentionError('Trusted default profile and target identity required')
    _duration(defaults);quoted=_quoted(table)
    def identity():
        rows=executor.query('DESCRIBE DETAIL '+quoted,{}).rows
        if len(rows)!=1 or rows[0].get('id')!=uuid:raise RetentionError('Retention target identity changed')
    identity()
    rows=executor.query('SHOW TBLPROPERTIES '+quoted,{}).rows
    if len(rows)>1024:raise RetentionError('Retention property observation exceeds bound')
    properties={}
    for row in rows:
        if set(row)!={'key','value'} or not isinstance(row['key'],str) or not isinstance(row['value'],str) or row['key'] in properties:raise RetentionError('Incomplete or ambiguous native properties')
        properties[row['key']]=row['value']
    configuration={}
    sources={}
    for name,key in [('data_retention','delta.deletedFileRetentionDuration'),('log_retention','delta.logRetentionDuration')]:
        configuration[name]=properties.get(key,defaults[name])
        sources[name]='explicit-property' if key in properties else default_profile
    _duration(configuration);identity()
    return {'table':table,'uuid':uuid,'configuration':configuration,'sources':sources,'original_properties':tuple(dict(row) for row in rows)}
