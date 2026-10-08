"""Serialized immutable manifest append with mandatory independent admission."""
import re
from .native import _quoted
from .schema import _json

class ManifestError(ValueError):
    pass

FIELDS=('publication_id','profile_version','table_versions_json','source_progress_json',
        'schema_revisions_json','validation_report_json','recorded_at')


def validate_manifest_row(row):
    if set(row)!=set(FIELDS) or any(not isinstance(v,str) or not v for v in row.values()):raise ManifestError('Complete exact manifest strings required')
    versions=_json(row['table_versions_json'].encode());revisions=_json(row['schema_revisions_json'].encode())
    progress=_json(row['source_progress_json'].encode());report=_json(row['validation_report_json'].encode())
    if not isinstance(versions,dict) or not versions:raise ManifestError('Complete table version vector required')
    for table,version in versions.items():
        _quoted(table)
        if type(version) is not int or not 0<=version<2**63:raise ManifestError('Invalid exact native version')
    if not isinstance(revisions,dict) or not revisions or any(not k or not isinstance(v,str) or not v for k,v in revisions.items()):raise ManifestError('Explicit schema revision strings required')
    if not isinstance(progress,dict) or not progress or not isinstance(report,dict) or report.get('complete') is not True:raise ManifestError('Complete source/validation inventory required')
    if not re.fullmatch('0|[1-9][0-9]*',row['recorded_at']) or int(row['recorded_at'])>=2**63:raise ManifestError('Canonical UTC microsecond clock required')

class DeltaManifestStore:
    def __init__(self,executor,policy,table,uuid):
        self.executor=executor;self.policy=policy;self.table=table;self.sql_table=_quoted(table);self.uuid=uuid
        if not isinstance(uuid,str) or not uuid:raise ManifestError('Trusted manifest UUID required')
    def _identity(self):
        rows=self.executor.query('DESCRIBE DETAIL '+self.sql_table,{}).rows
        if len(rows)!=1 or rows[0].get('id')!=self.uuid:raise ManifestError('Manifest identity changed')
    def commit(self,row,*,context):
        row=dict(row);validate_manifest_row(row)
        with self.policy.writer(self.table,self.uuid,context) as permit:
            if permit is not None:raise ManifestError('Writer policy did not complete')
            self._identity()
            # Admission must independently prove original intent/effects, source,
            # schemas, exact retained pins and current authority. Not just flags.
            if self.policy.admit(row,context) is not None:raise ManifestError('Publication admission incomplete')
            definition='STRUCT<'+','.join(k+':STRING' for k in FIELDS)+'>'
            fields=','.join(FIELDS)
            expression=','.join('timestamp_micros(cast(r.recorded_at AS BIGINT)) AS recorded_at' if k=='recorded_at' else 'r.'+k for k in FIELDS)
            same=' AND '.join('t.'+k+' IS NOT DISTINCT FROM s.'+k for k in FIELDS)
            sql='MERGE INTO '+self.sql_table+" t USING (SELECT "+expression+" FROM (SELECT from_json(:row,'"+definition+"') r)) s ON t.publication_id=s.publication_id WHEN MATCHED AND NOT ("+same+") THEN UPDATE SET validation_report_json=cast(raise_error('IMMUTABLE_PUBLICATION_CONFLICT') AS STRING) WHEN NOT MATCHED THEN INSERT ("+fields+') VALUES ('+','.join('s.'+k for k in FIELDS)+')'
            import json
            self.executor.query(sql,{'row':json.dumps(row,separators=(',',':'))})
            select=','.join('cast(unix_micros(recorded_at) AS STRING) AS recorded_at' if k=='recorded_at' else k for k in FIELDS)
            actual=self.executor.query('SELECT '+select+' FROM '+self.sql_table+' WHERE publication_id=:id',{'id':row['publication_id']}).rows
            if len(actual)!=1 or dict(actual[0])!=row:raise ManifestError('Ambiguous or mismatched original manifest')
            self._identity()
        return row


def manifest_pin_vector(row, table_uuids, *, authority):
    """Bind the complete original manifest to independently admitted UUIDs.

    This is correspondence, not retention or authorization admission. The caller
    must establish original native manifest custody and trusted table identities
    before registering this vector through PostgresPins' mandatory policy.
    Original JSON strings are hashed without semantic normalization.
    """
    import hashlib, json
    from .pins import PinVector
    row=dict(row)
    validate_manifest_row(row)
    versions=_json(row['table_versions_json'].encode('utf-8'))
    uuids=dict(table_uuids)
    if set(uuids)!=set(versions):raise ManifestError('Complete independently admitted UUID inventory required')
    original=json.dumps(row,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode('utf-8')
    digest=hashlib.sha256(original).hexdigest()
    return PinVector(authority,'manifest',row['publication_id'],digest,
                     {table:(uuids[table],version) for table,version in versions.items()})


def bind_manifest_pins(descriptor, vector, table_uuids, *, authority):
    """Check resolver descriptor and all held pins against original manifest bytes.

    Use from policy.bind_descriptor after independently authenticating admission.
    It provides no permissive read policy and does not inspect retained files.
    """
    from .publication import Descriptor, _decode, _freeze
    from .pins import PinVector
    if not isinstance(descriptor,Descriptor) or not isinstance(vector,PinVector):
        raise ManifestError('Resolved descriptor and original pin vector required')
    row=dict(descriptor.raw)
    expected=manifest_pin_vector(row,table_uuids,authority=authority)
    if (descriptor.publication_id!=row['publication_id'] or descriptor.profile!=row['profile_version']
        or descriptor.versions!=_freeze(_decode(row['table_versions_json']))
        or descriptor.revisions!=_freeze(_decode(row['schema_revisions_json']))
        or descriptor.source_progress!=_freeze(_decode(row['source_progress_json']))
        or descriptor.validation_report!=_freeze(_decode(row['validation_report_json']))):
        raise ManifestError('Resolved descriptor differs from original manifest')
    if vector!=expected:raise ManifestError('Held pins differ from complete original manifest custody')
