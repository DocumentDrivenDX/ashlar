"""Immutable raw UMF intake registry; executable schema acceptance is separate."""
import json
from .native import _quoted
from .schema import SchemaIntake,SchemaIntakeError

class DeltaSchemaRegistry:
    def __init__(self,executor,policy,table,uuid):
        self.executor=executor;self.policy=policy;self.table=table;self.sql_table=_quoted(table);self.uuid=uuid
        if not isinstance(uuid,str) or not uuid:raise SchemaIntakeError('Trusted schema registry UUID required')
    def _identity(self):
        rows=self.executor.query('DESCRIBE DETAIL '+self.sql_table,{}).rows
        if len(rows)!=1 or rows[0].get('id')!=self.uuid:raise SchemaIntakeError('Schema registry replaced')
    def register(self,artifact,revision,*,trusted_validator_revision,context):
        intake=SchemaIntake.read(artifact,revision,trusted_validator_revision=trusted_validator_revision)
        row=intake.row();fields=list(row)
        with self.policy.writer(self.table,self.uuid,context) as permit:
            if permit is not None:raise SchemaIntakeError('Intake writer authority incomplete')
            self._identity()
            definition='STRUCT<'+','.join(k+':'+('BOOLEAN' if k=='complete_interpretation' else 'STRING') for k in fields)+'>'
            same=' AND '.join('t.'+k+' IS NOT DISTINCT FROM s.'+k for k in fields)
            sql='MERGE INTO '+self.sql_table+" t USING (SELECT r.* FROM (SELECT from_json(:row,'"+definition+"') r)) s ON t.document_id=s.document_id AND t.document_revision=s.document_revision WHEN MATCHED AND NOT ("+same+") THEN UPDATE SET artifact_base64=cast(raise_error('SCHEMA_INTAKE_CONFLICT') AS STRING) WHEN NOT MATCHED THEN INSERT *"
            self.executor.query(sql,{'row':json.dumps(row,separators=(',',':'))})
            projection=','.join('cast(complete_interpretation AS STRING) AS complete_interpretation' if k=='complete_interpretation' else k for k in fields)
            actual=self.executor.query('SELECT '+projection+' FROM '+self.sql_table+' WHERE document_id=:id AND document_revision=:revision',{'id':intake.document_id,'revision':revision}).rows
            expected=dict(row,complete_interpretation=str(row['complete_interpretation']).lower())
            if len(actual)!=1 or dict(actual[0])!=expected:raise SchemaIntakeError('Original raw intake readback mismatch')
            self._identity()
        return intake
