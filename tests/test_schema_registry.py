from contextlib import contextmanager
from pathlib import Path
import json,unittest
from ashlar.schema_registry import DeltaSchemaRegistry
from ashlar.schema import SchemaIntakeError
from ashlar.native import SQLResult
ROOT=Path(__file__).resolve().parents[1];PIN='16c35e8d943769ccfa7bb57d16785aa7159abe65'
class Policy:
    @contextmanager
    def writer(self,t,u,c):
        if c!='trusted':raise PermissionError('Denied')
        yield
class Executor:
    def __init__(self):self.row=None;self.calls=[]
    def query(self,sql,p):
        self.calls.append(sql)
        if sql.startswith('DESCRIBE'):return SQLResult([{'id':'u'}])
        if sql.startswith('MERGE'):
            row=json.loads(p['row'])
            if self.row is not None and row!=self.row:raise SchemaIntakeError('SCHEMA_INTAKE_CONFLICT')
            self.row=row;return SQLResult([])
        return SQLResult([dict(self.row,complete_interpretation=str(self.row['complete_interpretation']).lower())])
class RegistryTests(unittest.TestCase):
    def artifact(self,v='v1'):return (ROOT/('examples/end-to-end/schema-'+v+'.intake.json')).read_bytes()
    def test_original_receipt_and_replay_preserve_unknown_state(self):
        e=Executor();r=DeltaSchemaRegistry(e,Policy(),'c.s.t','u')
        x=r.register(self.artifact(),'1',trusted_validator_revision=PIN,context='trusted')
        self.assertFalse(x.complete_interpretation)
        self.assertEqual(r.register(self.artifact(),'1',trusted_validator_revision=PIN,context='trusted'),x)
        with self.assertRaises(SchemaIntakeError):r.register(self.artifact('v3'),'1',trusted_validator_revision=PIN,context='trusted')
        self.assertEqual(e.row['source_sha256'],x.source_sha256)
    def test_denial_precedes_registry_effect(self):
        e=Executor()
        with self.assertRaises(PermissionError):DeltaSchemaRegistry(e,Policy(),'c.s.t','u').register(self.artifact(),'1',trusted_validator_revision=PIN,context='denied')
        self.assertEqual(e.calls,[])
if __name__=='__main__':unittest.main()
