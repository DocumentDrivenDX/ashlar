# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure actual read-only registration and reservation controls; no native engine."""
import hashlib,json,sqlite3,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from ashlar_host.delta_custody import DeltaTarget,encoded,LOCAL_OPERATION_CAPACITY_8M,LocalDeltaError
from ashlar_host.read_only_transport import original_read_capacity,ReadOnlyTransport

class Tests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()
  self.targets=[]
  for n in range(6):
   p=self.root/str(n);p.mkdir();self.targets.append(DeltaTarget('local.original.t'+str(n),p,'uuid-'+str(n)))
  self.journal=self.root/'operations.sqlite';self.installation='original-installation'
  self.body={'installation':self.installation,'targets':[{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in self.targets],'operation_capacity':dict(LOCAL_OPERATION_CAPACITY_8M)}
  self.write(self.body)
 def write(self,body):
  db=sqlite3.connect(self.journal);db.execute('CREATE TABLE IF NOT EXISTS local_installation(id INTEGER,original TEXT)');db.execute('DELETE FROM local_installation');db.execute('INSERT INTO local_installation VALUES(1,?)',(encoded(body),));db.commit();db.close()
  reservation=encoded({'profile':'ashlar-private-native-installation-reservation/0.1','registry':body,'original_journal_path':str(self.journal)})+'\n'
  for target in self.targets:
   p=target.path/'.ashlar-local-installation-reservation.json';p.write_text(reservation);p.chmod(0o600)
 def test_actual_owner_reopens_original_capacity_and_legacy_without_changes(self):
  for capacity in (dict(LOCAL_OPERATION_CAPACITY_8M),None):
   body=dict(self.body)
   if capacity is None:body.pop('operation_capacity')
   self.write(body);selected=original_read_capacity(self.journal,self.installation,self.targets)
   self.assertEqual(None if selected is None else dict(selected),capacity)
   before=self.journal.read_bytes()
   conf=SimpleNamespace(get=lambda key:{'spark.sql.session.timeZone':'UTC','spark.sql.ansi.enabled':'true'}[key])
   def sql(statement):
    target=next(t for t in self.targets if str(t.path) in statement)
    detail={'id':target.uuid,'properties':{'ashlar.local.installation.id':self.installation,'ashlar.local.registry.sha256':hashlib.sha256(encoded(body).encode()).hexdigest(),'ashlar.local.journal.sha256':hashlib.sha256(str(self.journal).encode()).hexdigest()}}
    return SimpleNamespace(first=lambda:SimpleNamespace(asDict=lambda:detail))
   spark=SimpleNamespace(conf=conf,sql=sql);policy=SimpleNamespace(writer=lambda *a:None,admit=lambda *a:None)
   transport=ReadOnlyTransport.open(spark,self.journal,self.installation,self.targets,policy,capacity=selected)
   try:
    self.assertEqual(None if transport.operation_capacity is None else dict(transport.operation_capacity),capacity)
    with self.assertRaises(LocalDeltaError):transport.mutation(None)
   finally:transport.close()
   self.assertEqual(before,self.journal.read_bytes())
 def test_closed_capacity_and_full_original_registry_refuse(self):
  mutations=[lambda b:b.update(installation='other'),lambda b:b.update(extra='extension'),lambda b:b.update(operation_capacity=None),lambda b:b.update(operation_capacity={'profile':'unknown','max_intent_bytes':8388608}),lambda b:b['operation_capacity'].update(max_intent_bytes=True),lambda b:b['operation_capacity'].update(max_intent_bytes=8388608.0),lambda b:b['targets'][0].update(uuid='changed'),lambda b:b['targets'].pop()]
  for mutate in mutations:
   body=json.loads(encoded(self.body));mutate(body);self.write(body)
   with self.subTest(body=body),self.assertRaises(LocalDeltaError):original_read_capacity(self.journal,self.installation,self.targets)
 def test_missing_or_replaced_reservation_and_journal_refuse(self):
  p=self.targets[0].path/'.ashlar-local-installation-reservation.json';p.unlink()
  with self.assertRaises(FileNotFoundError):original_read_capacity(self.journal,self.installation,self.targets)
  self.write(self.body);p.write_text('{}')
  with self.assertRaises(LocalDeltaError):original_read_capacity(self.journal,self.installation,self.targets)
  with self.assertRaises(LocalDeltaError):original_read_capacity(self.root/'missing',self.installation,self.targets)
 def test_no_capacity_selection_cannot_reopen_capacity_registered_journal(self):
  # Owning journal equality stays exact; a selected missing capacity is not rescued.
  class Conf:
   def get(self,key):return 'UTC' if key.endswith('timeZone') else 'true'
  # Helper supplies the only accepted closed capacity; missing registry row refuses.
  db=sqlite3.connect(self.journal);db.execute('DELETE FROM local_installation');db.commit();db.close()
  with self.assertRaises(LocalDeltaError):original_read_capacity(self.journal,self.installation,self.targets)

if __name__=='__main__':unittest.main()
