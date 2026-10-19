# SPDX-License-Identifier: MIT OR Apache-2.0
"""Small SQLite-only lineage controls; no native/source authority claim."""
import copy,json,sqlite3,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.medical_journal_inspection import journal_snapshot,require_journal_prefix,MedicalJournalInspection,validate_medical_journal_observations,OBSERVATION_PROFILE,SCOPE
from ashlar_host.medical_journal_inspection_configuration import parse_medical_journal_lineage,LINEAGE_PROFILE,STORAGE_PROFILE,TRANSPORT_HASHES,CODE_HASHES
from ashlar_host.delta_custody import local_journal_definitions

class JournalInspectionTests(unittest.TestCase):
    def test_complete_schema_snapshot_and_monotonic_original_bytes(self):
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory)/'original.sqlite';database=sqlite3.connect(path)
            for sql in local_journal_definitions().values():database.execute(sql)
            database.execute('INSERT INTO local_installation VALUES(1,?)',('registry',))
            database.execute('INSERT INTO local_operation VALUES(?,?,?,?,?,?)',('operation','intent','sha',1,'submitted',None));database.commit()
            before=journal_snapshot(path,maximum_bytes=1048576,maximum_rows=100,immutable=True)
            database.execute("UPDATE local_operation SET state='committed',receipt='original receipt'");database.commit()
            after=journal_snapshot(path,maximum_bytes=1048576,maximum_rows=100,immutable=True)
            require_journal_prefix(before,after)
            for mutate in ('receipt','missing','intent','state'):
                changed=copy.deepcopy(after)
                if mutate=='missing':changed['local_operation'].clear()
                else:
                    row=list(changed['local_operation']['operation']);row[{'receipt':5,'intent':1,'state':4}[mutate]]={'receipt':'other receipt','intent':'other intent','state':'prepared'}[mutate];changed['local_operation']['operation']=tuple(row)
                with self.assertRaises(PermissionError):require_journal_prefix(after,changed)
            database.execute('CREATE TABLE unexpected(value)');database.commit()
            with self.assertRaises(PermissionError):journal_snapshot(path,maximum_bytes=1048576,maximum_rows=100,immutable=True)
            database.close()

    def test_closed_lineage_code_and_aggregate_snapshot_bound(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve()
            def pin(name,digest='a'*64,size=1):return {'path':str(root/name),'bytes':size,'sha256':digest}
            value={'profile':LINEAGE_PROFILE,'installation_id':'00000000-0000-4000-8000-000000000001','request_digest':'a'*64,'installation':pin('installation'),
                'storage_profile':STORAGE_PROFILE,'snapshots':[pin('before'),pin('after')],
                'executions':[{'configuration':pin('configuration'),'terminal':pin('terminal'),'launcher':None,'code':{'transport':pin('transport',next(iter(TRANSPORT_HASHES))),**{name:pin(name,digest)for name,digest in CODE_HASHES.items()}}}]}
            selected=parse_medical_journal_lineage(json.dumps(value).encode());self.assertEqual(len(selected.snapshots),2)
            bad=copy.deepcopy(value);bad['executions'][0]['code']['transport']['sha256']='0'*64
            with self.assertRaises(PermissionError):parse_medical_journal_lineage(json.dumps(bad).encode())
            bad=copy.deepcopy(value);bad['snapshots'][0]['bytes']=128*1024*1024
            with self.assertRaises(ValueError):parse_medical_journal_lineage(json.dumps(bad).encode())
            with self.assertRaises(ValueError):parse_medical_journal_lineage(json.dumps({**value,'historical_inode':1}).encode())

    def test_current_interval_retains_new_operation_prefix(self):
        owner=object.__new__(MedicalJournalInspection)
        baseline={table:{}for table in local_journal_definitions()}
        added=copy.deepcopy(baseline);added['local_operation']['new']=('new','intent','sha',1,'submitted',None)
        owner.baseline=baseline;owner._last_current=baseline;owner.lease=object();owner.journal_identity=(1,2)
        owner.recovery=SimpleNamespace(journal=Path('/unused'),bounds=SimpleNamespace(journal_bytes=1048576,rows=100))
        owner.lineage=SimpleNamespace(installation_id='00000000-0000-4000-8000-000000000001',request_digest='a'*64,snapshots=(1,2),executions=(1,))
        owner.observations=[];owner.renew=lambda:None;owner.validate=lambda value:None;owner.require_extension=lambda *args:None
        with patch('ashlar_host.medical_journal_inspection.journal_snapshot',side_effect=[added,baseline]):
            owner.observe('before');self.assertIs(owner._last_current,added)
            with self.assertRaises(PermissionError):owner.observe('closing')
            self.assertIs(owner._last_current,added)
        # A failed closing renewal cannot advance the retained current prefix.
        owner._last_current=baseline
        calls=[]
        def renew():
            calls.append(None)
            if len(calls)==2:raise PermissionError('closing renewal refused')
        owner.renew=renew
        with patch('ashlar_host.medical_journal_inspection.journal_snapshot',return_value=added):
            with self.assertRaises(PermissionError):owner.observe('closing')
        self.assertIs(owner._last_current,baseline)

    def test_unknown_effect_extension_and_observation_claims_refuse(self):
        from ashlar_host.delta_custody import encoded
        owner=object.__new__(MedicalJournalInspection);owner.recovery=SimpleNamespace(bounds=SimpleNamespace(journal_bytes=1048576));owner.lineage=SimpleNamespace(request_digest='a'*64)
        with self.assertRaises(PermissionError):owner.require_extension('new-graph-effect',(None,encoded({'table':'unused'})))
        observed={'profile':OBSERVATION_PROFILE,'phase':'before','installation_id':'00000000-0000-4000-8000-000000000001','request_digest':'a'*64,
            'journal_device':1,'journal_inode':2,'snapshots':2,'executions':1,'baseline_operations':7,'current_operations':7,'scope':SCOPE}
        validate_medical_journal_observations([observed])
        with self.assertRaises(ValueError):validate_medical_journal_observations([{**observed,'process_absent':True}])
        with self.assertRaises(ValueError):validate_medical_journal_observations([observed,{**observed,'journal_inode':3}])

if __name__=='__main__':unittest.main()
