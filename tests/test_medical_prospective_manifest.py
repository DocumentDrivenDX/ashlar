"""Pure prospective-intent lifecycle controls; no native/source authority claim."""
import hashlib,json,tempfile,unittest,os,sqlite3
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from unittest.mock import patch
from ashlar_host.medical_prospective_manifest import MedicalProspectiveManifest,retain_prospective_authorization,require_prospective_authorization,original_inspection_facts
from ashlar_host.medical_prospective_manifest_configuration import decode_authorization,AUTHORIZATION_PROFILE

class ProspectiveManifestTests(unittest.TestCase):
    def test_authorization_closed_and_private_durable_selection(self):
        value={'profile':AUTHORIZATION_PROFILE,'authorization_id':'00000000-0000-4000-8000-000000000001','installation_id':'00000000-0000-4000-8000-000000000002','manifest_uuid':'00000000-0000-4000-8000-000000000003','anchor_version':1}
        for name in ('request_digest','applied_artifact_sha256','proposal_sha256','inspection_sha256','inspection_configuration_sha256'):value[name]='a'*64
        raw=json.dumps(value).encode();self.assertEqual(decode_authorization(raw),value)
        for changed in ({**value,'anchor_version':True},{**value,'extra':1}):
            with self.assertRaises(ValueError):decode_authorization(json.dumps(changed).encode())
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory)/'auth';retain_prospective_authorization(path,raw)
            retain_prospective_authorization(path,raw);require_prospective_authorization(path,raw)
            self.assertEqual(path.stat().st_mode&0o777,0o600)
            with patch('ashlar_host.medical_prospective_manifest.os.fsync',wraps=os.fsync)as synced:
                retain_prospective_authorization(path,raw)
                self.assertEqual(synced.call_count,2)
            with self.assertRaises(PermissionError):retain_prospective_authorization(path,raw+b' ')
            self.assertEqual(path.read_bytes(),raw)

    def test_installed_inspection_process_observations_are_separate(self):
        from ashlar_host.medical_process_inspection import PROCESS_SCOPE
        observed={'mapped_vnode_fallbacks':[],'terminal_local_processes':[],'observed_external_original_executors':0,'inspector_java':[123],'physically_distinct_java':[],
            'owner_uid':os.getuid(),'visibility_profile':'ashlar-private-local-same-uid-process-visibility/0.1',
            'other_uid_processes_excluded':0,'scope':PROCESS_SCOPE}
        self.assertEqual(original_inspection_facts({'classification':'observed-absent-at-anchor','process_observations':[observed]}),{'classification':'observed-absent-at-anchor'})
        with self.assertRaises(ValueError):original_inspection_facts({'process_observations':[{**observed,'observed_external_original_executors':1}]})

    def test_closed_configuration_nominal_constructor_and_artifact_guard(self):
        from ashlar_host.medical_prospective_manifest_configuration import parse_original_medical_prospective_manifest,OriginalMedicalProspectiveManifestConfig,PROFILE
        from ashlar_host.original_medical_local_configuration import MedicalPinnedFile
        from ashlar_host.delta_custody import LocalDeltaTransport,encoded
        from ashlar_host.driver import NativeDriver
        from ashlar_host.medical_publication_plan import MedicalPublicationPlan
        from ashlar_host.medical_publication_sources import OriginalMedicalSourceOwner
        from ashlar_host.medical_process_inspection import MedicalProcessInspection
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();installation={'installation_id':'00000000-0000-4000-8000-000000000002'}
            def pin(name,raw):
                path=root/name;path.write_bytes(raw)
                return {'path':str(path),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
            request={'request_digest':'a'*64,'schema_revisions_json':'{"x":"1"}'}
            row={'publication_id':'publication','profile_version':'ashlar-delta/0.3','table_versions_json':'{"c.s.object_current":0}',
                'source_progress_json':'{"x":{}}','schema_revisions_json':'{"x":"1"}',
                'validation_report_json':encoded({'complete':True,'request_digest':'a'*64}),'recorded_at':'1'}
            artifact=encoded({'effects':{},'manifest':row}).encode();proposal=(encoded(row)+'\n').encode()
            uuid='00000000-0000-4000-8000-000000000003'
            inspection={'classification':'observed-absent-at-anchor','installation_id':installation['installation_id'],'request_digest':'a'*64,
                'proposal_sha256':hashlib.sha256(proposal).hexdigest(),'manifest':{'uuid':uuid}}
            selected={'profile':PROFILE,'inspection_configuration':pin('inspection-config',b'{}'),'inspection':pin('inspection',encoded(inspection).encode()),'proposal':pin('proposal',proposal),'output':str(root/'fresh')}
            authorization={'profile':AUTHORIZATION_PROFILE,'authorization_id':'00000000-0000-4000-8000-000000000001',
                'installation_id':installation['installation_id'],'manifest_uuid':uuid,'anchor_version':1,'request_digest':'a'*64,
                'applied_artifact_sha256':hashlib.sha256(artifact).hexdigest(),'proposal_sha256':selected['proposal']['sha256'],
                'inspection_sha256':selected['inspection']['sha256'],'inspection_configuration_sha256':selected['inspection_configuration']['sha256']}
            selected['authorization']=pin('authorization',encoded(authorization).encode())
            config=parse_original_medical_prospective_manifest(encoded(selected).encode())
            with self.assertRaises(ValueError):parse_original_medical_prospective_manifest(encoded({**selected,'grant':True}).encode())
            ip=pin('installation',encoded(installation).encode());installation_pin=MedicalPinnedFile(Path(ip['path']),ip['sha256'],ip['bytes'])
            selection=SimpleNamespace(recovery=SimpleNamespace(installation=installation_pin))
            raw=tuple(Path(selected[name]['path']).read_bytes()for name in ('inspection_configuration','inspection','proposal','authorization'))
            database=sqlite3.connect(':memory:');self.addCleanup(database.close)
            database.execute('CREATE TABLE local_publication_artifact(request_digest,request,artifact)')
            database.execute('INSERT INTO local_publication_artifact VALUES(?,?,?)',('a'*64,encoded(request),artifact.decode()))
            transport=object.__new__(LocalDeltaTransport);transport.db=database;transport.journal_path=root/'operations.sqlite';transport.installation_id=installation['installation_id'];transport.targets={'manifest':SimpleNamespace(table='c.s.manifest',uuid=uuid)}
            driver=object.__new__(NativeDriver);driver.transport=transport;driver.tables={'manifest':'manifest'};driver.policy=SimpleNamespace(active={});driver.require=lambda context:None;driver.source_admit=lambda request:None
            validated=[];driver.validate=lambda *args:validated.append(args)
            source=object.__new__(OriginalMedicalSourceOwner);source.driver=driver;source.lease=SimpleNamespace(renew=lambda context:None)
            observer=object.__new__(MedicalProcessInspection);observer.config=selection;observer.closing=lambda:None
            plan=object.__new__(MedicalPublicationPlan);context=object()
            with patch.object(OriginalMedicalProspectiveManifestConfig,'inspection_selection',return_value=selection),patch('ashlar_host.medical_prospective_manifest.prospective_selection_bytes',return_value=raw),patch('ashlar_host.medical_prospective_manifest.renew_prospective_selection'),patch.object(MedicalPublicationPlan,'snapshot',return_value={'request':request}),patch.object(MedicalPublicationPlan,'require_request'):
                owner=MedicalProspectiveManifest(driver,plan,source,observer,context=context,bounds=SimpleNamespace(input_bytes=4194304),installation=installation,configuration=config)
                owner.require(context);self.assertEqual(len(validated),1)
                database.execute('UPDATE local_publication_artifact SET artifact=?',(artifact.decode()+' ',))
                with self.assertRaises(PermissionError):owner.require(context)
                self.assertEqual(len(validated),1)
                with self.assertRaises(ValueError):MedicalProspectiveManifest(object(),plan,source,observer,context=context,bounds=object(),installation=installation,configuration=config)

    def owner(self,directory,events,*,retained=None):
        owner=object.__new__(MedicalProspectiveManifest);owner.context=object();owner.proposal={'original':'row'}
        owner.original=(b'config',b'inspection',b'proposal',b'authorized');owner.inspection={'observed':True};owner.inspection_facts=owner.inspection
        owner.sidecar=Path(directory)/'auth';owner.request={'request_digest':'a'*64};owner.operation_prefix='new';owner.bounds=object();owner.installation={};owner.plan=object();owner.source=object()
        state=[retained]
        @contextmanager
        def writer(*args):yield
        def require(context):
            self.assertIs(context,owner.context);events.append('guard')
        owner.require=require
        owner.retained=lambda *args:state[0]
        def prepare(*args,**kwargs):
            require_prospective_authorization(owner.sidecar,owner.original[3]);events.append('prepare');state[0]=('new-child','intent','sha',1,'committed',None)
        owner.store=SimpleNamespace(statement=lambda row:('EXACT SQL',{'original':'parameter'}),prepare=prepare,policy=object(),table='manifest',uuid='uuid')
        owner.target=SimpleNamespace(uuid='uuid');owner.executor=object()
        owner.driver=SimpleNamespace(writer=writer,transport=SimpleNamespace(db=SimpleNamespace(execute=lambda *args:SimpleNamespace(fetchone=lambda:None)),inspect_committed=lambda *args,**kwargs:{'uuid':'uuid','before_version':1,'version':2,'snapshot':{'rows':[owner.proposal]}}))
        return owner

    def test_new_authorization_precedes_prepare_and_same_intent_resumes(self):
        events=[]
        with tempfile.TemporaryDirectory()as directory:
            owner=self.owner(directory,events)
            def commit(row,**kwargs):events.append('recover');return row
            with patch('ashlar_host.medical_prospective_manifest.inspect_medical_commit',return_value=(owner.inspection,b'proposal'))as inspect,patch('ashlar_host.medical_prospective_manifest.RecoveryExecutor',side_effect=lambda x:x),patch('ashlar_host.medical_prospective_manifest.DeltaManifestStore',return_value=SimpleNamespace(commit=commit)):
                self.assertEqual(owner.reconcile(owner.proposal,context=owner.context),owner.proposal)
                self.assertEqual(events.count('prepare'),1);self.assertEqual(inspect.call_count,1)
                self.assertEqual(owner.reconcile(owner.proposal,context=owner.context),owner.proposal)
                self.assertEqual(events.count('prepare'),1);self.assertEqual(inspect.call_count,1)
            self.assertLess(events.index('prepare'),events.index('recover'))

    def test_other_authorization_intent_refuses_even_without_sidecar(self):
        with tempfile.TemporaryDirectory()as directory:
            owner=self.owner(directory,[]);del owner.retained
            database=sqlite3.connect(':memory:');self.addCleanup(database.close)
            database.execute('CREATE TABLE local_operation(operation,intent,intent_sha,before_version,state,receipt)')
            database.execute('INSERT INTO local_operation VALUES(?,?,?,?,?,?)',('reconciled-manifest:'+owner.request['request_digest']+':other:child','original','sha',1,'submitted',None))
            owner.driver.transport.db=database;owner.driver.transport.installation_id='installation';owner.driver.transport.operation_capacity=None
            owner.target.table='manifest';owner.target.path=Path(directory)/'manifest'
            with patch('ashlar_host.medical_prospective_manifest.inspect_medical_commit')as inspect,patch('ashlar_host.medical_prospective_manifest.DeltaManifestStore')as store:
                with self.assertRaises(PermissionError):owner.reconcile(owner.proposal,context=owner.context)
                inspect.assert_not_called();store.assert_not_called()

    def test_uncertain_primary_preserved_and_missing_authorization_refuses_resume(self):
        events=[]
        with tempfile.TemporaryDirectory()as directory:
            owner=self.owner(directory,events)
            failure=OSError('uncertain response')
            with patch('ashlar_host.medical_prospective_manifest.inspect_medical_commit',return_value=(owner.inspection,b'proposal')),patch('ashlar_host.medical_prospective_manifest.RecoveryExecutor',side_effect=lambda x:x),patch('ashlar_host.medical_prospective_manifest.DeltaManifestStore',return_value=SimpleNamespace(commit=lambda *args,**kwargs:(_ for _ in ()).throw(failure))):
                with self.assertRaises(OSError)as caught:owner.reconcile(owner.proposal,context=owner.context)
                self.assertIs(caught.exception,failure);self.assertEqual(events[-1],'guard')
            owner.sidecar.unlink()
            with patch('ashlar_host.medical_prospective_manifest.inspect_medical_commit')as inspect,patch('ashlar_host.medical_prospective_manifest.DeltaManifestStore')as store:
                with self.assertRaises(FileNotFoundError):owner.reconcile(owner.proposal,context=owner.context)
                inspect.assert_not_called();store.assert_not_called()

if __name__=='__main__':unittest.main()
