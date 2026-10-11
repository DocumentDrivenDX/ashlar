"""Selected runtime correspondence controls with inert imports, no SDK/native run."""
from dataclasses import asdict,replace
import hashlib,json,os,sys,tempfile,unittest,tarfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.config import ProducerConfig
from ashlar_host.original_medical_local_configuration import MedicalPinnedFile,OriginalMedicalInputs,OriginalMedicalRuntime,OriginalMedicalBounds
from ashlar_host.original_medical_runtime import OriginalMedicalRuntimeCustody,PROFILE

ROOT=Path(__file__).resolve().parents[1]

class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name).resolve();self.root.chmod(0o700);self.selected=[]
        def file(name,raw):
            p=self.root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw);return self.pin(p)
        self.make=file
        model=file('model.json',(ROOT/'examples/domain-packs/medical/historical/archive/schemas/ontology.json').read_bytes());graph=file('graph.json',(ROOT/'examples/domain-packs/medical/upstream/graph/fixture.json').read_bytes());baseline=file('baseline.json',(ROOT/'docs/helix/04-build/evidence/medical-historical-admission-20261009/public-receipt.json').read_bytes());self.inputs=OriginalMedicalInputs(model,graph,baseline)
        python=file('python',b'inert selected interpreter');java=file('jdk/bin/java',b'inert selected java');cfg=file('jdk/lib/jvm.cfg',b'inert jvm config');jars=(file('jars/delta-spark.jar',b'inert jar1'),file('jars/delta-storage.jar',b'inert jar2'))
        self.bun=file('bun',b'inert bun');self.git=file('git',b'inert git');(self.root/'producer').mkdir();self.producer=ProducerConfig(self.root/'producer',self.bun.path,self.git.path,20,1048576,4194304)
        self.imports=[]
        for module,distribution,version in (('pyspark','pyspark','4.0.1'),('delta','delta-spark','4.0.0')):
            origin=file('sdk/'+module+'/'+module+'/__init__.py',b'inert module '+module.encode());meta=file('sdk/'+module+'/'+distribution.replace('-','_')+'-'+version+'.dist-info/METADATA',('Name: '+distribution+'\nVersion: '+version+'\n').encode())
            self.imports.append({'module':module,'distribution':distribution,'version':version,'origin':str(origin.path),'metadata':self.value(meta)})
            self.selected.extend((origin,meta))
        self.selected.extend((python,java,cfg,*jars,self.bun,self.git))
        self.references=self.descriptors()
        with tarfile.open(ROOT/'docs/helix/04-build/evidence/supply-chain-installed-native-20261011-c/actual.tar.gz','r:gz')as archive:
            member=archive.getmember('provenance/ashlar-supply-chain-installed-independent-actual-review-c.json');evidence=archive.extractfile(member).read()
        self.evidence=file('references/evidence.json',evidence)
        qualification=file('selection.json',b'{}')
        self.runtime=OriginalMedicalRuntime(python,'3.9.24',self.root/'jdk',java,'21.0.12',cfg,self.root/'sdk/pyspark','4.0.1',self.root/'sdk/delta','4.0.0',jars,qualification)
        self.bounds=OriginalMedicalBounds(4194304,10000,1048576,16777216,1048576,1048576,8388608,300,540,'local[1]','512m')
        runtime={name:self.value(v)if type(v)is MedicalPinnedFile else str(v)if type(v)is type(Path())else[self.value(x)for x in v]if name=='jars'else v for name,v in vars(self.runtime).items()if name!='qualification'}
        self.plan={'profile':PROFILE,'runtime':runtime,'imports':self.imports,'selected_files':[self.value(p)for p in self.selected],'namespaces':self.references,'evidence':[{'kind':'spark-delta-native','record':self.value(self.evidence),'profile':'ashlar-supply-chain-installed-native-evidence/0.1','scope':'No actual runtime qualification'}],'scope':'selected-runtime-correspondence-only'}
        self.save()
    def pin(self,p):
        raw=p.read_bytes();return MedicalPinnedFile(p,hashlib.sha256(raw).hexdigest(),len(raw))
    def value(self,p):return {'path':str(p.path),'bytes':p.bytes,'sha256':p.sha256}
    def save(self):
        p=self.runtime.qualification.path;p.write_text(json.dumps(self.plan,separators=(',',':')));self.runtime=replace(self.runtime,qualification=self.pin(p))
    def descriptors(self):
        rows=[];closure=[]
        for p in self.selected:
            s=p.path.stat();rows.append({'path':str(p.path),'bytes':s.st_size,'dev':s.st_dev,'ino':s.st_ino,'mode':s.st_mode,'mtimeNs':s.st_mtime_ns});closure.append(self.value(p))
        roots={}
        for root in (self.root/'sdk',self.root/'jdk',self.root/'jars'):
            entries={}
            for p in root.rglob('*'):entries[str(p)]={'kind':'directory'}if p.is_dir()else{'kind':'file','bytes':p.stat().st_size}
            roots[str(root)]=entries
        refs=[]
        for kind,profile,value in (('namespace','ashlar-evolution-runtime-namespace-j/0.1',{'roots':roots,'links':[],'site_startups':{}}),('metadata','ashlar-evolution-runtime-metadata-j/0.1',{'files':rows,'qualification':'Inert current metadata correspondence'}),('closure','ashlar-evolution-installed-runtime-closure/0.1',{'files':closure,'format':'ashlar-evolution-installed-runtime-closure/0.1','scope':'Inert selected resources'})):
            p=self.make('references/'+kind+'.json',json.dumps(value).encode());refs.append({'kind':kind,'profile':profile,'record':self.value(p),'scope':'Inert descriptor owner fixture'})
        return refs
    def opened(self):return OriginalMedicalRuntimeCustody.open(self.runtime,self.inputs,self.bounds,self.producer)
    def test_actual_files_metadata_and_all_original_inputs_no_imports(self):
        with patch('ashlar_host.original_medical_runtime.importlib.import_module')as imported:
            with self.opened()as owner:
                self.assertEqual(owner.input_bytes[0],self.inputs.model.path.read_bytes());self.assertEqual(owner.jar_paths,tuple(p.path for p in self.runtime.jars));self.assertEqual(owner.native_environment()['JAVA_HOME'],str(self.runtime.java_home));fds=[f.fd for f in owner.files]
            imported.assert_not_called()
        for fd in fds:
            with self.assertRaises(OSError):os.fstat(fd)
    def test_selected_byte_and_namespace_drift_refuse_at_current_and_closing(self):
        with self.assertRaises((PermissionError,ValueError)):
            with self.opened()as owner:
                self.runtime.java.path.write_bytes(b'changed');owner.renew()
        self.runtime.java.path.write_bytes(b'inert selected java')
        with self.assertRaises(PermissionError):
            with self.opened()as owner:(self.root/'sdk/added.py').write_bytes(b'new namespace member')
    def test_duplicate_unknown_and_bool_pins_refuse_before_imports(self):
        for change in ('unknown','bool','delta-evidence'):
            with self.subTest(change=change):
                original=json.loads(json.dumps(self.plan))
                if change=='unknown':self.plan['unknown']=True
                elif change=='bool':self.plan['selected_files'][0]['bytes']=True
                else:self.plan['evidence'][0]['kind']='spark-guard'
                self.save()
                with patch('ashlar_host.original_medical_runtime.importlib.import_module')as imported,self.assertRaises((ValueError,PermissionError)):
                    with self.opened():self.fail('Invalid selection admitted')
                imported.assert_not_called();self.plan=original;self.save()
    def test_actual_import_observation_with_inert_modules_and_exact_metadata(self):
        calls=[]
        def module(name):
            calls.append(name);row=next(r for r in self.imports if r['module']==name);return SimpleNamespace(__file__=row['origin'],__version__=row['version'])
        with patch('ashlar_host.original_medical_runtime.sys.executable',str(self.runtime.python.path)),patch('ashlar_host.original_medical_runtime.sys.version_info',(3,9,24)),patch('ashlar_host.original_medical_runtime.importlib.import_module',side_effect=module):
            with self.opened()as owner:owner.verify_imports()
        self.assertEqual(calls,['pyspark','delta','pyspark','delta'])
    def test_darwin_closed_environment_derives_uid_ignores_ambient(self):
        with patch('ashlar_host.original_medical_runtime.sys.platform','darwin'),patch('ashlar_host.original_medical_runtime.os.getuid',return_value=501)as uid,patch.dict(os.environ,{'__CF_USER_TEXT_ENCODING':'foreign preferences','FOREIGN_NATIVE_KEY':'foreign'}):
            with self.opened()as owner:
                environment=owner.native_environment()
                self.assertEqual(environment['__CF_USER_TEXT_ENCODING'],'0x1F5:0x0:0x0')
                self.assertNotIn('FOREIGN_NATIVE_KEY',environment)
                self.assertEqual(environment['SPARK_AUTH_SOCKET_TIMEOUT'],'15')
                self.assertEqual(environment['SPARK_BUFFER_SIZE'],'65536')
                self.assertEqual(set(environment),{'PATH','JAVA_HOME','PYSPARK_PYTHON','PYSPARK_DRIVER_PYTHON','PYTHONDONTWRITEBYTECODE','LANG','LC_ALL','TZ','SPARK_AUTH_SOCKET_TIMEOUT','SPARK_BUFFER_SIZE','__CF_USER_TEXT_ENCODING'})
        uid.assert_called_once_with()
    def test_linux_closed_environment_has_no_corefoundation_key(self):
        with patch('ashlar_host.original_medical_runtime.sys.platform','linux'),patch('ashlar_host.original_medical_runtime.os.getuid')as uid,patch.dict(os.environ,{'__CF_USER_TEXT_ENCODING':'foreign'}):
            with self.opened()as owner:
                environment=owner.native_environment()
                self.assertNotIn('__CF_USER_TEXT_ENCODING',environment)
                self.assertEqual(set(environment),{'PATH','JAVA_HOME','PYSPARK_PYTHON','PYSPARK_DRIVER_PYTHON','PYTHONDONTWRITEBYTECODE','LANG','LC_ALL','TZ','SPARK_AUTH_SOCKET_TIMEOUT','SPARK_BUFFER_SIZE'})
        uid.assert_not_called()
    def test_origin_and_python_mismatch_refuse_without_native_builder(self):
        with patch('ashlar_host.original_medical_runtime.sys.executable',str(self.runtime.python.path)),patch('ashlar_host.original_medical_runtime.sys.version_info',(3,9,24)),patch('ashlar_host.original_medical_runtime.importlib.import_module',return_value=SimpleNamespace(__file__='/foreign/sdk.py',__version__='4.0.1')):
            with self.assertRaises(PermissionError):
                with self.opened()as owner:owner.verify_imports()
        with patch('ashlar_host.original_medical_runtime.importlib.import_module')as imported,self.assertRaises(PermissionError):
            with self.opened()as owner:owner.verify_imports()
        imported.assert_not_called()
    def test_body_cancellation_and_multiple_closing_failures_preserve_primary_close_all(self):
        cancel=KeyboardInterrupt('body');closed=[]
        from ashlar_host import original_medical_runtime as runtime_owner
        actual=runtime_owner._File.close
        def close(file):
            fd=file.fd;actual(file);closed.append(fd)
            if len(closed)==1:raise OSError('closing failure')
        with patch.object(runtime_owner._File,'close',close):
            try:
                with self.opened()as owner:
                    count=len(owner.files);self.runtime.java.path.write_bytes(b'drift');raise cancel
            except KeyboardInterrupt as error:self.assertIs(error,cancel)
            else:self.fail('Missing primary')
        self.assertEqual(len(closed),count)
    def test_opening_acquisition_cancel_closes_previously_acquired_and_current_file(self):
        cancel=GeneratorExit('input read');opened=[];real=os.open
        def acquire(path,*args,**kwargs):
            fd=real(path,*args,**kwargs);opened.append(fd);return fd
        with patch('ashlar_host.original_medical_runtime.os.open',side_effect=acquire),patch('ashlar_host.original_medical_runtime.os.read',side_effect=cancel):
            try:
                with self.opened():self.fail('Unexpected owner')
            except GeneratorExit as error:self.assertIs(error,cancel)
        self.assertEqual(len(opened),1)
        for fd in opened:
            with self.assertRaises(OSError):os.fstat(fd)

    def test_parent_alias_replacement_refuses_held_file(self):
        from ashlar_host.original_medical_runtime import _File
        pin=self.make('isolated/resource',b'original')
        held=_File(self.value(pin));held.acquire()
        try:
            parent=pin.path.parent;moved=self.root/'moved';parent.rename(moved);parent.symlink_to(moved,target_is_directory=True)
            with self.assertRaises(PermissionError):held.read()
        finally:held.close()
    def test_configured_root_omission_refuses(self):
        ref=self.plan['namespaces'][0];path=Path(ref['record']['path']);value=json.loads(path.read_text());value['roots']={}
        path.write_text(json.dumps(value));ref['record']=self.value(self.pin(path));self.save()
        with self.assertRaises(PermissionError):
            with self.opened():self.fail('Incomplete configured runtime namespaces admitted')
    def test_namespace_root_alias_refuses(self):
        with self.assertRaises((PermissionError,ValueError)):
            with self.opened()as owner:
                root=self.root/'sdk';moved=self.root/'moved-sdk';root.rename(moved);root.symlink_to(moved,target_is_directory=True);owner.renew()
