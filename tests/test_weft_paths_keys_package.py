"""Inert selected-profile controls; injected test index is fixture authority only."""
import copy
from dataclasses import FrozenInstanceError
import gzip
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ashlar import weft_paths_keys_package as p


def desc(name,raw):
    return {'path':name,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


class PathsKeysPackageTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve()

    def config(self,raw=b'{}',package=None):
        index=self.root/'index'; index.write_bytes(raw)
        return p.PathsKeysInstallationConfig(index,'a'*40,hashlib.sha256(raw).hexdigest(),package,
                                            'fixture-keys',self.root/'out','aarch64-apple-darwin','27.0.1')

    def test_explicit_frozen_config_refuses_relative_unknown_and_malformed(self):
        c=self.config()
        with self.assertRaises(FrozenInstanceError): c.observed_os='11'
        for field,value in [('index_path',Path('relative')),('package',Path('relative')),
                            ('index_revision','a'*39),('index_sha256','z'*64),
                            ('realization_id','..'),('observed_os','secret\n')]:
            args=dict(c.__dict__); args[field]=value
            with self.subTest(field=field),self.assertRaises(p.PathsKeysInstallationError):
                p.PathsKeysInstallationConfig(**args)

    def test_trusted_pin_gate_precedes_package_and_no_output_effect(self):
        c=self.config(package=self.root/'absent'); object.__setattr__(c,'index_sha256','b'*64)
        original=p.read_snapshot
        # The owning public index verifier must be the only reader before refusal.
        from ashlar import weft_paths_package as generic
        def index_only(path,*args):
            self.assertEqual(path,c.index_path); return original(path,*args)
        with patch.object(generic,'read_snapshot',side_effect=index_only):
            with self.assertRaises(p.PathsKeysInstallationError): p.inspect_package(c)
        self.assertFalse(c.output.exists())

    def test_old_profile_and_missing_package_never_fallback(self):
        entry={'realizationId':'fixture-keys','manifest':desc('manifest.json',b'{}'),
               'assemblyCustody':desc('assembly-custody.json',b'{}'),
               'executable':{'path':'bin/weft-paths','sha256':'a'*64,'bytes':1},'target':'aarch64-apple-darwin'}
        raw=p.encode_document({'format':'weft-distribution-index/0.1','entries':[entry]})
        with self.assertRaises(p.PathsKeysInstallationError): p.inspect_package(self.config(raw))
        with self.assertRaises(p.PathsKeysInstallationError): p.validate_manifest({'format':'weft-distribution/0.1'})

    def test_bounded_single_gzip_and_old_limits_unchanged(self):
        raw=b'x'*31; zipped=gzip.compress(raw,mtime=0)
        self.assertEqual(p._inflate(zipped,31),raw)
        for candidate,limit in [(zipped,30),(zipped+zipped,100),(zipped+b'tail',100)]:
            with self.assertRaises(p.PathsKeysInstallationError): p._inflate(candidate,limit)
        from ashlar import weft_installation as legacy
        from ashlar import weft_paths_package as paths
        self.assertEqual(legacy.DECODED_LIMIT,20*1024*1024)
        self.assertEqual(paths.SOURCE,'530ae3511a4a50364d3d7e26195d3883952601df')
        self.assertEqual((p.JSON_LIMIT,p.FILE_LIMIT,p.TOTAL_LIMIT,p.DECODED_LIMIT,p.PROTOCOL_LIMIT),
                         tuple(x*1024*1024 for x in (4,32,96,64,16)))

    def test_aggregate_preflight_and_conflicting_snapshot(self):
        store=p._Package(self.root); store.total=p.TOTAL_LIMIT
        with patch.object(p,'read_snapshot',side_effect=AssertionError('read beyond budget')):
            with self.assertRaises(p.PathsKeysInstallationError): store.artifact(desc('next',b'x'))
        store=p._Package(self.root)
        with patch.object(p,'read_snapshot',side_effect=[b'a',b'b']):
            store.artifact(desc('file',b'a'))
            with self.assertRaises(p.PathsKeysInstallationError): store.artifact(desc('file',b'b'))

    def test_closing_drift_and_both_modes(self):
        path=self.root/'bin/weft-paths-keys'; path.parent.mkdir(); path.write_bytes(b'bin'); path.chmod(0o555)
        metadata=self.root/'metadata'; metadata.write_bytes(b'proof'); metadata.chmod(0o444)
        store=p._Package(self.root); store.artifact(desc('bin/weft-paths-keys',b'bin')); store.artifact(desc('metadata',b'proof')); store.close()
        path.chmod(0o444)
        with self.assertRaises(p.PathsKeysInstallationError): store.close()
        path.chmod(0o555); metadata.chmod(0o644)
        with self.assertRaises(p.PathsKeysInstallationError): store.close()
        metadata.write_bytes(b'drift'); metadata.chmod(0o444)
        with self.assertRaises(p.PathsKeysInstallationError): store.close()

    def test_extra_directory_fifo_and_symlink_are_never_read(self):
        (self.root/'known').write_bytes(b'x'); store=p._Package(self.root); store.artifact(desc('known',b'x'))
        (self.root/'known').chmod(0o444); extra=self.root/'extra'; extra.mkdir()
        from ashlar import weft_paths_package as generic
        original=os.scandir; visits=[]
        def scan(path): visits.append(path); return original(path)
        with patch.object(generic.os,'scandir',side_effect=scan):
            with self.assertRaises(p.PathsInstallationError): store.close()
        self.assertEqual(visits,[self.root]); extra.rmdir(); os.mkfifo(extra)
        with self.assertRaises(p.PathsInstallationError): store.close()
        extra.unlink(); extra.symlink_to(self.root/'known')
        with self.assertRaises(p.PathsInstallationError): store.close()

    def test_current_records_inventory_producer_and_namespace_correspondence(self):
        receipt,bundle,backend=self.records_fixture()
        self.assertEqual(len(p._records(receipt,bundle,backend)),74)
        for mutation in ('history-as-current','count','producer','namespace','coverage','target'):
            r=copy.deepcopy(receipt); b=copy.deepcopy(bundle)
            if mutation=='history-as-current': r['sourceCommit']='530ae3511a4a50364d3d7e26195d3883952601df'
            elif mutation=='count': r['executedProtocolCases']=463
            elif mutation=='producer': r['producerProvenance']['harnessSha256']='b'*64
            elif mutation=='namespace':
                r['cases'][0]['responseHex']=p.encode_document({'interfaceVersion':'weft-compile/0.4.0','status':'blocked'}).hex()
                b['namespaceFences'][0]['responseHex']=r['cases'][0]['responseHex']
            elif mutation=='coverage': r['coverage']=b['coverage']={'missing':{'accepted':[],'refused':['namespace:0.1-pair'],'scope':'fixture'}}
            else:
                value=p.decode_document(bytes.fromhex(r['cases'][5]['responseHex'])); value['targetContext']['id']='wrong'
                r['cases'][5]['responseHex']=p.encode_document(value).hex(); b['paths'][0]['responseHex']=r['cases'][5]['responseHex']
            with self.subTest(mutation=mutation),self.assertRaises(p.PathsKeysInstallationError): p._records(r,b,backend)

    def records_fixture(self):
        cap='relationship.boundedKeys'; blocked=p.encode_document(p.FENCE).hex()
        compiled=p.encode_document({'interfaceVersion':'weft-compile/0.4.0','status':'compiled',
            'backend':{'backendId':p.BACKEND_ID,'backendVersion':p.VERSION,'interfaceVersion':'weft-backend/0.3.0','targetProfile':p.TARGET},
            'targetContext':{'id':p.TARGET},'logicalPlan':{'requiredCapabilities':[cap]}}).hex()
        def case(identity,status,role='valid'): return {'id':identity,'role':role,'requestHex':b'{}'.hex(),'responseHex':compiled if status else blocked}
        bundle={'format':'weft-paths-keys-corpus/0.1','sourceCommit':p.SOURCE,
                'paths':[case(str(i),i<36) for i in range(50)],
                'controls':[case(name,i>=16) for i,name in enumerate(p.CONTROL_IDS)],
                'namespaceFences':[case(name,False,'namespace' if i<3 else 'invalid') for i,name in enumerate(p.NAMESPACE_IDS)],
                'coverage':{cap:{'accepted':['paths:0'],'refused':['namespace:0.1-pair'],'scope':'fixture only'}},
                'sources':[],'historicalQualification':{'fixture':'separately checked by historical proof'}}
        rows=[]
        for prefix,group in [('namespace',bundle['namespaceFences']),('paths',bundle['paths']),('controls',bundle['controls'])]:
            for i,c in enumerate(group):
                rows.append(dict(c,id=prefix+':'+c['id'],scope=('legacy' if i<3 else 'controls') if prefix=='namespace' else prefix,exit=0,stderrHex='',migrations=[]))
        receipt={'sourceCommit':p.SOURCE,'binarySha256':p.BINARY,'sourceInventorySha256':p.INVENTORY,
                 'declaredCapabilityCount':48,'profile':'paths-keys','cases':rows,'executedProtocolCases':74,
                 'protocolStatusCounts':{'compiled':39,'blocked':35},'historicalQualification':bundle['historicalQualification'],
                 'harnessSha256':p.HARNESS,'schemaCheckerSha256':p.BRIDGE,'producerProvenance':{'bytes':35371,'harnessSha256':p.HARNESS,'scope':'Separately reviewed qualification producer; not asserted to belong to the compiler source inventory'},
                 'coverage':bundle['coverage']}
        return receipt,bundle,{'capabilities':[{'id':cap}]}

    def test_metadata_public_refusal_is_safe_and_zero_exit_is_exact(self):
        for value in (None, [], {'format':'weft-distribution/0.1'}, {'secret':'payload'}):
            with self.subTest(value=type(value)),self.assertRaises(p.PathsKeysInstallationError) as caught:
                p.validate_manifest(value)
            self.assertEqual(str(caught.exception),'ASHLAR-WEFT-PATHS-KEYS-REFUSED')
        receipt,bundle,backend=self.records_fixture()
        receipt['cases'][0]['exit']=False
        with self.assertRaises(p.PathsKeysInstallationError): p._records(receipt,bundle,backend)
        receipt,bundle,backend=self.records_fixture()
        receipt['producerProvenance']['scope']='compiler source inventory'
        with self.assertRaises(p.PathsKeysInstallationError): p._records(receipt,bundle,backend)

    def test_original_source_blob_and_inventory_validation(self):
        raw=b'original'; entry={'path':'file','mode':'100644','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
                              'gitBlob':hashlib.sha1(b'blob 8\0'+raw).hexdigest()}
        document=p.encode_document({'sourceCommit':p.SOURCE,'files':[entry]})
        self.assertEqual(p._inventory(document,p.SOURCE,1),{'file':entry}); p._source_bytes(raw,entry)
        with self.assertRaises(p.PathsKeysInstallationError): p._source_bytes(b'changed!',entry)
        for field,value in [('mode','120000'),('gitBlob','z'*40),('path','../escape')]:
            altered=dict(entry); altered[field]=value
            with self.subTest(field=field),self.assertRaises(p.PathsKeysInstallationError):
                p._inventory(p.encode_document({'sourceCommit':p.SOURCE,'files':[altered]}),p.SOURCE,1)

if __name__=='__main__': unittest.main()
