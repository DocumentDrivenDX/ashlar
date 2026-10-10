"""Portable trust-order, Git-input custody and selected transport controls."""
import json,os,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar import weft_count_star_package as package
from ashlar import weft_count_star_installation as installation
from ashlar import weft_count_star_distribution as distribution

class ConsumerControls(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve());self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
    def config(self,**changes):
        values=dict(index_path=self.root/'index.json',index_revision=package.INDEX_REVISION,index_sha256=package.INDEX_SHA256,
                    realization_id=package.REALIZATION_ID,package=self.root/'unopened-package',output=self.root/'installation',
                    observed_target='aarch64-apple-darwin',observed_os='27.0.1');values.update(changes)
        return package.CountStarInstallationConfig(**values)
    def test_trust_cannot_be_replaced_by_operator_configuration(self):
        for change in ({'index_revision':'0'*40},{'index_sha256':'0'*64},{'realization_id':'older-profile'},
                       {'observed_os':'unknown'},{'observed_target':'unknown'}):
            with self.assertRaises(package.CountStarInstallationError):self.config(**change)
    def test_nonprimitive_configuration_refuses_before_comparison_or_read(self):
        class Equality:
            def __eq__(self,other):raise AssertionError('comparison-effect')
            def __ne__(self,other):raise AssertionError('comparison-effect')
        class StringSubclass(str):pass
        with patch.object(package,'read_snapshot',side_effect=AssertionError('read-effect')):
            for name in ('index_revision','index_sha256','realization_id','observed_target','observed_os'):
                for value in (Equality(),StringSubclass(getattr(self.config(),name))):
                    with self.assertRaises(package.CountStarInstallationError):self.config(**{name:value})
    def test_invalid_read_limits_refuse_before_open(self):
        with patch.object(package.os,'open',side_effect=AssertionError('open-effect')):
            for value in (-1,0,True,None,package.FILE_LIMIT+1):
                with self.assertRaises(package.CountStarInstallationError):package.read_snapshot(self.root/'unopened',value)
    def test_index_failure_precedes_package_access(self):
        self.config().index_path.write_bytes(b'{}');observed=[];original=package.read_snapshot
        def read(path,*args):observed.append(path);return original(path,*args)
        with patch.object(package,'read_snapshot',side_effect=read):
            with self.assertRaises(package.CountStarInstallationError):package.inspect_package(self.config())
        self.assertEqual(observed,[self.config().index_path])
    def test_normal_git_source_modes_read_without_chmod(self):
        for mode in (0o644,0o755):
            source=self.root/str(mode);source.write_bytes(b'exact');source.chmod(mode)
            self.assertEqual(package.read_snapshot(source),b'exact')
            self.assertEqual(source.stat().st_mode&0o777,mode)
        link=self.root/'link';link.symlink_to(source)
        with self.assertRaises(package.CountStarInstallationError):package.read_snapshot(link)
    def test_source_read_cleanup_cancellation_priority(self):
        cancellation=KeyboardInterrupt();original=os.close
        def close(fd):original(fd);raise cancellation
        with patch.object(package.os,'close',side_effect=close):
            with self.assertRaises(KeyboardInterrupt)as caught:package.read_snapshot(self.root)
        self.assertIs(caught.exception,cancellation)
    def test_unknown_tree_entry_refuses_before_descent(self):
        (self.root/'extra').mkdir();(self.root/'expected').write_bytes(b'exact')
        with self.assertRaises(package.CountStarInstallationError):package.verify_exact_tree(self.root,('expected',))
    def test_tree_cleanup_cancellation_outranks_ordinary_failure(self):
        cancellation=GeneratorExit()
        class Scan:
            def __iter__(self):raise ValueError('enumeration')
            def close(self):raise cancellation
        with patch.object(package.os,'scandir',return_value=Scan()):
            with self.assertRaises(GeneratorExit)as caught:package.verify_exact_tree(self.root,())
        self.assertIs(caught.exception,cancellation)
    def test_installed_transport_never_accepts_older_compiled_profile(self):
        opened=installation.CountStarInstallation(self.config(package=None),b'fixed-ready')
        def transport(executable,request,*,refuse,validate_response,closing_verify):
            validate_response(self.response);closing_verify();return self.response
        for version,status,backend,ir,accepted in (
            ('weft-compile/0.4.1','compiled',package.VERSION,'weft-ir/0.4.1',True),
            ('weft-compile/0.4.0','compiled','0.4.0-paths-keys-candidate','weft-ir/0.4.0',False),
            ('weft-compile/0.4.0','blocked',None,None,True)):
            value={'interfaceVersion':version,'status':status}
            if status=='compiled':value.update(backend={'backendVersion':backend},logicalPlan={'irVersion':ir})
            self.response=(json.dumps(value)+'\n').encode()
            with patch.object(installation,'open_installation',return_value=opened),patch.object(installation.mechanics,'compile_transport',side_effect=transport):
                if accepted:self.assertEqual(installation.compile_request(opened,b'original'),self.response)
                else:
                    with self.assertRaises(package.CountStarInstallationError):installation.compile_request(opened,b'original')
    def test_public_configuration_cannot_select_unreviewed_trust(self):
        with self.assertRaises(distribution.CountStarDistributionError):distribution.open_count_star_distribution(object())
        self.assertEqual(distribution.INDEX_REVISION,package.INDEX_REVISION)
        self.assertEqual(distribution.INDEX_SHA256,package.INDEX_SHA256)
if __name__=='__main__':unittest.main()
