import dataclasses,json,tempfile,unittest,base64,zlib,hashlib
from pathlib import Path
from unittest.mock import patch
from ashlar import weft_implicit_arithmetic_package as package
from ashlar import weft_implicit_arithmetic_installation as installation
from ashlar import weft_implicit_arithmetic_distribution as distribution
from ashlar import _weft_installation_mechanics as mechanics

INDEX_BYTES=zlib.decompress(base64.b64decode('eNrNl81uHDcMx+9+CsPnbCJKFEX12lOfoeiBosh6Wu/a2F0jcYO8e7nb2E3RSVK4gOHLADvSSPz48U/ux4vLyyu/3W/lePXD5dV78+NmLofjfhn3x+V2t1l20z68S2/h6s1pq+1iyQ6x9+f4eXn58fyMhb3JzfKHnL75aT4dVShLV9rI4N6hpo3IXq8JN3J3d2ObKfv3y26jspvLlKOd7zgft5Xd4nY4GfV4Rby9k+P16ezH1be/HW53Tx/FhsO15EqnLard2L0KeqeecuVJiiI2mhaXWry5T4LUW28pteFVZ2cuDiotzy9PHQ/Hs8uVCn1+++nJUvtgen+UcWPrto5l9+4ci/397rhsbd3cxwDNDE1y3MMykqZeUSg3TDARUgHXRuytcsaWC5hUKFKwtDVzETsC5H9ZfJT9r3ZO91o2/s6BHA62HTcPP94fjrfzYd29x00b/WvXN1KSvI80PaGRU8lWCiTPUi2bZ84VEqUIAeeZETITkEGb5FaAsOdVHwFrK48eXnzh53fArCWJlbo5uXF4OSpdOAFAiTRjaux1DFUnMcuzi04HRtXZRtiWR/NZqzGO1nFWUbS1ELSE+HwqzwFYN7bBEGZJTXMjAoTCJYUdmCNjUqOaeKQGMwos/Og8x4iSKwRVSDvAmrGcW2FMr4ZJ0DnOAiGhEZoyOGNNyGw8AGZOPZXw3txMuwaXNWUDDzC7Te11lcmaGZ/BZJEsretnJn+3hxcEEwFn6Ih3CCUMeTQNWRk0++QWEjpzLohMSqXC6JVb9yhXEB/QSlqXy5bov8vlEyClJKr05nvInuPzFV9a6GSdkR92tiEFKiHihK6agcBFmySKmyiEflbKYBO7yeRRc9ar16OYxbg367NbGhwmFkGOkgtapagiplraxBILQyC+Khxlydo9FNXXFbOElObnKOacamNu9Db62OYQIXk5OqvMjuiUepSqAXDEBbCNelIlT416TBk+Wx91KtWWokLdODCF6PNYVulspfxP2fwGgykM7F24x6CBNBtUUG6mKYGRsk33KDlN4l1NlRSKJROeudg0TqvaWUsOYl8NnZJGrrWicu05WgAgBnQUA0vhGE1yDaGk0Aka1SJRLV55sqGxWZOqrGrnSWeeQWe0Hc+JNsv27mbR5biR/XK83tpx0ZfDlM3bQFCAKFmYTC1aSaLQGi8BwZQY8eQUIcHuSaCHcubglDN5jc65FpAefen5mK6E42umg2ERYRgFskDGMIs6Y2sGpWcnDXHBCrPqiLmFsxTuBbVFc6x1NZdMHFWKr4bXgFKtxBTVJWosWPUIO8YkyDziz4HbgJBOpWSAvWFVj35YqsUExCE+tMprS63/k9d4/nLx6eJPIVt/Ug=='))

class Tests(unittest.TestCase):
    def config(self,root):
        return package.ImplicitArithmeticInstallationConfig(root/'index.json',package.INDEX_REVISION,
            package.INDEX_SHA256,root/'package',package.REALIZATION_ID,root/'installed','aarch64-apple-darwin','27.0.1')

    def verified(self):
        resources=tuple((name,b'{}')for name in mechanics.installation_layout('implicit-arithmetic').resources)
        return package.VerifiedImplicitArithmeticPackage(b'{}',b'{}',b'inert',b'{}',resources)

    def test_mixed_index_platform_and_forged_configuration(self):
        with tempfile.TemporaryDirectory()as name:
            config=self.config(Path(name).resolve())
            for field,value in [('index_revision','bd490abc877a1992666c7b45412b14a0ca6c2cac'),('realization_id','count-star'),('observed_os','27.0.2'),('index_sha256',True)]:
                with self.assertRaises(package.ImplicitArithmeticInstallationError):dataclasses.replace(config,**{field:value})
            with self.assertRaises(package.ImplicitArithmeticInstallationError):package.verify_trusted_index(object())
            (config.index_path).write_bytes(b'{}')
            with self.assertRaises(package.ImplicitArithmeticInstallationError):package.inspect_package(config)

    def test_exact_five_entry_index_and_altered_product_refusal(self):
        with tempfile.TemporaryDirectory()as name:
            config=self.config(Path(name).resolve());config.index_path.write_bytes(INDEX_BYTES)
            raw,entry=package.verify_trusted_index(config)
            self.assertEqual(raw,INDEX_BYTES);self.assertEqual(entry['executable']['path'],'bin/weft-implicit-arithmetic')
            index=json.loads(INDEX_BYTES);index['entries'][-1]['executable']['sha256']='0'*64
            config.index_path.write_text(json.dumps(index))
            with self.assertRaises(package.ImplicitArithmeticInstallationError):package.verify_trusted_index(config)

    def test_closed_layout_old_profile_unchanged(self):
        old=mechanics.installation_layout('count-star');new=mechanics.installation_layout('implicit-arithmetic')
        self.assertEqual(old.executable,'weft-paths-keys');self.assertEqual(new.executable,'weft-implicit-arithmetic')
        self.assertIn('compile-response-v0.4.2.schema.json',new.schemas)
        with self.assertRaises(ValueError):dataclasses.replace(new,executable=old.executable)
        with self.assertRaises(ValueError):mechanics.installation_layout('caller-selected')

    def test_fresh_readonly_installation_no_clobber(self):
        with tempfile.TemporaryDirectory()as name:
            config=self.config(Path(name).resolve());verified=self.verified()
            def verify(cfg,raw,**kwargs):return installation.ImplicitArithmeticInstallation(cfg,raw)
            with patch.object(installation,'inspect_package',return_value=verified),patch.object(installation,'_verify',side_effect=verify):
                result=installation.install(config)
                self.assertEqual((config.output/'weft-implicit-arithmetic').read_bytes(),b'inert')
                self.assertEqual((config.output/'weft-implicit-arithmetic').stat().st_mode&0o777,0o555)
                self.assertEqual((config.output/'schemas/compile-request-v0.4.2.schema.json').stat().st_mode&0o777,0o444)
                self.assertEqual(json.loads(result.ready_bytes)['format'],'ashlar-weft-implicit-arithmetic-ready/0.1')
                with self.assertRaises(package.ImplicitArithmeticInstallationError):installation.install(config)
                self.assertEqual((config.output/'weft-implicit-arithmetic').read_bytes(),b'inert')

    def test_cancellation_before_ready_retains_identity_and_cleans_owned_output(self):
        with tempfile.TemporaryDirectory()as name:
            config=self.config(Path(name).resolve());cancel=KeyboardInterrupt()
            with patch.object(installation,'inspect_package',return_value=self.verified()),patch.object(installation,'_verify',side_effect=cancel):
                with self.assertRaises(KeyboardInterrupt)as caught:installation.install(config)
            self.assertIs(caught.exception,cancel);self.assertFalse(config.output.exists())
            self.assertEqual(list(config.output.parent.glob('.ashlar-implicit-arithmetic-*')),[])

    def test_transport_rejects_mixed_response_and_renews_before_release(self):
        with tempfile.TemporaryDirectory()as name:
            selected=installation.ImplicitArithmeticInstallation(self.config(Path(name).resolve()),b'ready')
            def transport(executable,request,**kwargs):
                self.assertEqual(executable.name,'weft-implicit-arithmetic')
                kwargs['validate_response'](json.dumps(response).encode()+b'\n')
                kwargs['closing_verify']();return b'original\n'
            response={'interfaceVersion':'weft-compile/0.4.2','status':'compiled','backend':{'backendVersion':package.VERSION},'logicalPlan':{'irVersion':'weft-ir/0.4.2'}}
            with patch.object(installation,'open_installation',return_value=selected)as opening,patch.object(mechanics,'compile_transport',side_effect=transport):
                self.assertEqual(installation.compile_request(selected,b'whole-original\n'),b'original\n')
                self.assertEqual(opening.call_count,2)
                response['logicalPlan']['irVersion']='weft-ir/0.4.1'
                with self.assertRaises(package.ImplicitArithmeticInstallationError):installation.compile_request(selected,b'whole-original\n')

    def test_complete_readonly_closure_refuses_mode_hash_and_foreign_file(self):
        with tempfile.TemporaryDirectory()as name:
            root=Path(name).resolve();layout=mechanics.installation_layout('implicit-arithmetic')
            def desc(path,raw):return {'path':path,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
            resources=[desc(path,b'{}')for path in layout.resources]
            executable=desc('bin/weft-implicit-arithmetic',b'inert')
            ready={'executable':{**executable,'path':layout.executable},'resources':resources,'provenance':desc('provenance.json',b'{}')}
            for item in [ready['executable'],ready['provenance'],*resources]:
                path=root/item['path'];path.parent.mkdir(parents=True,exist_ok=True)
                path.write_bytes(b'inert'if item['path']==layout.executable else b'{}')
                path.chmod(0o555 if item['path']==layout.executable else 0o444)
            def verify():return mechanics.verify_installed_files(root,ready,layout=layout,ready_present=False,
                expected_resources=resources,expected_executable=executable,read=package.read_snapshot,
                exact_tree=package.verify_exact_tree,refuse=package.refuse)
            self.assertEqual(verify(),b'{}')
            target=root/resources[0]['path'];target.chmod(0o644)
            with self.assertRaises(package.ImplicitArithmeticInstallationError):verify()
            target.write_bytes(b'[]');target.chmod(0o444)
            with self.assertRaises(package.ImplicitArithmeticInstallationError):verify()
            target.chmod(0o644);target.write_bytes(b'{}');target.chmod(0o444)
            (root/'foreign').write_bytes(b'untouched')
            with self.assertRaises(ValueError):verify()
            self.assertEqual((root/'foreign').read_bytes(),b'untouched')

    def test_distribution_platform_refuses_before_installed_files(self):
        paths=distribution.ImplicitArithmeticDistributionPaths(Path('/index'),Path('/installed'))
        with patch.object(distribution.sys,'platform','linux'),patch.object(installation,'open_installation')as opening:
            with self.assertRaises(distribution.ImplicitArithmeticDistributionError):distribution.open_implicit_arithmetic_distribution(paths)
            opening.assert_not_called()

if __name__=='__main__':unittest.main()
