# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded lossless receipt bytes; no native SDK or database execution."""
import base64,gzip,hashlib,json,random,unittest
from types import SimpleNamespace
from ashlar_host.truss_feed_sessions import encode_truss_destination_receipt,TrussSessionError

class Tests(unittest.TestCase):
    def vector(self):
        return SimpleNamespace(authority='private-local-process',scope_kind='installation',scope_id='original',custody_digest='a'*64,targets={'local.truss.object_current':('original-uuid',2)})
    def original(self,request,manifest,vector):
        return (json.dumps({'profile':'ashlar-truss-destination-receipt/0.1','request_base64':base64.b64encode(request).decode(),'manifest_base64':base64.b64encode(manifest).decode(),'vector':{'authority':vector.authority,'scope_kind':vector.scope_kind,'scope_id':vector.scope_id,'custody_digest':vector.custody_digest,'targets':{k:list(v)for k,v in vector.targets.items()}}},sort_keys=True,separators=(',',':'))+'\n').encode()
    def test_small_exact_and_original_diagnosed_catalog_receipt_lossless(self):
        vector=self.vector();request=b'{"original":"exact bytes"}\n';manifest=b'{"publication_id":"original"}'
        self.assertEqual(encode_truss_destination_receipt(request,manifest,vector),self.original(request,manifest,vector))
        # Catalog-like repeated source/report bytes at the actual failed 1.3MiB scale.
        request=b'original-complete-catalog-and-source-page\n'*25000
        raw=self.original(request,manifest,vector);self.assertGreater(len(raw),1048576)
        encoded=encode_truss_destination_receipt(request,manifest,vector);envelope=json.loads(encoded)
        self.assertEqual(envelope['profile'],'ashlar-truss-destination-receipt-gzip-base64/0.1')
        compressed=base64.b64decode(envelope['gzip_base64'],validate=True)
        self.assertEqual(compressed[9],255);self.assertEqual(compressed[4:8],b'\0'*4)
        self.assertEqual(gzip.decompress(compressed),raw);self.assertEqual(envelope['original_bytes'],len(raw));self.assertEqual(envelope['original_sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(encoded,encode_truss_destination_receipt(request,manifest,vector));self.assertLessEqual(len(encoded),1048576)
    def test_incompressible_original_receipt_refuses_instead_of_truncation(self):
        request=random.Random(17).randbytes(1048576)
        with self.assertRaisesRegex(TrussSessionError,'compressed destination receipt exceeds'):
            encode_truss_destination_receipt(request,b'original manifest',self.vector())
