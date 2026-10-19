# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact declared decimal correspondence; inert reports confer no authority."""
import unittest
from ashlar_host.truss_graph_admission import admitted_facets,TrussGraphAdmission
import test_truss_typed_admission as fixtures

class Tests(unittest.TestCase):
    def test_declared_scale_preserves_tokens_and_distinct_whole_number_ranges(self):
        for scale,tokens in ((2,('9999999999999999.99','-9999999999999999.99','1.23000','123e-2','0e999999999999')),
                (4,('99999999999999.9999','1.23000','123e-2','0e999999999999'))):
            facets={'precision':18,'scale':scale}
            for token in tokens:
                with self.subTest(scale=scale,token=token):
                    batch,catalogs=fixtures.Tests().fixture('decimal',value={'kind':'decimal','text':token},facets=facets)
                    admission=TrussGraphAdmission(batch,catalogs)
                    self.assertEqual(admission.revisions['1']['types']['1']['facets']['99'],facets)
                    self.assertIn(token,admission.changes[0].state.props_json)
        for scale,token in ((2,'10000000000000000.00'),(2,'1.231'),(2,'1e99999999'),
                (4,'100000000000000.0000'),(4,'9999999999999999.99'),(4,'1.23001')):
            with self.subTest(scale=scale,token=token):
                batch,catalogs=fixtures.Tests().fixture('decimal',value={'kind':'decimal','text':token},facets={'precision':18,'scale':scale})
                with self.assertRaises(PermissionError):TrussGraphAdmission(batch,catalogs)

    def test_closed_exact_facet_types_refuse_unknown_or_coerced_domains(self):
        for facets in ({'precision':18,'scale':True},{'precision':True,'scale':2},
                {'precision':18.0,'scale':2},{'precision':18,'scale':2.0},
                {'precision':18,'scale':3},{'precision':19,'scale':2},
                {'precision':18,'scale':2,'extra':True}):
            with self.subTest(facets=facets):
                with self.assertRaises(ValueError):admitted_facets({'scalarType':'decimal','facets':facets})

if __name__=='__main__':unittest.main()
