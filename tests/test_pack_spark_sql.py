import json,unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from unittest.mock import Mock,MagicMock
from run_pack_spark_sql import target_value,typed_rows,view_design,baseline
from run_pack_publication_weft import original_ports
import test_pack_publication_weft as binding_fixtures

class Tests(unittest.TestCase):
    def test_complete_original_scalar_null_profiles_keep_source_tokens(self):
        for pack,nodes,nulls in [('archaeology',39,13),('ecology',40,7)]:
            converter,native,_,_=original_ports(pack);model=(converter.ROOT/'ontology.json').read_bytes();graph=(converter.ROOT/'graph/fixture.json').read_bytes();_,bindings=converter.build_transaction(model,graph,source_system=native.SOURCE_SYSTEM)
            plans=view_design(pack,model,graph,bindings)
            self.assertEqual(sum(len(plan['expected_rows'])for plan in plans),nodes)
            self.assertEqual(sum(value is None for plan in plans for row in plan['expected_rows']for value in row),nulls)
            for plan in plans:
                self.assertEqual(len(plan['columns']),len(plan['expressions']))
                self.assertEqual(len({column['name']for column in plan['columns']}),len(plan['columns']))
            if pack=='archaeology':
                self.assertIn('2E+1',graph.decode());self.assertIn('4E+1',graph.decode())
                self.assertEqual(target_value('2E+1','decimal'),Decimal(20));self.assertEqual(target_value('4E+1','decimal'),Decimal(40))

    def test_numeric_range_scale_float_and_family_refusals(self):
        self.assertEqual(target_value('9'*38,'integer'),Decimal('9'*38))
        self.assertEqual(target_value('0.1','decimal'),Decimal('0.1'))
        for token,family in [('1e38','integer'),('1e20','decimal'),('0.0000000000000000001','decimal'),('NaN','decimal'),('Infinity','decimal'),(None,'unknown')]:
            with self.assertRaises(ValueError):target_value(token,family)
        with self.assertRaises(ValueError):typed_rows([[0.1]],['decimal'])
        with self.assertRaises(ValueError):typed_rows([[True]],['integer'])
        self.assertEqual(typed_rows([[None,'null',Decimal('0.1')]],['integer','string','decimal']),[(None,'null',Decimal('0.1'))])

    def test_later_view_bag_failure_prevents_every_authored_query(self):
        model,graph,bindings,manifest,registry,aliases=binding_fixtures.Tests().inputs('archaeology')
        plans=view_design('archaeology',model,graph,bindings);first=Mock();first.collect.return_value=plans[0]['expected_rows'];second=Mock();second.collect.return_value=[('forged',)]
        frame=MagicMock();frame.where.return_value.selectExpr.side_effect=[first,second]
        spark=Mock();spark.read.format.return_value.option.return_value.load.return_value=frame;spark.catalog.tableExists.return_value=False;spark.catalog.dropTempView.return_value=True
        provider=Mock()
        @contextmanager
        def interval(context):yield
        provider.interval.side_effect=interval
        table='local.archaeology.object_current';provider.resolve.return_value=SimpleNamespace(snapshots={table:SimpleNamespace(version=2)},descriptor=SimpleNamespace(raw={'fixture':'test-only'}))
        reader=SimpleNamespace(model=model,graph=graph,bindings=bindings,manifest=manifest,original_report={'table_registry':registry},aliases=aliases,context=object(),provider=provider,driver=SimpleNamespace(transport=SimpleNamespace(spark=spark,targets={table:SimpleNamespace(path=Path('/not-read-by-mock'))}),tables={'object_current':table}))
        with self.assertRaises(ValueError):baseline(reader,'archaeology')
        spark.sql.assert_not_called();first.createOrReplaceTempView.assert_called_once();spark.catalog.dropTempView.assert_called_once_with(plans[0]['name'])
