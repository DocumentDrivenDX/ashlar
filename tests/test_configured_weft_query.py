"""Held provider resolution over real core/source owners and inert native ports."""
import json,unittest,uuid
from types import SimpleNamespace
from unittest.mock import patch
import test_configured_local_read as read_fixture
from test_configured_query_aliases import run_with_alias_scope
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar_host.configured_weft_binding import configured_count_star_request
from ashlar_host.configured_weft_query import ConfiguredWeftProvider


class Tests(unittest.TestCase):
    def test_original_vector_binding_and_interval_closure(self):
        observed=[]
        def action(scope,transport,aliases,spark):
            spark.version='4.0.1'
            options={'spark.sql.session.timeZone':'UTC','spark.sql.ansi.enabled':'true',
                     'spark.sql.decimalOperations.allowPrecisionLoss':'false'}
            spark.conf=SimpleNamespace(get=lambda name:options[name])
            original=spark.sql
            def sql(statement):
                if statement.startswith("SELECT ('a' COLLATE UTF8_BINARY)"):
                    return SimpleNamespace(first=lambda:{'equal':False})
                return original(statement)
            spark.sql=sql
            request=configured_count_star_request(scope,dict(aliases),'SELECT COUNT(*) AS n FROM Item i',layout_sha256=LAYOUT_SHA256)
            provider=ConfiguredWeftProvider(scope,aliases,request)
            with self.assertRaises(AttributeError):provider.driver=SimpleNamespace(transport=object())
            with self.assertRaises(AttributeError):provider.driver.transport=object()
            with self.assertRaises(AttributeError):provider.scope=object()
            with self.assertRaises(ValueError):provider.resolve(scope.context)
            with provider.interval(scope.context):
                resolved=provider.resolve(scope.context)
                self.assertEqual(set(resolved.snapshots),set(scope.descriptor.versions))
                binding=json.loads(request['target']['bindingJson'])
                provider.admit_binding(binding,resolved,scope.context)
                changed=json.loads(json.dumps(binding));changed['publication']['tables'][0]['version']+=1
                with self.assertRaises(ValueError):provider.admit_binding(changed,resolved,scope.context)
                self.assertIsNone(provider.closed)
                with self.assertRaises(ValueError):
                    with provider.interval(scope.context):pass
            receipt=provider.closed_interval_custody(scope.context)
            self.assertEqual(receipt['publication_id'],scope.descriptor.publication_id)
            with self.assertRaises(ValueError):provider.resolve(scope.context)
            # A failed closing cannot leave a released interval receipt.
            with self.assertRaises(ValueError):
                with provider.interval(scope.context):options['spark.sql.ansi.enabled']='false'
            self.assertFalse(provider.active);self.assertIsNone(provider.closed)
            options['spark.sql.ansi.enabled']='true';observed.append(provider)
        def native_port(**fields):
            if 'uuid'in fields:fields['uuid']=str(uuid.uuid5(uuid.NAMESPACE_URL,fields['uuid']))
            return SimpleNamespace(**fields)
        with patch.object(read_fixture,'SimpleNamespace',side_effect=native_port):run_with_alias_scope(action)
        self.assertEqual(len(observed),1)
        with self.assertRaises(ValueError):observed[0].runtime(observed[0].context)


if __name__=='__main__':unittest.main()
