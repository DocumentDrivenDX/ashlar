"""Export configuration uses the existing owner; no native services."""
import contextlib,io,json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar.cli import main
from ashlar_host.pack_configuration import FinitePackBudget,make_finite_pack_export_configuration
from ashlar_host.finite_export_cli import EXPORT_PROFILE,parse_finite_export_configuration

class Tests(unittest.TestCase):
    def selections(self,root):
        return {name:root/name for name in ('publication','output','jars','pack_file','model','graph','producer_source','bun','git')}

    def test_each_pack_preserves_exact_selections_without_compiler_or_effects(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();paths=self.selections(root)
            for pack in ('archaeology','ecology','payments'):
                raw=make_finite_pack_export_configuration(pack=pack,budget=FinitePackBudget('small-local'),**paths)
                value=json.loads(raw);selected=parse_finite_export_configuration(raw,paths['output'])
                self.assertEqual(value['profile'],EXPORT_PROFILE)
                self.assertFalse({'query','mode','output'}&value.keys())
                self.assertEqual(selected.native.producer.definition.name,pack)
                self.assertEqual(selected.native.output,paths['publication'])
                self.assertEqual(selected.output,paths['output'])
                self.assertIsNone(selected.query)
                for name in ('publication','jars','pack_file','model','graph'):self.assertEqual(value[name],str(paths[name]))
            self.assertEqual(list(root.iterdir()),[])
            for change in ({'pack':'medical'},{'output':paths['publication']},{'graph':'relative'}):
                values={**paths,'pack':'ecology','budget':FinitePackBudget('small-local'),**change}
                with self.assertRaises(ValueError):make_finite_pack_export_configuration(**values)

    def test_cli_prints_owner_admitted_profile_and_rejects_compiler_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();argv=['ashlar','configure-finite-pack-export','--pack','payments','--budget-profile','small-local']
            for name,value in self.selections(root).items():argv+=['--'+name.replace('_','-'),str(value)]
            output=io.StringIO()
            with patch.object(sys,'argv',argv),contextlib.redirect_stdout(output):main()
            self.assertIsNone(parse_finite_export_configuration(output.getvalue().encode(),root/'output').query)
            for option in ('--index','--installation','--query-profile'):
                with patch.object(sys,'argv',argv+[option,'unselected']),contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit) as error:main()
                self.assertEqual(error.exception.code,2)
            self.assertEqual(list(root.iterdir()),[])
