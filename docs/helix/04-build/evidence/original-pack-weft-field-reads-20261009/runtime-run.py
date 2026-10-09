import importlib.util,sys
from pathlib import Path
root=Path('/Users/erik/Projects/ashlar')
sys.path[:0]=[str(root/'src'),str(root/'tools')]
p=Path(__file__).with_name('local_delta_custody.py')
spec=importlib.util.spec_from_file_location('local_delta_custody',p)
module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
from run_pack_field_weft import run
r=run({'archaeology':'/private/tmp/ashlar-archaeology-native-publication-20261009-a','ecology':'/private/tmp/ashlar-ecology-native-publication-20261009-a'},Path('/private/tmp/ashlar-pack-field-weft-20261009-b'),Path('/private/tmp/ashlar-delta4-jars'),Path('/private/tmp/ashlar-weft-unqualified-20261009-d/weft-runtime'),Path('/private/tmp/ashlar-weft-land-5856/scripts/local/public_integer_source.ts'),Path('/private/tmp/ashlar-umf-dataset-45473'))
print('Completed reviewed original field Weft cases:',sum(len(x['queries'])for x in r['reports']))
