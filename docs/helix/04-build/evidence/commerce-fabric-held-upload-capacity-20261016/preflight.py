from pathlib import Path
import hashlib,importlib.metadata,json,sys
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'source/src'))
from runpy import run_path
owner=run_path(str(ROOT/'run-proof.py'),run_name='review_preflight')
owner['source_check']()
from ashlar_host.commerce_export_cli import parse_commerce_export_configuration
cfg=parse_commerce_export_configuration((ROOT/'selection-original.json').read_bytes())
assert str(cfg.publication)=='/private/tmp/ashlar-current-commerce-preparation-20261015-9pn8k_h7/publication'
assert sys.version_info[:2]==(3,12)
for package,version in [('pyspark','4.0.1'),('delta-spark','4.0.0'),('psycopg','3.2.13')]:
    assert importlib.metadata.version(package)==version,(package,importlib.metadata.version(package))
base=Path('/private/tmp/ashlar-current-commerce-preparation-20261015-9pn8k_h7/query-jars')
for name,wanted in [('delta-spark_2.13-4.0.0.jar','538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e'),('delta-storage-4.0.0.jar','9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d')]:
    assert hashlib.sha256((base/name).read_bytes()).hexdigest()==wanted

projection=Path('/private/tmp/ashlar-commerce-fabric-native-20261016-a')
for name,size,wanted in [
 ('nodes/part-00000-753bf520-b9a8-45c5-88b4-8e915a29e144-c000.snappy.parquet',11356,'88c4044cbdb53d4203e0b6a8d3514a17b3793885bfa238c65a09c37edf6ffb97'),
 ('nodes/_delta_log/00000000000000000000.json',4184,'8b8b90d3644a7f2fe523822c20036dad1cb0ac45214c999773c4cd57cd724c93'),
 ('edges/part-00000-d0ab884b-a097-4671-81b0-6f0dad9df55e-c000.snappy.parquet',13831,'71780d500269200ba09fb6797057a80dc67d172ccf18ab2863e4c758f06cd226'),
 ('edges/_delta_log/00000000000000000000.json',4815,'ccf48f6edfd95abbf4785c78411762808d5216e4a3345ec91b4ac9b677b6f1c0')]:
 raw=(projection/name).read_bytes()
 assert len(raw)==size and hashlib.sha256(raw).hexdigest()==wanted,name

# This is import/configuration/pin checking only. No engine/source/cloud authority.
print(json.dumps({'preflight':'PASS','python':sys.version,'source_revision':(ROOT/'expected-source-revision.txt').read_text().strip(),'qualification':'SDK versions, exact source and JAR pins; no native/source/cloud execution'}))
