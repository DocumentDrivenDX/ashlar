"""Read-only installed-package/runtime observation; never creates a gateway."""
import hashlib,json,sys,sqlite3,importlib.metadata
from pathlib import Path
import ashlar,ashlar_host.commerce
from ashlar_host.config import ProducerConfig,PrivatePostgresConfig,PublishCommerceConfig,QueryCommerceConfig
import pyspark,delta,psycopg
from pyspark.sql import SparkSession
from pyspark import SparkContext
phase=sys.argv[1];root=Path(sys.argv[2]);site=Path('/private/tmp/ashlar-host-wheel-env-20261010-a/lib/python3.11/site-packages');resources=site/'ashlar_host/resources'
producer=ProducerConfig(Path('/private/tmp/ashlar-umf-dataset-45473'),Path('/opt/homebrew/bin/bun'),Path('/usr/bin/git'),60,1024*1024,4*1024*1024)
pg=PrivatePostgresConfig('ashlar-e2e-truss-pg17','127.0.0.1',15432,'truss_e2e')
if phase=='publish':
 import graphframes
 config=PublishCommerceConfig(root/'publication',Path('/private/tmp/ashlar-authored-path-publish-jars-20261010-a'),resources/'ontology.json',resources/'graph.json',producer,pg,'private-original-commerce-fixture','ashlar-commerce-development-bindings/0.2')
else:
 config=QueryCommerceConfig(Path('/private/tmp/ashlar-weft-fresh-cli-20261010-a/ashlar/out/weft-distribution/distributions/index.json'),Path('/private/tmp/ashlar-weft-fresh-cli-20261010-a/ashlar/out/weft-installation'),root/'publication',root/'queries',Path('/private/tmp/ashlar-installed-commerce-query-jars-20261010-a'),resources/'ontology.json',resources/'graph.json',producer,pg)
paths=ashlar_host.commerce.runtime_paths(config,phase=='publish')
assert SparkContext._active_spark_context is None and SparkContext._gateway is None
assert str(Path(ashlar.__file__).resolve()).startswith(str(site))
assert not any(name=='tools' or name.startswith('tools.') for name in sys.modules)
files=[]
for name,m in sorted(sys.modules.items()):
 p=getattr(m,'__file__',None)
 if p and Path(p).is_file():
  path=Path(p).resolve();b=path.read_bytes();files.append({'module':name,'path':str(path),'sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b)})
print(json.dumps({'phase':phase,'python':sys.version,'sqlite':sqlite3.sqlite_version,'versions':{n:importlib.metadata.version(n)for n in ['pyspark','delta-spark','psycopg','graphframes-py']if phase=='publish' or n!='graphframes-py'},'jars':[str(p)for p in paths],'loadedModules':files,'noGateway':True,'noActiveContext':True,'installedPackage':str(site)},indent=2))
