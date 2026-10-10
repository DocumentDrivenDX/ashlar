from pathlib import Path
import hashlib, json, sys
import ashlar.commerce_evolution as portable
import ashlar_host.evolution_admission as host
root=Path(__file__).resolve().parent/'venv'
assert Path(portable.__file__).resolve().is_relative_to(root)
assert Path(host.__file__).resolve().is_relative_to(root)
assert all(not name.startswith(('pyspark','delta','databricks','pydantic','tools')) for name in sys.modules)
resources=host.packaged_inputs()
print(json.dumps({'portable_module':portable.__file__,'host_module':host.__file__,'sdk_free':True,'resource_count':len(resources)-1,'resource_hashes':{name:hashlib.sha256(raw).hexdigest() for name,raw in resources},'python':sys.version},indent=2))
