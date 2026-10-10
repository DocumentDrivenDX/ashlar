from pathlib import Path
import hashlib, json, os, sys, time
import ashlar.commerce_evolution as portable
from ashlar_host.config import EvolutionAdmissionConfig
import ashlar_host.evolution_admission as host
root=Path(__file__).resolve().parent
assert 'PYTHONPATH' not in os.environ
for module in (portable,host):
    assert Path(module.__file__).resolve().is_relative_to(root/'venv')
config=EvolutionAdmissionConfig(source=Path('/private/tmp/ashlar-umf-evolution-land-6a292'),bun=Path('/opt/homebrew/bin/bun'),git=Path('/usr/bin/git'),timeout_seconds=60,maximum_output_bytes=1048576,maximum_receipt_bytes=4194304)
started=time.monotonic()
admitted=host.admit_commerce_evolution(config,source_system='installed-evolution-A-20261010',epoch='installed-qualification-20261010-a')
assert len(admitted.prepared.batches)==4
assert len(set(batch.batch_id for batch in admitted.prepared.batches))==4
assert len(admitted.transitions)==19
assert hashlib.sha256(admitted.receipt_bytes).hexdigest()==portable.PRESERVATION_SHA
metadata=admitted.metadata()
assert len(json.dumps(metadata).encode())<1048576
result={'qualification':'Installed public source admission only; no graph publication, native ingest, source or ACK authority.','commit':'02e23cdf16c9c8fe0c4719b54d7e1bdc6688f499','module':host.__file__,'elapsed_seconds':time.monotonic()-started,'metadata':metadata,'change_count':len(admitted.changes),'transition_count':len(admitted.transitions)}
(root/'actual-admission-result.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
