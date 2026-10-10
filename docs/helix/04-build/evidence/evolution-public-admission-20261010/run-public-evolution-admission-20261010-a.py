from pathlib import Path
import json,time,hashlib
from ashlar_host.config import EvolutionAdmissionConfig
from ashlar_host.evolution_admission import admit_commerce_evolution
output=Path('/private/tmp/ashlar-public-evolution-admission-20261010-a')
output.mkdir(exist_ok=False)
config=EvolutionAdmissionConfig(source=Path('/private/tmp/ashlar-umf-evolution-land-6a292'),bun=Path('/opt/homebrew/bin/bun'),git=Path('/usr/bin/git'),timeout_seconds=60,maximum_output_bytes=1048576,maximum_receipt_bytes=4194304)
start=time.monotonic()
admission=admit_commerce_evolution(config,source_system='installed-commerce-A',epoch='public-evolution-1')
(output/'public-receipt.json').write_bytes(admission.receipt_bytes)
(output/'custody.json').write_bytes(admission.custody_bytes)
summary={'scope':'Actual fresh selected public UMF source/data verification only; no installed wheel or native authority/publication/ACK/recovery claim','seconds':time.monotonic()-start,'metadata':admission.metadata(),'batches':len(admission.prepared.batches),'changes':len(admission.changes),'transitions':len(admission.transitions),'receipt_sha256':hashlib.sha256(admission.receipt_bytes).hexdigest()}
(output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary))
