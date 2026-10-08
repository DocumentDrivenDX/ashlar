"""Replay-safe role checks for the installed private development pin registry."""
import hashlib,json,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout/out/native/pins_20261008'
sql=ROOT/'sql/ashlar-pins/02-native-fixture-check.sql'
r=subprocess.run(['/usr/local/bin/docker','exec','-i','ashlar-e2e-truss-pg17','psql','-U','postgres','-d','truss_e2e','-v','ON_ERROR_STOP=1'],input=sql.read_bytes(),capture_output=True)
OUT.mkdir(parents=True,exist_ok=True)
(OUT/'check-output.txt').write_bytes(r.stdout+r.stderr)
if r.returncode:raise SystemExit(r.returncode)
summary={'state':'passed','database':'truss_e2e','schema':'ashlar_pins','ddl_sha256':hashlib.sha256((ROOT/'sql/ashlar-pins/01-postgresql.sql').read_bytes()).hexdigest(),'checks':['exact original register replay preserves one active pin','changed original version refuses','writer direct DELETE denied','reader release denied','active pin refuses maintenance admission','rolled-back release leaves original active'],'qualification':'Protected private PostgreSQL pin registry and ordinary-role guard checks only. Trusted registration must prove target data availability/source/custody before commit. Every admitted Delta retention operator must hold the guard transaction and check the full affected version/file union. No Delta retention operator is wired yet; no future availability, remote role-table fencing, publication or resolver authority claim. No automatic expiry or cleanup.'}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))
