"""Inspect selected assertion sources through actual pinned UMF APIs.

Produces an unregistered candidate inventory, never an accepted report.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from ashlar.schema import SchemaIntake
from ashlar.assertions import selected_assertion_inventory


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['umf-source','validator-revision','document','revision','output']:p.add_argument('--'+name,required=True)
    p.add_argument('--bun',default='bun');a=p.parse_args()
    out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
    artifacts=[]
    for tool,name in [('inspect_umf.ts','intake.json'),('inspect_schema_semantics.ts','interpretation.json')]:
        result=subprocess.run([a.bun,str(ROOT/'tools'/tool),a.umf_source,a.validator_revision,a.document],capture_output=True,timeout=60)
        if result.returncode:raise RuntimeError('Pinned UMF inspection refused: '+result.stderr.decode('utf-8','replace'))
        artifacts.append(result.stdout);(out/name).write_bytes(result.stdout)
    intake=SchemaIntake.read(artifacts[0],a.revision,trusted_validator_revision=a.validator_revision)
    inventory=selected_assertion_inventory(intake,artifacts[1])
    (out/'assertion-inventory.json').write_text(json.dumps(inventory,ensure_ascii=False,indent=2)+'\n')
    print(str(len(inventory['entries']))+' selected source assertions retained; all enforcement none/unqualified')

if __name__=='__main__':main()
