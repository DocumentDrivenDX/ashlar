"""Offline exact recovery against a read-only original journal snapshot.

Retained authority is an observation, not current authentication or admission.
This check grants no native writes, publication, source fencing or ACK.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from durable_effects import DurableEffects,EffectPlanError
from durable_sql import DurableSQL

class OfflineAPI:
    def do(self,*args,**kwargs):raise AssertionError('Offline recovery attempted native transport')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.output.exists():parser.error('Fresh evidence output required')
    with sqlite3.connect(args.journal.resolve().as_uri()+'?mode=ro',uri=True) as original:
        with tempfile.TemporaryDirectory(prefix='ashlar-offline-recovery-') as temporary:
            path=Path(temporary)/'copy.sqlite'
            with sqlite3.connect(path) as copy:original.backup(copy)
            with sqlite3.connect(path) as copy:
                records=copy.execute('SELECT operation,intent_digest,plan_json,plan_digest FROM effect_plan ORDER BY operation').fetchall()
            if not records:raise ValueError('Original retained effect plans required')
            observations=[]
            for operation,intent,text,digest in records:
                if hashlib.sha256(text.encode()).hexdigest()!=digest:raise ValueError('Original plan digest differs')
                plan=json.loads(text)
                class Policy:
                    @contextmanager
                    def writer(self,op,context):
                        if op!=operation or context is not plan:raise PermissionError('Wrong offline original plan')
                        yield
                    def admit(self,value,context):
                        if value!=plan:raise PermissionError('Offline plan differs')
                client=DurableSQL(str(path),OfflineAPI(),plan['warehouse'],plan['authority'])
                try:
                    runner=DurableEffects(client,Policy())
                    result=runner.recover(operation,intent,plan['steps'],context=plan)
                    observations.append({'operation':operation,'plan_digest':digest,'intent_digest':intent,'responses':len(result['responses'])})
                    with client.db:client.db.execute('DELETE FROM effect_plan WHERE operation=?',(operation,))
                    try:runner.recover(operation,intent,plan['steps'],context=plan)
                    except EffectPlanError:pass
                    else:raise AssertionError('Missing original plan recreated')
                finally:client.close()
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps({'state':'passed','plans':observations,'missing_plan_refused':True,
        'native_transport_calls':0,'qualification':'Offline recovery from copied original native receipts; original journal opened read-only. No current authentication, native execution, writer/source admission, publication or ACK.'},indent=2)+'\n')
    print('Recovered '+str(len(observations))+' original plans offline; missing custody refused; no native transport calls.')

if __name__=='__main__':main()
