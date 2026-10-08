"""Host-only immutable multi-statement effect plan and original-handle execution.

Policy must authorize/fence the whole plan and independently admit its exact
source, schema, target UUID, prior state and intent before effects. Successful
statements are evidence only; complete graph parity/pins/publication are separate.
"""
import hashlib
import json
from durable_sql import SQLCustodyError


class EffectPlanError(ValueError):
    pass


class DurableEffects:
    def __init__(self, transport, policy):
        self.transport = transport
        self.policy = policy
        with transport.db:
            transport.db.execute('CREATE TABLE IF NOT EXISTS effect_plan (operation TEXT PRIMARY KEY, intent_digest TEXT NOT NULL, plan_json TEXT NOT NULL, plan_digest TEXT NOT NULL)')

    def run(self, operation, intent_digest, steps, *, context):
        if not isinstance(operation,str) or not operation:
            raise EffectPlanError('Original operation required')
        import re
        if not isinstance(intent_digest,str) or not re.fullmatch('[0-9a-f]{64}',intent_digest):
            raise EffectPlanError('Original publication request digest required')
        if not isinstance(steps,list) or not 1 <= len(steps) <= 100:
            raise EffectPlanError('Bounded ordered effect steps required')
        for step in steps:
            if not isinstance(step,dict) or set(step)!={'statement','parameters'}:
                raise EffectPlanError('Exact statement and parameter fields required')
            if not isinstance(step['statement'],str) or not step['statement']:
                raise EffectPlanError('Nonempty effect statement required')
            if not isinstance(step['parameters'],dict) or any(not isinstance(k,str) or not k or not isinstance(v,str) for k,v in step['parameters'].items()):
                raise EffectPlanError('Named exact string parameters required')
        text=json.dumps({'authority':self.transport.authority,'warehouse':self.transport.warehouse,
                         'operation':operation,'intent_digest':intent_digest,'steps':steps},
                        sort_keys=True,separators=(',',':'))
        if len(text.encode()) > 1024*1024:
            raise EffectPlanError('Effect plan byte budget exceeded')
        digest=hashlib.sha256(text.encode()).hexdigest()
        # Isolate from caller mutation, then reconstitute a fresh admission view.
        plan=json.loads(text)
        with self.policy.writer(operation,context) as permit:
            if permit is not None:
                raise EffectPlanError('Effect writer authority incomplete')
            if self.policy.admit(json.loads(text),context) is not None:
                raise EffectPlanError('Original effect plan admission incomplete')
            db=self.transport.db
            with db:
                db.execute('INSERT OR IGNORE INTO effect_plan VALUES (?,?,?,?)',(operation,intent_digest,text,digest))
                retained=db.execute('SELECT intent_digest,plan_json,plan_digest FROM effect_plan WHERE operation=?',(operation,)).fetchone()
            if retained != (intent_digest,text,digest):
                raise EffectPlanError('Immutable original effect plan conflict')
            results=[]
            for index,step in enumerate(plan['steps']):
                # Namespace cannot alias a different plan or ordinal. Original
                # completed receipts replay, pending handles GET, no-handle
                # uncertainty refuses. No replacement SQL is issued on recovery.
                key='effect:'+digest+':'+str(index)
                results.append(self.transport.query(key,step['statement'],step['parameters']))
            return {'intent_digest':intent_digest,'plan_digest':digest,'responses':results}
