# SPDX-License-Identifier: MIT OR Apache-2.0
import base64,hashlib,json,unittest
from ashlar.apply import empty_state
from ashlar.truss_native_feed import native_feed_batch,scalar_entity_changes,scalar_object_changes,NativeFeedError
from ashlar_host.graph_sql import graph_sql_plan,TABLES

class Tests(unittest.TestCase):
    def batch(self,delete=False):
        encode=lambda value:json.dumps(value).encode()
        prior={'profile':'truss-caller-asserted-feed/0.1','installationId':'installation','sourceEpoch':'epoch','consumerId':'consumer','registrationId':'registration','nextXid':'43'if delete else'0'}
        following={**prior,'nextXid':'44'if delete else'43'};writer='43'if delete else'42';rows=[]
        identities=[{'typeId':'3','id':'7','definitionPin':'a-pin'},{'typeId':'4','id':'8','definitionPin':'b-pin'},{'typeId':'9','id':'9','definitionPin':'relationship-pin'}]
        images=[]
        for ordinal,identity in enumerate(identities):
            edge=ordinal==2
            image={'interfaceVersion':'truss-history-record/0.1.0','kind':'edge'if edge else'object','identity':identity,'recordVersion':'1','catalogRevision':'1','properties':[],'retained':[{'name':'note','value':{'kind':'string','text':'original edge'}}]if edge else[]}
            if edge:image.update(source=identities[0],target=identities[1],orderKey={'state':'text','text':'ordered first'})
            else:image['ownership']={'state':'rootless'}
            images.append(image)
        order=[2,0,1]if delete else[0,1,2]
        for seq,ordinal in enumerate(order,1):
            edge=ordinal==2;image=images[ordinal];identity=identities[ordinal]
            event={'interfaceVersion':'truss-history-event/0.2.0-proposal','sourceEpoch':'epoch','xid':writer,'seq':str(seq),'eventVersion':'2'if delete else'1','eventCatalogRevision':'1','identity':identity,'operation':'delete'if delete else'create',('before'if delete else'after'):image}
            raw=encode(event)
            rows.append({'seq':seq,'xid':writer,'entity_kind':'e'if edge else'o','entity_id':int(identity['id']),'entity_type':int(identity['typeId']),'ver':2 if delete else 1,'new_value':{'interfaceVersion':'truss-journal-exact-bytes/0.2.0-proposal','component':'event','bytesBase64':base64.b64encode(raw).decode(),'bytesSha256':hashlib.sha256(raw).hexdigest()}})
        content=encode({'interfaceVersion':'truss-native-feed/0.1','prior':prior,'next':following,'writerXid':writer,'changes':rows,'configuration':{},'catalogPrerequisites':[],'revisions':[],'sources':[],'reservations':[]})
        args={'sha256':hashlib.sha256(content).hexdigest(),'prior':encode(prior),'next':encode(following),'feed':'feed'}
        return native_feed_batch(content,**args),scalar_entity_changes(content,**args),args
    def plan(self,state,batch):
        return graph_sql_plan(state,batch,{role:'local.edges.'+role for role in TABLES},materialized_at='2026-10-12T00:00:00+00:00',schema_policy=lambda change:None)
    def test_atomic_objects_edge_keep_typed_endpoints_order_and_originals(self):
        batch,parsed,args=self.batch();state,steps=self.plan(empty_state(),batch)
        edge=parsed.changes[2].state
        self.assertEqual([(key.type_id,key.id)for key in edge.endpoints],[(3,7),(4,8)])
        self.assertEqual(parsed.edge_order_keys,((edge.key,1,'ordered first'),))
        rows=[row for step in steps if 'rows'in step['parameters']for row in json.loads(step['parameters']['rows'])]
        current=next(row for row in rows if 'rel_type_id'in row)
        self.assertEqual(current['order_key'],'ordered first');self.assertEqual(current['source_type'],'3');self.assertEqual(current['target_id'],'8')
        self.assertEqual(len(state.current),3);self.assertEqual(len(parsed.event_bytes),3)
        with self.assertRaises(NativeFeedError):scalar_object_changes(batch.begin,**args)
    def test_atomic_delete_uses_true_versions_and_replay_has_no_effects(self):
        created,_=self.plan(empty_state(),self.batch()[0]);batch,parsed,_=self.batch(delete=True)
        deleted,steps=self.plan(created,batch)
        self.assertEqual(len(deleted.current),0);self.assertEqual(len(deleted.tombstones),3)
        self.assertTrue(all(change.state.version==2 for change in parsed.changes))
        replay,repeated=self.plan(deleted,batch)
        self.assertEqual(replay,deleted);self.assertEqual(repeated,[])
        self.assertEqual(len(deleted.history),6)
