import gzip
import json
from pathlib import Path
import unittest
from dataclasses import replace

from ashlar import commerce_evolution as tx
from ashlar_host.schema_rows import fixture_columns

ROOT=Path(__file__).resolve().parents[1]
EVIDENCE=ROOT/'docs/helix/04-build/evidence/commerce-present-field-preparation-20261009'

class PortableEvolutionPreparationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.candidate=(EVIDENCE/'candidate.json').read_bytes()
        cls.proof=gzip.decompress((EVIDENCE/'public-presence.json.gz').read_bytes())
        cls.model=(ROOT/'docs/helix/04-build/evidence/commerce-evolution-preparation-20261009/original-model.json').read_bytes()
        cls.columns=fixture_columns(ROOT)

    def prepare(self,source='A',epoch='original'):
        return tx.prepare(self.candidate,self.proof,self.model,source_system=source,epoch=epoch)

    def oracle(self,prepared,prefix=4):
        return tx.independent_oracle(self.candidate,self.proof,self.model,prepared,self.columns,prefix=prefix,materialized_at='2026-10-09T00:00:00+00:00')

    def test_prefix_topology_history_and_no_dangling_edges(self):
        prepared=self.prepare()
        self.assertEqual([len(b.records)for b in prepared.batches],[21,1,2,21])
        for prefix,counts in enumerate([(11,10,0,21),(11,10,0,22),(10,9,2,24),(11,10,2,45)],1):
            rows=self.oracle(prepared,prefix)
            self.assertEqual(tuple(len(rows[k])for k in self.columns),counts)
            identities={(r['type_id'],r['id'])for r in rows['object_current']}
            for edge in rows['edge_current']:
                self.assertIn((edge['source_type'],edge['source_id']),identities)
                self.assertIn((edge['target_type'],edge['target_id']),identities)
            for role,values in rows.items():
                self.assertTrue(all(set(row)=={name for name,_ in self.columns[role]}for row in values))

    def test_new_field_absent_and_present_keep_decimal_and_registry(self):
        prepared=self.prepare();rows=self.oracle(prepared)
        registry=json.loads(prepared.registry_json)
        self.assertEqual(len(registry['properties']),35)
        self.assertEqual(len({tuple(p['identity'])for p in registry['properties']}),35)
        products=[r for r in rows['object_current']if json.loads(r['retained_json'])['original']['type']['element']=='products']
        self.assertEqual(len(products),2)
        field=next(p['property_id']for p in registry['properties']if p['identity'][-1]=='products.evolution_note')
        original=next(r for r in products if json.loads(r['retained_json'])['original_key']!='ashlar-authored-evolution-product-present')
        added=next(r for r in products if r is not original)
        self.assertNotIn(field,json.loads(original['props_json']))
        self.assertEqual(json.loads(added['props_json'])[field],'Explicit newly authored String')
        self.assertIn(':12.75',original['props_json'])
        self.assertEqual(original['entity_version'],'3');self.assertEqual(added['entity_version'],'1')
        self.assertTrue(all(r['entity_version']=='2'for r in rows['tombstone']))

    def test_namespaces_do_not_collapse_overlapping_numeric_ids(self):
        a=self.oracle(self.prepare());b=self.oracle(self.prepare('B','other'))
        self.assertEqual({r['id']for r in a['object_current']},{r['id']for r in b['object_current']})
        self.assertTrue({r['lookup_hash']for r in a['object_current']}.isdisjoint({r['lookup_hash']for r in b['object_current']}))
        self.assertEqual({r['feed']for r in b['whole_source_history']},{'B'})

    def test_breaking_and_changed_custody_refuse_before_output(self):
        with self.assertRaises(ValueError):
            tx.prepare(self.candidate,self.proof,self.model,source_system='A',epoch='e',through_revision='breaking-candidate')
        for index in range(3):
            args=[self.candidate,self.proof,self.model];args[index]+=b' '
            with self.assertRaises(ValueError):tx.prepare(*args,source_system='A',epoch='e')
        prepared=self.prepare()
        with self.assertRaises(ValueError):self.oracle(replace(prepared,registry_json='{}'))

    def test_oracle_does_not_adopt_emitted_bad_property(self):
        prepared=self.prepare();batch=prepared.batches[1];record=batch.records[0]
        event=json.loads(record.raw);event['props_json']='{"999":"wrong"}'
        raw=(tx.text(event)+'\n').encode()
        changed=replace(record,raw=raw,sha256=tx.digest(raw))
        mutated=replace(prepared,batches=(prepared.batches[0],replace(batch,records=(changed,)),*prepared.batches[2:]))
        expected=self.oracle(prepared,2);observed=self.oracle(mutated,2)
        self.assertEqual(expected['object_current'],observed['object_current'])
        self.assertNotEqual(expected['whole_source_history'],observed['whole_source_history'])

    def test_actual_apply_requires_exact_delete_carrier_and_transition_policy(self):
        from ashlar.apply import empty_state,plan_apply,ApplyError
        from ashlar.whole_entity import changes_from_batch
        prepared=self.prepare();state=empty_state()
        allowed=tuple(c for b in prepared.batches for c in changes_from_batch(b))
        def admit(change):
            if change not in allowed:raise PermissionError()
        for batch in prepared.batches[:3]:
            state=plan_apply(state,changes_from_batch(batch),schema_policy=admit)
        self.assertEqual((len(state.current),len(state.tombstones)),(19,2))
        with self.assertRaises(ApplyError):
            plan_apply(state,changes_from_batch(prepared.batches[3]),schema_policy=admit)
        calls=[]
        def transition(previous,change):
            self.assertEqual(previous.schema_revision,prepared.schema_revisions[2])
            self.assertEqual(change.state.schema_revision,prepared.schema_revisions[3])
            calls.append(change.state.key)
        after=plan_apply(state,changes_from_batch(prepared.batches[3]),schema_policy=admit,schema_transition_policy=transition)
        self.assertEqual(len(calls),19);self.assertEqual((len(after.current),len(after.tombstones),len(after.history)),(21,2,45))

    def test_graph_sql_plan_all_prefixes_matches_independent_source_cells_and_replay(self):
        from ashlar.apply import empty_state
        from ashlar.whole_entity import changes_from_batch
        from ashlar_host.graph_sql import graph_sql_plan
        prepared=self.prepare();state=empty_state();allowed=tuple(c for b in prepared.batches for c in changes_from_batch(b))
        def admit(change):
            if change not in allowed:raise PermissionError()
        def transition(previous,change):
            if (previous.schema_revision,change.state.schema_revision)!=(prepared.schema_revisions[2],prepared.schema_revisions[3]):raise PermissionError()
        tables={r:'local.prepared.'+r for r in self.columns}
        for prefix,batch in enumerate(prepared.batches,1):
            state,steps=graph_sql_plan(state,batch,tables,materialized_at='2026-10-09T00:00:00+00:00',schema_policy=admit,schema_transition_policy=transition)
            expected=self.oracle(prepared,prefix)
            self.assertEqual(len(state.history),len(expected['whole_source_history']))
            for entity in state.current.values():
                role='object_current'if entity.key.kind=='object'else'edge_current';column='type_id'if entity.key.kind=='object'else'rel_type_id'
                row=next(r for r in expected[role]if (r['source_system'],r[column],r['id'])==(entity.key.source,str(entity.key.type_id),str(entity.key.id)))
                self.assertEqual((entity.props_json,entity.retained_json,entity.version,entity.schema_revision),(row['props_json'],row['retained_json'],int(row['entity_version']),row['schema_revision']))
            self.assertTrue(steps)
            replay,empty=graph_sql_plan(state,batch,tables,materialized_at='2026-10-09T00:00:00+00:00',schema_policy=admit,schema_transition_policy=transition)
            self.assertEqual(replay,state);self.assertEqual(empty,[])

    def test_qualified_envelopes_preserve_records_and_eight_identities(self):
        identities=set()
        for source in ('A','B'):
            original=self.prepare(source,'original-'+source)
            qualified=tx.qualify(original)
            for before,after in zip(original.batches,qualified.batches):
                identities.add(after.batch_id)
                self.assertEqual(json.loads(after.batch_id),[source,'original-'+source,before.batch_id])
                self.assertEqual(tuple(r.raw for r in before.records),tuple(r.raw for r in after.records))
                self.assertEqual(after.cursor_before,'0')
                self.assertEqual(int(after.cursor_after),len(after.begin)+sum(len(r.raw) for r in after.records)+len(after.commit))
            for prefix in range(1,5):
                expected=tx.scoped_oracle(self.candidate,self.proof,self.model,qualified,self.columns,prefix=prefix,materialized_at='2026-10-09T00:00:00+00:00')
                self.assertTrue(all(row['source_system']==source for row in expected['object_current']))
        self.assertEqual(len(identities),8)

    def test_qualified_oracle_refuses_wrong_identity_or_cursor(self):
        qualified=tx.qualify(self.prepare())
        batch=qualified.batches[0]
        for changed in (replace(batch,batch_id='R1'),replace(batch,cursor_after='0'),replace(batch,records=(replace(batch.records[0],cursor='0'),*batch.records[1:]))):
            with self.assertRaises(ValueError):
                tx.scoped_oracle(self.candidate,self.proof,self.model,replace(qualified,batches=(changed,*qualified.batches[1:])),self.columns,prefix=1,materialized_at='2026-10-09T00:00:00+00:00')

if __name__=='__main__':unittest.main()
