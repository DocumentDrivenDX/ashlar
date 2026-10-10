import ast, dataclasses, gzip, hashlib, json, pathlib, sys, unittest
ROOT=pathlib.Path('/Users/erik/Projects/ashlar')
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'tools'),str(ROOT/'tests')]
from ashlar import commerce_evolution as new
import commerce_evolution_transactions as old
import commerce_evolution_batch_scope as scope
from fixture_oracle import fixture_columns
FILES=['src/ashlar/commerce_evolution.py','tests/test_portable_commerce_evolution.py','tools/commerce_evolution_transactions.py','tools/commerce_evolution_batch_scope.py']
def descriptor(p):
    raw=p.read_bytes();return {'path':str(p),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
opening=[descriptor(ROOT/p) for p in FILES]
E=ROOT/'docs/helix/04-build/evidence'
c=(E/'commerce-present-field-preparation-20261009/candidate.json').read_bytes()
p=gzip.decompress((E/'commerce-present-field-preparation-20261009/public-presence.json.gz').read_bytes())
m=(E/'commerce-evolution-preparation-20261009/original-model.json').read_bytes()
columns=fixture_columns(ROOT);clock='2026-10-09T00:00:00+00:00'
assert hashlib.sha256(m).hexdigest()==new.ORIGINAL_MODEL_SHA==old.SOURCE_SHA
asts={p:{node.name:node for node in ast.parse((ROOT/p).read_text()).body if isinstance(node,(ast.FunctionDef,ast.ClassDef))} for p in (FILES[0],FILES[2],FILES[3])}
matched=[]
for path,names in [(FILES[2],['text','digest','rows','model_bytes','numeric_token','properties_from_public_input','PreparedEvolution','prepare','independent_oracle']),(FILES[3],['qualify','scoped_oracle'])]:
    for name in names:
        assert ast.dump(asts[FILES[0]][name])==ast.dump(asts[path][name]),name
        matched.append(name)
assert new.BATCH_IDENTITY_PROFILE==scope.PROFILE
old_prepared={};new_prepared={};identities=set();prefix_receipts=[]
for label in ('a','b'):
    args={'source_system':'commerce-evolution-'+label,'epoch':'private-original-'+label}
    a=old.prepare(c,p,m,**args);b=new.prepare(c,p,m,**args)
    assert dataclasses.asdict(a)==dataclasses.asdict(b)
    old_prepared[label]=scope.qualify(a);new_prepared[label]=new.qualify(b)
    assert dataclasses.asdict(old_prepared[label])==dataclasses.asdict(new_prepared[label])
    for before,after in zip(a.batches,new_prepared[label].batches):
        identities.add(after.batch_id)
        assert tuple((r.delivery_id,r.raw,r.sha256)for r in before.records)==tuple((r.delivery_id,r.raw,r.sha256)for r in after.records)
        offset=len(after.begin)
        for r in after.records:
            offset+=len(r.raw);assert r.cursor==str(offset)
        assert after.cursor_before=='0' and after.cursor_after==str(offset+len(after.commit))
assert len(identities)==8
prefixes={'a':0,'b':0}
for index in range(4):
    for label in ('a','b'):
        prefixes[label]=index+1
        expected={k:[]for k in columns};actual={k:[]for k in columns}
        for source,count in prefixes.items():
            if not count:continue
            a=scope.scoped_oracle(c,p,m,old_prepared[source],columns,prefix=count,materialized_at=clock)
            b=new.scoped_oracle(c,p,m,new_prepared[source],columns,prefix=count,materialized_at=clock)
            assert a==b
            for role in columns:expected[role].extend(a[role]);actual[role].extend(b[role])
        assert expected==actual
        raw=json.dumps(actual,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
        prefix_receipts.append({'prefixes':dict(prefixes),'counts':{k:len(v)for k,v in actual.items()},'oracle_sha256':hashlib.sha256(raw).hexdigest()})
negative=[]
for index in range(3):
    args=[c,p,m];args[index]+=b' '
    try:new.prepare(*args,source_system='commerce-evolution-a',epoch='private-original-a')
    except ValueError:negative.append('changed-original-'+str(index))
    else:raise AssertionError('accepted changed original')
prepared=new_prepared['a'];batch=prepared.batches[0]
mutations={
 'batch-id':dataclasses.replace(batch,batch_id='R1'),
 'feed':dataclasses.replace(batch,feed='other'),
 'epoch':dataclasses.replace(batch,epoch='other'),
 'inner-ordinal':dataclasses.replace(batch,cursor_after='1'),
 'cursor-before':dataclasses.replace(batch,cursor_before='1'),
 'record-cursor':dataclasses.replace(batch,records=(dataclasses.replace(batch.records[0],cursor='1'),*batch.records[1:])),
 'record-raw':dataclasses.replace(batch,records=(dataclasses.replace(batch.records[0],raw=batch.records[0].raw+b' '),*batch.records[1:])),
 'extra-record':dataclasses.replace(batch,records=(*batch.records,batch.records[0])),
 'missing-record':dataclasses.replace(batch,records=batch.records[:-1]),
 'begin':dataclasses.replace(batch,begin=batch.begin+b' '),
 'commit':dataclasses.replace(batch,commit=batch.commit+b' '),
}
for name,mutated in mutations.items():
    changed=dataclasses.replace(prepared,batches=(mutated,*prepared.batches[1:]))
    try:new.scoped_oracle(c,p,m,changed,columns,prefix=1,materialized_at=clock)
    except ValueError:negative.append(name)
    else:raise AssertionError('accepted '+name)
# A caller-mutated decoded candidate/proof returned by input decoding must not
# affect fresh preparation from immutable original bytes.
a,b=new.admitted_inputs(c,p,m);a['registry']['properties'].clear();b.clear()
assert dataclasses.asdict(new.qualify(new.prepare(c,p,m,source_system='commerce-evolution-a',epoch='private-original-a')))==dataclasses.asdict(prepared)
negative.append('decoded-input-alias-does-not-affect-next-call')
suite=unittest.defaultTestLoader.loadTestsFromName('test_portable_commerce_evolution')
result=unittest.TextTestRunner(verbosity=2).run(suite)
assert result.wasSuccessful()
assert opening==[descriptor(ROOT/p)for p in FILES]
receipt={'format':'astra-portable-commerce-evolution-source-review/0.1','verdict':'PASS scoped source-only extraction compatibility','scope':'No Git/compiler/native/SDK/network execution. Existing tool functions are compatibility references, not new native proof. No source edits.','runtime':sys.version,'files':opening,'inputs':[{'role':k,'bytes':len(v),'sha256':hashlib.sha256(v).hexdigest()} for k,v in [('candidate',c),('public-proof-uncompressed',p),('original-model',m)]],'unchanged_function_asts':matched,'eight_prefix_full_row_parity':prefix_receipts,'negative_controls':negative,'owner_tests':result.testsRun,'closing_sources_unchanged':True}
path=pathlib.Path('/private/tmp/astra-portable-commerce-evolution-review-20261010-a.json');path.write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(descriptor(path)));print('PASS 8 combined prefix exact-row/parsing parity, 15 input/envelope controls, 9 owner tests')
