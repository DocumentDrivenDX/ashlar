"""Private finite development source policy, backed by fresh public UMF calls.

This port admits only the exact prepared fixture changes and prior carriers. It
does not establish producer/source authority, migration durability or ACK truth;
the surrounding protected publication workflow must supply those independently.
"""
import json
from pathlib import Path
import subprocess
import tempfile
import zlib

from ashlar.apply import EntityKey,EntityState
from ashlar.whole_entity import changes_from_batch
from commerce_evolution_transactions import prepare,independent_oracle,digest,PRESERVATION_SHA,CANDIDATE_SHA
from fixture_oracle import fixture_columns

ROOT=Path(__file__).resolve().parents[1]
PIN='e44cd15f336dfb33db35acf20eee13dd120a1a28'

def bounded_bytes(path):
    with Path(path).open('rb')as stream:raw=stream.read(4_000_001)
    if len(raw)>4_000_000:raise PermissionError('Bounded original source/receipt bytes required')
    return raw

class CommerceEvolutionAdmission:
    def __init__(self,candidate_path,public_path,model_path,*,umf_repo,source_system,epoch):
        self.paths=tuple(Path(p).resolve()for p in (candidate_path,public_path,model_path))
        original=tuple(bounded_bytes(p)for p in self.paths)
        if any(len(raw)>4_000_000 for raw in original):raise PermissionError('Bounded original source/receipt bytes required')
        proof=original[1]
        if self.paths[1].suffix=='.gz':
            decoder=zlib.decompressobj(31);proof=decoder.decompress(proof,4_000_001)
            if len(proof)>4_000_000 or not decoder.eof or decoder.unused_data:raise PermissionError('Bounded exact single compressed receipt required')
        self.prepared=prepare(original[0],proof,original[2],source_system=source_system,epoch=epoch)
        self.hashes=tuple(digest(raw)for raw in original);self.umf_repo=Path(umf_repo).resolve()
        self._custody()
        # Genuine public inspectors and expected-source verifiers execute again.
        # Retained receipt booleans alone are never the runtime admission gate.
        archive=ROOT/'docs/helix/04-build/evidence'
        with tempfile.TemporaryDirectory(prefix='ashlar-commerce-evolution-public-')as temporary:
            output=Path(temporary)/'receipt.json'
            result=subprocess.run(['bun',str(ROOT/'tools/prepare_commerce_present_field.ts'),str(self.umf_repo),str(archive/'commerce-evolution-preparation-20261009/candidate.json'),str(archive/'commerce-public-revision-preservation-20261009/public-revisions.json.gz'),str(output)],cwd=ROOT,capture_output=True,check=False,timeout=60)
            if result.returncode!=0 or not output.is_file()or digest(bounded_bytes(output))!=PRESERVATION_SHA:raise PermissionError('Fresh exact public dataset/source preservation verification required')
        self._custody()
        self.changes=tuple(c for b in self.prepared.batches for c in changes_from_batch(b))
        expected=independent_oracle(original[0],proof,original[2],self.prepared,fixture_columns(ROOT),prefix=3,materialized_at='2026-10-09T00:00:00+00:00')
        prior={}
        for role in ('object_current','edge_current'):
            kind='object'if role=='object_current'else'edge';type_name='type_id'if kind=='object'else'rel_type_id'
            for row in expected[role]:
                key=EntityKey(row['source_system'],kind,int(row[type_name]),int(row['id']))
                endpoints=tuple(EntityKey(key.source,'object',int(row[prefix+'_type']),int(row[prefix+'_id']))for prefix in ('source','target'))if kind=='edge'else None
                prior[key]=EntityState(key,int(row['entity_version']),row['schema_revision'],row['props_json'],row['retained_json'],endpoints)
        self.transitions=tuple((prior[c.state.key],c)for c in changes_from_batch(self.prepared.batches[3])if c.state.key in prior)
        if len(self.transitions)!=19:raise PermissionError('Exact original surviving-entity transition inventory required')
        self.facts={'profile':'ashlar-commerce-evolution-private-source-admission/0.1','qualification':__doc__,'umf_revision':PIN,'candidate_sha256':CANDIDATE_SHA,'public_receipt_sha256':PRESERVATION_SHA,'source_system':source_system,'epoch':epoch,'registry_sha256':digest(self.prepared.registry_json.encode()),'schema_revisions':list(self.prepared.schema_revisions),'deliveries':[{'feed':c.feed,'epoch':c.epoch,'delivery_id':c.delivery_id,'raw_sha256':c.raw_digest}for c in self.changes],'source_preservation':'Fresh public complete datasets and exact expected-source verifiers; fixture data transitions require exact independent prior/source rows, not model compatibility inference.'}

    def _custody(self):
        if tuple(digest(bounded_bytes(p))for p in self.paths)!=self.hashes:raise PermissionError('Original fixture/public receipt byte custody changed')
        for args,expected in ((['rev-parse','HEAD'],PIN),(['status','--porcelain'],'')):
            result=subprocess.run(['git','-C',str(self.umf_repo),*args],capture_output=True,text=True,check=False)
            if result.returncode or result.stdout.strip()!=expected:raise PermissionError('Exact clean public semantic producer required')

    def metadata(self):
        self._custody();return json.loads(json.dumps(self.facts))

    def admit(self,change):
        self._custody()
        if change not in self.changes:raise PermissionError('Exact original source change required')

    def admit_transition(self,previous,change):
        self.admit(change)
        if (previous,change)not in self.transitions:raise PermissionError('Exact independently derived prior entity/source transition required')
        self._custody()
