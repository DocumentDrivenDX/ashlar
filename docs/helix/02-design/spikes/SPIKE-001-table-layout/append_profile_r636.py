"""Local calibration of immutable historical allocation plus disjoint growth IDs."""
import hashlib
import json
from pathlib import Path
from scale_mixed_r219 import Workload, node_key, compact, tokens

PROFILE = 'ashlar-scale-append/2'
OLD_NODES, OLD_EDGES = 8000000, 40000000
NEW_NODES, NEW_EDGES = 16000000, 80000000

class AppendWorkload(Workload):
    def __init__(self):
        super().__init__(NEW_NODES, NEW_EDGES)
        self.historical = Workload(OLD_NODES, OLD_EDGES)

    def carrier(self, kind, ordinal):
        prior = OLD_NODES if kind == 'node' else OLD_EDGES
        if ordinal < prior:
            return self.historical.carrier(kind, ordinal)
        r = super().carrier(kind, ordinal)
        # Existing global allocation ends at 48M. New nodes use (48M,56M].
        # New edges naturally use (56M,96M]. No old carrier is regenerated.
        if kind == 'node':
            r['id'] = str(int(r['id']) + OLD_EDGES)
            r['logical_key_json'] = compact({'synthetic_native_tuple': [r['type_id'], r['id']]})
        else:
            for side in ['source', 'target']:
                if int(r[side + '_id']) > OLD_NODES:
                    r[side + '_id'] = str(int(r[side + '_id']) + OLD_EDGES)
        r.update(schema_revision=PROFILE, source_epoch=PROFILE, apply_batch_id='append-calibration')
        field = 'type_id' if kind == 'node' else 'rel_type_id'
        r['lookup_hash'] = hashlib.sha256(compact({'source_system': r['source_system'], field: int(r[field]), 'id': int(r['id'])}).encode()).hexdigest()
        return r

def calibrate():
    w = AppendWorkload()
    counts = {'historical': 0, 'new': 0, 'new_endpoint_references': 0}
    digest = hashlib.sha256()
    for kind, old, total in [('node', OLD_NODES, NEW_NODES), ('edge', OLD_EDGES, NEW_EDGES)]:
        # Every residue/property shape plus both allocation boundaries; scoped sample.
        indices = sorted(set(range(1024)) | set(range(old-1024, old+1024)) | set(range(total-1024, total)))
        for ordinal in indices:
            r = w.carrier(kind, ordinal)
            historical = ordinal < old
            if historical:
                assert r == w.historical.carrier(kind, ordinal)
            else:
                lower, upper = (48000000, 56000000) if kind == 'node' else (56000000, 96000000)
                assert lower < int(r['id']) <= upper
            counts['historical' if historical else 'new'] += 1
            if kind == 'edge':
                for side in ['source', 'target']:
                    ident = int(r[side+'_id'])
                    assert 1 <= ident <= OLD_NODES or 48000000 < ident <= 56000000
                    logical = ident-1 if ident <= OLD_NODES else ident-OLD_EDGES-1
                    key = node_key(logical)
                    assert key[:2] == (r['source_system'], r[side+'_type'])
                    node = w.carrier('node', logical)
                    assert node['id'] == str(ident)
                    counts['new_endpoint_references'] += ident > OLD_NODES
            replay = {}
            rows = list(w.roles(kind, ordinal))
            if historical:
                assert rows == list(w.historical.roles(kind, ordinal))
            for role, row in rows:
                digest.update((compact([role,row])+'\n').encode())
                if role == 'source_record':
                    assert json.loads(row['payload_json'])['carrier'] == r
                    assert hashlib.sha256(row['payload_json'].encode()).hexdigest() == row['payload_digest']
                if role == 'property_journal':
                    assert row['id'] == r['id'] and row['source_epoch'] == r['source_epoch']
                    replay[row['property_id']] = row['new_json']
            assert replay == tokens(r['props_json'])
    return {'format':'ashlar-append-calibration/1', 'profile': PROFILE, 'counts': counts,
            'role_stream_sha256':digest.hexdigest(), 'old_graph':[OLD_NODES,OLD_EDGES], 'proposed_graph':[NEW_NODES,NEW_EDGES],
            'allocation':{'old_nodes':[1,8000000], 'old_edges':[8000001,48000000], 'new_nodes':[48000001,56000000], 'new_edges':[56000001,96000000]},
            'qualification':'Local boundary/residue sample, not full-scale proof or native admission. Existing carrier and role content is byte-equivalent. New raw envelope retains historical generator profile while carrier schema_revision/source_epoch explicitly identify append profile. Native SQL must independently implement this mapping; old extent-based SQL slicing must not be reused. No canonical tables or published pins changed.'}

if __name__ == '__main__':
    b = Path(__file__).resolve().parent
    result = calibrate()
    (b/'out/append-calibration-r636.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
