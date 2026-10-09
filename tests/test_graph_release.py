import base64,copy,hashlib,json
from pathlib import Path
import unittest
from ashlar.graph_release import (NODE_INTS,EDGE_INTS,NODE_TEXT,EDGE_TEXT,
                                 read_graph_release,graph_key,decode_graph_key)
from ashlar.native import SQLResult
from ashlar.pins import PinVector
from ashlar.publication import ResolutionError
from test_singleton import Pins
from test_publication import FakeBackend,TABLE,OTHER


def carrier(kind,identity,source='snow 雪',target='2'):
    ints=NODE_INTS if kind=='node' else EDGE_INTS
    texts=NODE_TEXT if kind=='node' else EDGE_TEXT
    row={name:'1' for name in ints};row.update({name:'exact' for name in texts})
    row.update(source_system=source,id=identity,props_json=' {"23":9007199254740993,"24":-0.00} ',retained_json='{"future":{"value":null}}',published_at='-1',source_position=None,apply_batch_id=None,source_cursor_json='{"tuple":["a",1]}',source_delivery_id='delivery',source_feed='feed',source_epoch='epoch',schema_revision='r1')
    if kind=='node':row['root_id']=None
    else:row.update(source_type='1',source_id='1',target_type='1',target_id=target,order_key=None)
    typed='type_id' if kind=='node' else 'rel_type_id'
    row['lookup_hash']=hashlib.sha256(json.dumps({'source_system':source,typed:int(row[typed]),'id':int(identity)},ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
    return row


class Policy:
    def __init__(self,pins):self.pins=pins;self.calls=[];self.deny=False;self.expire=False
    def bind_descriptor(self,*args):
        assert self.pins.active;self.calls.append('bind')
        if self.expire and self.calls.count('bind')>1:raise PermissionError('Expired source custody')
    def authorize_graph_row(self,d,t,row,c):
        assert self.pins.active
        if self.deny:raise PermissionError('Denied semantic/row profile')
        self.calls.append('row')
        with unittest.TestCase().assertRaises(TypeError):row['id']='changed'
    def authorize_graph_result(self,d,n,e,c):
        assert self.pins.active;self.calls.append('result')
        if n:
            with unittest.TestCase().assertRaises(TypeError):n[0]['id']='changed'


class Executor:
    def __init__(self,pins):
        self.p=pins;self.calls=[];self.replace=False;self.replace_nodes_on_edge=False;self.edge_read=False
        self.nodes=[carrier('node','1'),carrier('node','2'),carrier('node','3'),carrier('node','1',source='another')]
        self.edges=[carrier('edge','7'),carrier('edge','8'),carrier('edge','9',target='1')]
    def query(self,sql,params):
        assert self.p.active;self.calls.append(sql)
        if sql.startswith('DESCRIBE'):
            return SQLResult([{'id':'replaced' if (self.replace and len(self.calls)>2) or (self.replace_nodes_on_edge and self.edge_read and 'object_current' in sql) else 'trusted-uuid'}])
        kind='node' if 'object_current' in sql else 'edge'
        if kind=='edge':self.edge_read=True
        names=(NODE_INTS+NODE_TEXT if kind=='node' else EDGE_INTS+EDGE_TEXT)+('published_at',)
        return SQLResult(self.nodes if kind=='node' else self.edges,tuple((n,'STRING') for n in names))


class GraphReleaseTests(unittest.TestCase):
    def setup(self):
        p=Pins();e=Executor(p);b=FakeBackend();policy=Policy(p)
        v=PinVector('a','manifest','p1','a'*64,{TABLE:('trusted-uuid',6),OTHER:('trusted-uuid',2)})
        return p,e,b,policy,v
    def read(self,p,e,b,policy,v,**kw):
        return read_graph_release(e,b,p,v,policy,publication_id='p1',node_table=TABLE,edge_table=OTHER,context='authorized',supported_profiles=['ashlar-delta/0.3'],supported_revisions={'source':['r1']},max_nodes=kw.get('max_nodes',10),max_edges=kw.get('max_edges',10))
    def test_complete_reversible_graph_and_exact_carriers(self):
        p,e,b,policy,v=self.setup();result=self.read(p,e,b,policy,v)
        self.assertFalse(p.active);self.assertEqual(hashlib.sha256(result.payload).hexdigest(),result.sha256)
        release=json.loads(result.payload);nodes=release['nodes'];edges=release['edges']
        self.assertEqual((len(nodes),len(edges)),(4,3))
        self.assertEqual(len({r['graph_id'] for r in nodes}),4)
        self.assertEqual(sum(r['src']==r['dst'] for r in edges),1)
        self.assertEqual(sum(r['dst']==graph_key('node','snow 雪','1','2') for r in edges),2)
        self.assertEqual(nodes[0]['props_json'],e.nodes[0]['props_json'])
        self.assertEqual(release['publication']['validation_report_json'],b.rows[0]['validation_report_json'])
        self.assertEqual(release['snapshots'][TABLE],{'uuid':'trusted-uuid','version':6})
        self.assertTrue(any('VERSION AS OF 6' in s for s in e.calls));self.assertTrue(any('VERSION AS OF 2' in s for s in e.calls))
        for row in nodes+edges:self.assertEqual(graph_key(*decode_graph_key(row['graph_id'])),row['graph_id'])
        e.nodes.reverse();e.edges.reverse();self.assertEqual(self.read(p,e,b,policy,v).payload,result.payload)
    def test_invalid_identity_profiles_refuse(self):
        for args in [('node','snow 雪','01','1'),('node','x','1',str(2**63)),('node','x',True,'1'),('bad','x','1','1')]:
            with self.assertRaises(ResolutionError):graph_key(*args)
        key=graph_key('node','x:y','-1','9007199254740993')
        self.assertEqual(decode_graph_key(key),('node','x:y','-1','9007199254740993'))
        for bad in [key+'=',key+'!',key.replace('ashlar-key/1','unknown')]:
            with self.assertRaises(ResolutionError):decode_graph_key(bad)
    def test_full_release_refuses_budget_duplicate_dangling_corrupt_and_expiry(self):
        for fail in ['budget','duplicate-node','duplicate-edge','dangling','hash','numeric','missing','null','replace','node-replaced-during-edge','deny','expire','pin-exit','version']:
            p,e,b,policy,v=self.setup();kw={}
            if fail=='budget':kw['max_nodes']=2
            if fail=='duplicate-node':e.nodes.append(copy.deepcopy(e.nodes[0]))
            if fail=='duplicate-edge':e.edges.append(copy.deepcopy(e.edges[0]))
            if fail=='dangling':e.edges[0]['target_id']='999'
            if fail=='hash':e.nodes[0]['lookup_hash']='0'*64
            if fail=='numeric':e.nodes[0]['id']=9007199254740993
            if fail=='missing':del e.nodes[0]['retained_json']
            if fail=='null':e.nodes[0]['published_at']=None
            if fail=='replace':e.replace=True
            if fail=='node-replaced-during-edge':e.replace_nodes_on_edge=True
            if fail=='deny':policy.deny=True
            if fail=='expire':policy.expire=True
            if fail=='pin-exit':p.fail_exit=True
            if fail=='version':b.version_delta=1
            with self.subTest(fail=fail),self.assertRaises((ResolutionError,PermissionError)):self.read(p,e,b,policy,v,**kw)
            self.assertFalse(p.active)
    def test_old_release_bytes_remain_stable_with_successor_values(self):
        p,e,b,policy,v=self.setup();old=self.read(p,e,b,policy,v);old_bytes=old.payload
        e.nodes[0]['props_json']='{"23":"successor"}'
        b.rows[0]['publication_id']='p2'
        # Reader refuses obsolete request rather than silently selecting latest.
        with self.assertRaises(ResolutionError):self.read(p,e,b,policy,v)
        self.assertEqual(old.payload,old_bytes)

    def test_empty_release_and_closing_refusal(self):
        p,e,b,policy,v=self.setup();e.nodes=[];e.edges=[]
        release=json.loads(self.read(p,e,b,policy,v).payload)
        self.assertEqual((release['nodes'],release['edges']),([],[]))
        self.assertIn('result',policy.calls)
        def refuse(*args):raise PermissionError('Incomplete graph coverage')
        policy.authorize_graph_result=refuse
        with self.assertRaises(PermissionError):self.read(p,e,b,policy,v)
        self.assertFalse(p.active)
    def test_exact_signed64_identity_boundaries(self):
        p,e,b,policy,v=self.setup()
        e.nodes=[carrier('node',str(-(2**63))),carrier('node',str(2**63-1))];e.edges=[]
        nodes=json.loads(self.read(p,e,b,policy,v).payload)['nodes']
        self.assertEqual({decode_graph_key(r['graph_id'])[3] for r in nodes},{str(-(2**63)),str(2**63-1)})

    def test_atomic_immutable_local_persistence_and_interrupted_write(self):
        import tempfile
        from unittest.mock import patch
        from ashlar.graph_release import persist_graph_release,GraphRelease
        p,e,b,policy,v=self.setup();release=self.read(p,e,b,policy,v)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with patch('ashlar.graph_release.os.link',side_effect=OSError('Interrupted exposure')):
                with self.assertRaises(OSError):persist_graph_release(root,release)
            self.assertEqual(list(root.iterdir()),[])
            path=persist_graph_release(root,release);self.assertEqual(path.read_bytes(),release.payload)
            self.assertEqual(persist_graph_release(root,release),path)
            path.write_bytes(b'conflicting bytes')
            with self.assertRaises(ResolutionError):persist_graph_release(root,release)
            self.assertEqual(path.read_bytes(),b'conflicting bytes')
            with self.assertRaises(ResolutionError):persist_graph_release(root,GraphRelease(b'wrong',release.sha256))
