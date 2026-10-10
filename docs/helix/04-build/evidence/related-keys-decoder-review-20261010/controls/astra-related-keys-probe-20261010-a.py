import copy, json, sys, unittest
from unittest.mock import patch
from ashlar import weft_related_keys_decode as d

P=[{'documentId':'doc','revision':'r','sha256':'a'*64,'umfVersion':'0.8.0'}]
def rep(n=1,bound=3):
 return {'kind':'relatedKeys','relationship':{'documentId':'doc','revision':'r','module':'m','relationship':'rel'},'key':{'id':'pk','fields':[{'documentId':'doc','revision':'r','module':'keys','element':'k'+str(i)} for i in range(n)],'types':[{'family':'string','facets':{},'nullable':False} for _ in range(n)]},'bound':bound}
def decode(raw=b'{"items":[],"truncated":false}',r=None,p=None,limit=1024*1024):
 return d.decode_related_keys(rep() if r is None else r,raw,model_pins=P if p is None else p,config=d.RelatedKeysDecodeConfig(limit))
class Probe(unittest.TestCase):
 def test_closed_metadata_each_nested_object(self):
  for loc in ((),('relationship',),('key',),('key','fields',0),('key','types',0)):
   for action in ('extra','missing'):
    r=rep();v=r
    for k in loc:v=v[k]
    if action=='extra':v['secret-input']='secret-input'
    else:del v[next(iter(v))]
    with self.subTest(loc=loc,action=action),self.assertRaises(d.RelatedKeysDecodeError) as caught:decode(r=r)
    self.assertNotIn('secret-input',str(caught.exception))
 def test_complete_identity_pin_refusals_and_cross_module_positive(self):
  decode(r=rep()) # Field may be authored in another module in same document/revision.
  for path in [('relationship','documentId'),('relationship','revision'),('relationship','module'),('relationship','relationship'),('key','id'),('key','fields',0,'revision'),('key','fields',0,'element')]:
   for value in ('','\0','\udfff'):
    r=rep();v=r
    for k in path[:-1]:v=v[k]
    v[path[-1]]=value
    with self.subTest(path=path,value=repr(value)),self.assertRaises(d.RelatedKeysDecodeError):decode(r=r)
  for change in ({'sha256':'A'*64},{'umfVersion':'1.0.0'},{'revision':'changed'},{'sha256':True}):
   with self.subTest(change=change),self.assertRaises(d.RelatedKeysDecodeError):decode(p=[dict(P[0],**change)])
  decode(p=[dict(P[0],umfVersion='0.7.0')])
 def test_unicode_binary_order_without_normalization(self):
  values=['','a','e\u0301','é','\ue000','\U00010000','\U0001f642']
  expected=sorted(values,key=lambda x:x.encode('utf8'))
  raw=json.dumps({'items':[[x] for x in expected],'truncated':False},ensure_ascii=False).encode()
  result=decode(raw,rep(bound=len(values)))
  self.assertEqual(result.items,tuple((x,) for x in expected));self.assertEqual(result.original,raw)
  self.assertIn(('e\u0301',),result.items);self.assertIn(('é',),result.items)
  values=[['\U00010000'],['\ue000']]
  with self.assertRaises(d.RelatedKeysDecodeError):decode(json.dumps({'items':values,'truncated':False}).encode())
 def test_numeric_parser_callback_and_deep_input(self):
  called=[]
  def refuse(token):called.append(len(token));raise d.RelatedKeysDecodeError('numeric-refused')
  with patch.object(d,'_number',refuse):
   with self.assertRaises(d.RelatedKeysDecodeError):decode(b'{"items":[['+b'9'*100000+b']],"truncated":false}')
  self.assertEqual(called,[100000])
  with self.assertRaises(d.RelatedKeysDecodeError):decode(b'['*2000+b'[]'+b']'*2000)
 def test_boundaries_and_full_prefix_not_complete_bag_claim(self):
  items=[['same']]*1000
  for marker in (False,True):
   raw=json.dumps({'items':items,'truncated':marker}).encode();out=decode(raw,rep(bound=1000),limit=len(raw));self.assertEqual(len(out.items),1000);self.assertIs(out.truncated,marker)
   with self.assertRaises(d.RelatedKeysDecodeError):decode(raw,rep(bound=1000),limit=len(raw)-1)
  with self.assertRaises(d.RelatedKeysDecodeError):decode(b'{"items":[["a"]],"truncated":true}',rep(bound=2))
 def test_snapshot_precedes_parser_callback(self):
  r=rep(1,2);pins=copy.deepcopy(P);loads=d.json.loads
  def parse(*a,**kw):
   r['bound']=1;r['key']['fields'].clear();pins[0]['revision']='changed'
   return loads(*a,**kw)
  with patch.object(d.json,'loads',parse):out=decode(b'{"items":[["a"],["a"]],"truncated":false}',r,pins)
  self.assertEqual(out.items,(('a',),('a',)))
 def test_utf8_only_privacy_and_no_sdk(self):
  raw='{"items":[["secret-input"]],"truncated":false}'
  for encoding in ('utf-16','utf-16-le','utf-16-be','utf-32','utf-32-le','utf-32-be'):
   with self.subTest(encoding=encoding),self.assertRaises(d.RelatedKeysDecodeError) as caught:decode(raw.encode(encoding))
   self.assertNotIn('secret-input',str(caught.exception))
  for cell in (b'\xef\xbb\xbf'+raw.encode(),b'\xffsecret-input',b'{"items":[["secret-input"]],"truncated":NaN}'):
   with self.assertRaises(d.RelatedKeysDecodeError) as caught:decode(cell)
   self.assertNotIn('secret-input',str(caught.exception))
  self.assertFalse(any(x=='pyspark' or x.startswith('pyspark.') or x=='delta' or x.startswith('delta.') for x in sys.modules))
if __name__=='__main__':unittest.main(verbosity=2)
