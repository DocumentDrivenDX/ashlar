import json
from dataclasses import replace
from pathlib import Path
from umf_access.related_keys import validate_decoder, make_related_result_model
from umf_access.codec_support import make_projection_model
from umf_access.types import CodecError
from native_related.keys import CustomerKey, OrderKey
from native_related.models import Order
from ashlar.sdk_related_keys import typed_related_keys
from ashlar.weft_related_keys_decode import decode_related_keys, RelatedKeysDecodeConfig
program=json.loads(Path('/private/tmp/weft-combined-sdk-local-r12/ashlar-related-keys.json').read_text())
decoder=program['outputs'][1]['decoder'];validate_decoder(decoder,CustomerKey)
raw=b'{"items":[["customer-b"],["customer-b"]],"truncated":true}'
native=decode_related_keys(decoder['representation'],raw,model_pins=decoder['modelPins'],config=RelatedKeysDecodeConfig(8192))
result=typed_related_keys(native,decoder,CustomerKey)
assert tuple(type(v) for v in result.items)==(CustomerKey,CustomerKey)
assert tuple(v.customer_id for v in result.items)==('customer-b','customer-b') and result.truncated is True
for value,key in ((replace(native,items=(('different',),('different',))),CustomerKey),(native,OrderKey)):
 try:typed_related_keys(value,decoder,key)
 except (ValueError,CodecError):pass
 else:raise AssertionError('carrier/source mismatch must refuse')
row=make_projection_model('OrderId',(('id',Order,'id'),))
model=make_related_result_model('OrderCustomers',row,CustomerKey,'related',decoder,Order)
validated=model.model_validate({'id':'o-1','related':result});assert validated.related==result
from ashlar.access import _Policy
from time import monotonic
class CustodySpy:
 def authorize_result(self,rows,descriptor,context):
  self.rows=rows
spy=CustodySpy()
policy=_Policy(spy,None,json.dumps(program),model,monotonic()+60)
rows=(("o-1",native),)
policy.authorize_result(rows,None,None)
assert spy.rows is rows and spy.rows[0][1] is native
assert policy.validated[0].related==result
print(json.dumps({'checks':['actual compiled source descriptor matches generated CustomerKey','original native decoder exact bytes preserved','duplicate occurrences preserved as typed generated Keys','altered native bytes/items refuses','wrong generated Key source refuses','sealed row transport validates','original authority receives unchanged native rows while typed validation sees generated Keys'], 'nativeQualification':False}))
