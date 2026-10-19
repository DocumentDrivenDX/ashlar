# SPDX-License-Identifier: MIT OR Apache-2.0
"""Source-only transports against actual original engine0.2 compiler outputs."""
import copy,json,unittest
from pathlib import Path
from ashlar.publication import ResolutionError
from ashlar.sdk_presence import decode_presence
from ashlar.sdk_ordered_page import admit_metadata,decoded_cursor
from umf_access.types import MISSING

ROOT=Path(__file__).parent/'fixtures'/'sdk_engine02'
class OriginalPresenceControls(unittest.TestCase):
    def presence(self):return json.loads((ROOT/'presence.json').read_text())['artifact']
    def page(self):return json.loads((ROOT/'page.json').read_text())
    def test_required_optional_and_physical_null_are_distinct(self):
        a=self.presence();required,optional=a['columns']
        for value in [None,'{"state":"absent"}','{"state":"null"}']:
            with self.assertRaises(ResolutionError):decode_presence(required,value,a)
        for text,state,value in [('{"state":"absent"}','missing',None),('{"state":"null"}','null',None),('{"state":"value","value":""}','present',''),('{"state":"value","value":"雪"}','present','雪')]:
            decoded=decode_presence(optional,text,a);self.assertEqual((decoded.state,decoded.value,decoded.original),(state,value,text))
        for value in [None,'{"state":"value","value":true}','{"state":"null","value":null}','{"state":"unknown"}']:
            with self.assertRaises(ResolutionError):decode_presence(optional,value,a)
    def test_original_field_source_and_native_profile_are_required(self):
        a=self.presence()
        for mutate in [lambda a:a['backend'].update(interfaceVersion='weft-backend/0.2.0'),lambda a:a['columns'][0].update(sourceIdentities=[]),lambda a:a['columns'][0]['representation'].update(nativeNull=False)]:
            b=copy.deepcopy(a);mutate(b)
            with self.assertRaises(ResolutionError):decode_presence(b['columns'][0],'{"state":"value","value":"id"}',b)
    def test_full_nullish_order_state_and_selected_key_slots(self):
        p=self.page();a=p['artifact'];guard=next(o['parameters']for o in a['obligations']if o['id']=='ashlar.candidate.orderedPage');slots=admit_metadata(a,guard);self.assertEqual(p['page']['keySlots'],[1,2]);row=[None]*len(a['columns'])
        for state,text in [('missing','{"state":"absent"}'),('null','{"state":"null"}')]:
            for i,token in enumerate([text,'{"state":"value","value":"A"}','{"state":"value","value":"3"}']):
                slot=slots[i];row[slot['valueOutput']]=decode_presence(a['columns'][slot['valueOutput']],token,a);row[slot['stateOutput']]=state if i==0 else 'present'
            values,owner=decoded_cursor(tuple(row),p['page'],a);self.assertIs(values[0],MISSING if state=='missing'else None);self.assertEqual(owner['key'],[{'state':'present','carrier':'string','value':'A'},{'state':'present','carrier':'string','value':'3'}]);self.assertEqual(owner['sortValues'][0],{'state':state})
        row[slots[0]['stateOutput']]='missing'
        with self.assertRaises(ResolutionError):decoded_cursor(tuple(row),p['page'],a)
if __name__=='__main__':unittest.main()
