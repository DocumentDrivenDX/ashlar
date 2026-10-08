import unittest
from ashlar.protocol import ReaderProtocolProfile,ProtocolError,validate_protocol_detail

class ProtocolTests(unittest.TestCase):
    def profile(self):return ReaderProtocolProfile('selected-reader',(3,),(7,),('deletionVectors','clustering'))
    def detail(self):return {'id':'original','format':'delta','minReaderVersion':'3','minWriterVersion':'7','tableFeatures':'["deletionVectors","clustering"]'}
    def test_selected_reader_and_features_are_defensive_and_explicit(self):
        features=['deletionVectors','clustering'];profile=ReaderProtocolProfile('reader',(3,),(7,),features)
        features.append('future')
        self.assertEqual(validate_protocol_detail(self.detail(),uuid='original',profile=profile)['features'],['clustering','deletionVectors'])
        with self.assertRaises(ProtocolError):validate_protocol_detail(dict(self.detail(),tableFeatures='["future"]'),uuid='original',profile=profile)
    def test_unknown_missing_noncanonical_and_numeric_aliases_refuse(self):
        for fields in [dict(id='other'),dict(format='parquet'),dict(minReaderVersion='03'),dict(minReaderVersion=3),dict(minReaderVersion=True),dict(minWriterVersion='8'),dict(tableFeatures='null'),dict(tableFeatures='["clustering","clustering"]'),dict(tableFeatures='[true]')]:
            with self.assertRaises(ProtocolError):validate_protocol_detail(dict(self.detail(),**fields),uuid='original',profile=self.profile())
        with self.assertRaises(ProtocolError):ReaderProtocolProfile('reader',(True,),(7,),())
