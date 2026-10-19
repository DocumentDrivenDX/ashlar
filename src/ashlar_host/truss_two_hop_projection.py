# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact original two-hop root-key row bag; no effect or compiler authority."""
import json
from collections import Counter
from dataclasses import dataclass
from ashlar.weft_decode import decode_exact_scalar
from .delta_custody import encoded

@dataclass(frozen=True)
class TrussTwoHopProjection:
    columns_json:str
    expected_rows_json:str
    def decode(self,column,value):
        if encoded(column)not in [encoded(c)for c in json.loads(self.columns_json)]:raise ValueError('Original admitted path projection column required')
        return decode_exact_scalar(column['representation'],value).value
    def require_complete_bag(self,rows):
        name=json.loads(self.columns_json)[0]['outputName']
        if type(rows)is not list or any(type(r)is not dict or set(r)!={name}or type(r[name])is not str for r in rows):raise ValueError('Exact complete path root STRING rows required')
        if Counter(encoded(r)for r in rows)!=Counter(encoded(r)for r in json.loads(self.expected_rows_json)):raise ValueError('Independent complete original root path bag differs')
