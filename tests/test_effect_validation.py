import unittest
from ashlar.native import SQLResult
from ashlar.effect_validation import validate_effect_snapshot,EffectValidationError
COLS=(('id','BIGINT'),('retained','STRING'),('clock','TIMESTAMP'))
ROW={'id':'9223372036854775807','retained':'{"x":18446744073709551615}','clock':'1791450000000000'}
class Executor:
    def __init__(self,rows=None):self.rows=[ROW] if rows is None else rows;self.calls=[]
    def query(self,sql,params):
        self.calls.append(sql)
        if sql.startswith('DESCRIBE'):return SQLResult([{'id':'uuid'}])
        if sql.endswith('LIMIT 0'):return SQLResult([],COLS)
        return SQLResult(self.rows)
class EffectValidationTests(unittest.TestCase):
    def test_full_exact_inventory_and_version(self):
        ex=Executor();r=validate_effect_snapshot(ex,'c.s.t','uuid',7,COLS,[ROW])
        self.assertEqual(r['rows'],1);self.assertEqual(len(ex.calls),4)
        self.assertIn('VERSION AS OF 7',ex.calls[2])
    def test_duplicate_extra_missing_and_changed_text_refuse(self):
        for rows in [[],[ROW,ROW],[dict(ROW,retained='{"x": 18446744073709551615}')],[dict(ROW,clock=None)]]:
            with self.assertRaises(EffectValidationError):validate_effect_snapshot(Executor(rows),'c.s.t','uuid',7,COLS,[ROW])
    def test_bad_profile_and_identity_refuse(self):
        for uuid,version,cols in [('wrong',7,COLS),('uuid',True,COLS),('uuid',7,(('unsafe`','STRING'),))]:
            with self.assertRaises(EffectValidationError):validate_effect_snapshot(Executor(),'c.s.t',uuid,version,cols,[ROW])
if __name__=='__main__':unittest.main()
