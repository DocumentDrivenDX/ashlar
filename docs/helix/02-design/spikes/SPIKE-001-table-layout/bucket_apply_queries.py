"""Owned bucket-aware property apply; canonical identity remains the full tuple."""
from property_apply_queries import COLS,PropertyApply
BUCKET_SQL="cast(pmod(cast(conv(substr(lookup_hash,1,15),16,10) AS BIGINT),64) AS INT)"
def bucket_apply(q):
 if type(q) is not PropertyApply or q.eligibility_placement!='on':raise ValueError('Owned update-only eligible profile required')
 sql=q.apply()
 sql=sql.replace("SELECT "+','.join(COLS)+" FROM", "SELECT "+','.join(COLS)+','+BUCKET_SQL+' lookup_bucket FROM',1)
 sql=sql.replace('ON t.lookup_hash=s.lookup_hash','ON t.lookup_bucket=s.lookup_bucket AND t.lookup_hash=s.lookup_hash',1)
 return sql.replace('UPDATE SET *','UPDATE SET '+','.join('t.'+col+'=s.'+col for col in COLS))
