"""Private ten-table concurrent closing metadata; caller owns interval/fence/history.
No clients/writes on import. Expected heads must be independently qualified SQL
commit IDs; metadata is an observation, never a production writer fence.
"""
import copy,re
from publisher_custody_r462 import collect,CustodyMismatch
ROLES={'base-'+r for r in ['object_current','edge_current','source_record','property_journal','adjacency_forward','tombstone']}|{'input-'+r for r in ['source_record','property_journal','tombstone','current_replacement']}
def close(workers,expected,phase):
 if len(workers)!=4 or len({id(w) for w in workers})!=4 or len({str(w.out.resolve()) for w in workers})!=4:raise CustodyMismatch('Four distinct owned workers/outdirs required')
 if not re.fullmatch(r'[A-Za-z0-9_-]+',phase):raise CustodyMismatch('Invalid durable phase tag')
 if len(expected)!=10 or {x['key'] for x in expected}!=ROLES:raise CustodyMismatch('All ten unique base/input roles required')
 if any(not isinstance(x['head'].get('queryHistoryStatementId'),str) or not x['head']['queryHistoryStatementId'] for x in expected):raise CustodyMismatch('Qualified SQL commit IDs required')
 wanted=[{**copy.deepcopy(x),'key':phase+'-'+x['key']} for x in expected];known={x['key']:x for x in wanted};accepted=collect(workers,wanted)
 for x in accepted:
  if x['head']['queryHistoryStatementId']!=known[x['key']]['head']['queryHistoryStatementId']:raise CustodyMismatch('Commit ID changed despite matching version')
 return accepted
