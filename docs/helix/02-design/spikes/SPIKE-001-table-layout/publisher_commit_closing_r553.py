"""Bind concurrent closing checks to completed private publisher commits.
Read-only metadata observations do not fence writers. Caller retains closed
commit-interval checks before/after descriptor publication and final telemetry.
"""
import copy
from publisher_closing_r548 import close,ROLES
from publisher_custody_r462 import CustodyMismatch
MUTABLE={'edge_current','source_record','property_journal','adjacency_forward','tombstone'}
def expected_after(initial,base,tables,events):
 if len(initial)!=10 or {x['key'] for x in initial}!=ROLES:raise CustodyMismatch('Complete initial custody required')
 required=MUTABLE|{'object_current'}
 if set(base)!=required or set(tables)!=required:raise CustodyMismatch('Complete base/current vectors required')
 if len(events)!=5 or {x['role'] for x in events}!=MUTABLE:raise CustodyMismatch('Exactly five unique commit events required')
 known={x['key']:x for x in initial};bound=copy.deepcopy(initial)
 for role in required:
  old=base[role];new=tables[role];e=known['base-'+role]
  if old['table']!=new['table'] or old['table']!=e['table'] or old['id']!=new['id'] or old['id']!=e['detail']['id']:raise CustodyMismatch('Table identity changed')
  if role=='object_current':
   if new!=old:raise CustodyMismatch('Selected node pin changed')
   continue
  if int(e['head']['version'])!=old['version']:raise CustodyMismatch('Initial mutable head mismatches base')
  event=next(x for x in events if x['role']==role);h=event['history'];sid=event['statement_id']
  if not isinstance(sid,str) or not sid or h.get('queryHistoryStatementId')!=sid:raise CustodyMismatch('Unbound commit statement')
  if new['version']!=old['version']+1 or int(h['version'])!=new['version']:raise CustodyMismatch('Unexpected commit interval')
  next(x for x in bound if x['key']=='base-'+role)['head']=copy.deepcopy(h)
 return bound

def close_after(workers,initial,base,tables,events,phase):
 return close(workers,expected_after(initial,base,tables,events),phase)
