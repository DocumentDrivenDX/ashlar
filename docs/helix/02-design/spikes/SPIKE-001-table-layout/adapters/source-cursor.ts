/** Exact tuple arithmetic for a declared source profile; not completeness proof. */
export type CursorRange = {profileId:string;xidMin:string;xidMax:string;seqMin:string;seqMax:string};
export type SafeXidWatermark = {feed:string;epoch:string;profileId:string;xid:string};
export type SourceCursor = {feed:string;epoch:string;profileId:string;xid:string;seq:string};
function integer(text:string):bigint {
  if(typeof text!=='string'||!/^(0|-?[1-9][0-9]*)$/.test(text))throw Error('Noncanonical exact integer text');
  return BigInt(text);
}
function ranges(p:CursorRange){
  if(!p||typeof p.profileId!=='string'||!p.profileId)throw Error('Missing source profile');
  const bounds={xidMin:integer(p.xidMin),xidMax:integer(p.xidMax),seqMin:integer(p.seqMin),seqMax:integer(p.seqMax)};
  if(bounds.xidMin<0n||bounds.xidMax<bounds.xidMin||bounds.seqMax<bounds.seqMin)throw Error('Invalid source range');
  return bounds;
}
export function validateCursor(c:SourceCursor,p:CursorRange):Readonly<SourceCursor>{
  const b=ranges(p);
  if(!c||typeof c.feed!=='string'||!c.feed||typeof c.epoch!=='string'||!c.epoch||c.profileId!==p.profileId)throw Error('Invalid cursor namespace/profile');
  const xid=integer(c.xid),seq=integer(c.seq);
  if(xid<b.xidMin||xid>b.xidMax||seq<b.seqMin||seq>b.seqMax)throw Error('Cursor outside declared range');
  return Object.freeze({feed:c.feed,epoch:c.epoch,profileId:c.profileId,xid:c.xid,seq:c.seq});
}
export function compareCursors(left:SourceCursor,right:SourceCursor,p:CursorRange):-1|0|1{
  const a=validateCursor(left,p),b=validateCursor(right,p);
  if(a.feed!==b.feed||a.epoch!==b.epoch)throw Error('Incomparable cursor namespaces');
  for(const key of ['xid','seq'] as const){const x=integer(a[key]),y=integer(b[key]);if(x<y)return -1;if(x>y)return 1;}
  return 0;
}
/** Strict xid watermark predicate only. Eligibility cannot advance a checkpoint
 * without independently verified transaction membership and source authority.
 * Watermark namespace equality is checked; its trusted origin remains external.
 */
export function belowSafeXid(c:SourceCursor,watermark:SafeXidWatermark,p:CursorRange):boolean{
  const a=validateCursor(c,p),b=ranges(p);
  const mark=validateCursor({...watermark,seq:p.seqMin},p);
  if(a.feed!==mark.feed||a.epoch!==mark.epoch)throw Error('Watermark namespace mismatch');
  const w=integer(mark.xid);
  if(w<b.xidMin||w>b.xidMax)throw Error('Watermark outside declared range');
  return integer(a.xid)<w;
}
