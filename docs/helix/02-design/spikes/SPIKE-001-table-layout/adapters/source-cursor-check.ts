import {validateCursor,compareCursors,belowSafeXid,type SourceCursor} from './source-cursor';
const profile={profileId:'synthetic-u64-i64/1',xidMin:'0',xidMax:'18446744073709551615',seqMin:'-9223372036854775808',seqMax:'9223372036854775807'};
const watermark=(xid:string)=>({feed:'fixture',epoch:'e1',profileId:profile.profileId,xid});
const cursor=(xid:string,seq='1'):SourceCursor=>({feed:'fixture',epoch:'e1',profileId:profile.profileId,xid,seq});
const cases:[SourceCursor,SourceCursor,number][]=[
 [cursor('9'),cursor('10'),-1],
 [cursor('9007199254740992'),cursor('9007199254740993'),-1],
 [cursor('9223372036854775808'),cursor('18446744073709551615'),-1],
 [cursor('10','-1'),cursor('10','0'),-1],
 [cursor('10','9223372036854775807'),cursor('10','1'),1],
 [cursor('10','2'),cursor('10','2'),0]];
for(const[a,b,expected]of cases){if(compareCursors(a,b,profile)!==expected)throw Error('Wrong exact tuple order');}
if(belowSafeXid(cursor('10'),watermark('10'),profile)||!belowSafeXid(cursor('9'),watermark('10'),profile))throw Error('Unsafe watermark equality');
const original=cursor('9007199254740993');const copy=validateCursor(original,profile);original.xid='1';if(copy.xid!=='9007199254740993'||!Object.isFrozen(copy))throw Error('Cursor changed');
let refused=0;function reject(fn:()=>unknown){try{fn()}catch{refused++;return}throw Error('Accepted invalid cursor')}
for(const bad of ['01','-0','1e3','1.0','-1','18446744073709551616'])reject(()=>validateCursor(cursor(bad),profile));
reject(()=>validateCursor(cursor(9007199254740993 as unknown as string),profile));
reject(()=>validateCursor(cursor('1','9223372036854775808'),profile));
reject(()=>validateCursor(cursor('1','-9223372036854775809'),profile));
reject(()=>compareCursors(cursor('1'),{...cursor('2'),epoch:'e2'},profile));
reject(()=>compareCursors(cursor('1'),{...cursor('2'),feed:'other'},profile));
reject(()=>validateCursor({...cursor('1'),profileId:'unknown'},profile));
reject(()=>belowSafeXid(cursor('1'),watermark('18446744073709551616'),profile));
reject(()=>validateCursor(cursor('1'),{...profile,xidMin:'10',xidMax:'9'}));
reject(()=>belowSafeXid(cursor('1'),{...watermark('2'),feed:'other'},profile));
reject(()=>belowSafeXid(cursor('1'),{...watermark('2'),epoch:'e2'},profile));
console.log(JSON.stringify({state:'passed',ordered_pairs:cases.length,refusals:refused,scope:'Synthetic declared range exact tuple arithmetic, frozen copy and strict xid eligibility only. No real Truss range qualification, transaction membership, trusted watermark, seed/coverage boundary, acknowledgement or source progress claim.'},null,2));
