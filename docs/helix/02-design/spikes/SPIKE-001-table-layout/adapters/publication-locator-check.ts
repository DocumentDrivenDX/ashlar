import {publicationLocator} from './publication-locator';
import {singletonQuery,type Publication} from './native-read';
const node='client_dev.example.object_current',edge='client_dev.example.edge_current';
const input:Publication={profile:'ashlar-delta/0.3',publicationId:'p1',tableVersions:{[node]:7,[edge]:1}};
const pinned=publicationLocator(input,[node,edge]);
(input.tableVersions as Record<string,number>)[node]=9; // hostile caller mutation after read admission
if(pinned.tableVersions[node]!==7 || !Object.isFrozen(pinned) || !Object.isFrozen(pinned.tableVersions)) throw Error('Snapshot changed');
const q=singletonQuery(pinned,node,'node',{sourceSystem:'pilot',typeId:'1',id:'1'});
if(!q.sql.includes('VERSION AS OF 7'))throw Error('Lost admitted pin');
let refused=0;
function rejects(label:string,fn:()=>unknown){try{fn()}catch{refused++;return}throw Error('Accepted '+label)}
rejects('missing consumed edge',()=>publicationLocator({...input,tableVersions:{[node]:7}},[node,edge]));
rejects('unsafe unrelated version',()=>publicationLocator({...input,tableVersions:{[node]:7,[edge]:Number.MAX_SAFE_INTEGER+1}},[node]));
rejects('negative version',()=>publicationLocator({...input,tableVersions:{[node]:-1}},[node]));
rejects('string version',()=>publicationLocator({...input,tableVersions:{[node]:'7'} as unknown as Record<string,number>},[node]));
rejects('invalid unconsumed table',()=>publicationLocator({...input,tableVersions:{[node]:7,'bad;table':0}},[node]));
rejects('inherited version',()=>publicationLocator({...input,tableVersions:Object.create({[node]:7})},[node]));
rejects('null vector',()=>publicationLocator({...input,tableVersions:null as unknown as Record<string,number>},[node]));
rejects('array vector',()=>publicationLocator({...input,tableVersions:[7] as unknown as Record<string,number>},[node]));
rejects('empty plan',()=>publicationLocator(input,[]));
rejects('duplicate plan',()=>publicationLocator(input,[node,node]));
rejects('invalid plan',()=>publicationLocator(input,['bad']));
rejects('missing id',()=>publicationLocator({...input,publicationId:''},[node]));
rejects('unsupported profile',()=>publicationLocator({...input,profile:'future' as Publication['profile']},[node]));
console.log(JSON.stringify({state:'passed',refusals:refused,checks:['admitted locator immune to later input mutation','generated SQL retains admitted pin','all vector entries checked','required read-plan tables checked'],scope:'Local decoded-locator validation only; no descriptor authentication, duplicate JSON member decoding, projection semantics, native execution, policy or performance claim'},null,2));
