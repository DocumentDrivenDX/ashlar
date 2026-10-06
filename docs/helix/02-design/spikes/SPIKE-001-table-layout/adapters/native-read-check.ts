/** Adversarial boundary checks for the portable query builder. */
import {singletonQuery, type Publication} from './native-read';
const table='client_dev.example.object_current';
const p:Publication={profile:'ashlar-delta/0.3',publicationId:'fixture',tableVersions:{[table]:7}};
const id={sourceSystem:"pilot'; SELECT 1 --",typeId:'1',id:'9223372036854775807'};
const q=singletonQuery(p,table,'node',id);
if(q.sql.includes(id.sourceSystem) || q.sql.includes(id.id)) throw new Error('Interpolated identity');
if(q.parameters.id!==id.id || !q.sql.includes('VERSION AS OF 7') || !q.sql.includes('lookup_hash') || !q.sql.includes('AND id =')) throw new Error('Lost pin/identity predicate');
function refuses(label:string,f:()=>unknown){try{f()}catch{return}throw new Error('Accepted '+label)}
for(const value of ['9223372036854775808','-9223372036854775809','01','-0','1e3','1.0']) refuses(value,()=>singletonQuery(p,table,'node',{...id,id:value}));
refuses('absent table',()=>singletonQuery({...p,tableVersions:{}},table,'node',id));
refuses('unsafe version',()=>singletonQuery({...p,tableVersions:{[table]:9007199254740992}},table,'node',id));
refuses('table injection',()=>singletonQuery(p,'client_dev.example.object_current;DROP','node',id));
refuses('kind mismatch',()=>singletonQuery(p,table,'edge',id));
refuses('non-ASCII source',()=>singletonQuery(p,table,'node',{...id,sourceSystem:'π'}));
console.log(JSON.stringify({state:'passed',scope:'Portable builder boundary checks only; no backend authorization or latency claim'}));
const {adjacencyQuery}=await import('./native-read');
const forward='client_dev.example.adjacency_forward';
const ap:Publication={...p,tableVersions:{[forward]:1}};
const aq=adjacencyQuery(ap,forward,'out',id,2,{relTypeId:'7',edgeId:'9223372036854775807'});
if(!aq.sql.includes('ORDER BY rel_type_id, edge_id LIMIT 2') || aq.parameters.afterEdge!=='9223372036854775807') throw new Error('Unstable pagination');
refuses('unbounded page',()=>adjacencyQuery(ap,forward,'out',id,1001));
refuses('negative page',()=>adjacencyQuery(ap,forward,'out',id,-1));
refuses('reverse/table mismatch',()=>adjacencyQuery(ap,forward,'in',id,2));
refuses('overflow cursor',()=>adjacencyQuery(ap,forward,'out',id,2,{relTypeId:'7',edgeId:'9223372036854775808'}));
console.log('Adjacency pagination boundaries passed');

refuses('JavaScript numeric identity',()=>singletonQuery(p,table,'node',{...id,id:9007199254740993 as unknown as string}));
refuses('numeric source',()=>singletonQuery(p,table,'node',{...id,sourceSystem:1 as unknown as string}));
