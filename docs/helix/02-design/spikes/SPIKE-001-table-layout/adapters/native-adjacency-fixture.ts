import {adjacencyQuery,type Publication} from './native-read';
const source=await Bun.file(new URL('../out/native/ashlar_layout_v03_references_20261006_r74/summary.json',import.meta.url)).json();
const schema='client_dev.ashlar_layout_v03_20261006_r73';
const p:Publication={profile:'ashlar-delta/0.3',publicationId:'r74:publication1',tableVersions:source.versions};
const fixtures=[
 {direction:'out' as const,table:'adjacency_forward',sourceSystem:'pilot',typeId:'1',id:'1',expectedEdges:['1','2'],after:undefined},
 {direction:'out' as const,table:'adjacency_forward',sourceSystem:'pilot',typeId:'1',id:'1',expectedEdges:['3'],after:{relTypeId:'7',edgeId:'2'}},
 {direction:'in' as const,table:'adjacency_reverse',sourceSystem:'pilot',typeId:'2',id:'1',expectedEdges:['1','2'],after:undefined},
 {direction:'out' as const,table:'adjacency_forward',sourceSystem:'pilot',typeId:'1',id:'-1',expectedEdges:[],after:undefined},
];
await Bun.write(new URL('../out/native-adjacency-queries.json',import.meta.url),JSON.stringify(fixtures.map(f=>({...f,...adjacencyQuery(p,schema+'.'+f.table,f.direction,f,2,f.after)})),null,2)+'\n');
