import {singletonQuery,type Publication} from './native-read';
const source=await Bun.file(new URL('../out/native/ashlar_layout_v03_references_20261006_r74/summary.json',import.meta.url)).json();
const schema='client_dev.ashlar_layout_v03_20261006_r73';
const p:Publication={profile:'ashlar-delta/0.3',publicationId:'r74:publication1',tableVersions:source.versions};
const fixtures=[
 {kind:'node' as const,table:'object_current',sourceSystem:'pilot',typeId:'1',id:'1',expectedRows:1},
 {kind:'node' as const,table:'object_current',sourceSystem:'pilot',typeId:'2',id:'1',expectedRows:1},
 {kind:'edge' as const,table:'edge_current',sourceSystem:'pilot',typeId:'7',id:'1',expectedRows:1},
 {kind:'node' as const,table:'object_current',sourceSystem:"pilot'; SELECT 1 --",typeId:'1',id:'1',expectedRows:0},
];
await Bun.write(new URL('../out/native-read-queries.json',import.meta.url),JSON.stringify(fixtures.map(f=>({...f,...singletonQuery(p,schema+'.'+f.table,f.kind,f)})),null,2)+'\n');
