import {test,expect} from 'bun:test';
import {gunzipSync} from 'node:zlib';
import {augment,run} from '../tools/prepare_commerce_present_field';
const directory='docs/helix/04-build/evidence/';
test('authored presence control preserves all original source rows and reserved IDs',async()=>{
 const seed=await Bun.file(directory+'commerce-evolution-preparation-20261009/candidate.json').json();
 const proof=JSON.parse(gunzipSync(await Bun.file(directory+'commerce-public-revision-preservation-20261009/public-revisions.json.gz').bytes()).toString());
 const before=JSON.stringify([seed,proof]);const result=augment(seed,proof);
 expect(JSON.stringify([seed,proof])).toBe(before);
 expect(result.candidate.registry.entities.slice(0,seed.registry.entities.length)).toEqual(seed.registry.entities);
 expect(result.candidate.registry.properties).toEqual(seed.registry.properties);
 for(let i=0;i<5;i++)expect(result.candidate.revisions[i].source).toEqual(seed.revisions[i].source);
 const r4=result.candidate.revisions[3].graph;
 expect(r4.objects.slice(0,-1)).toEqual(seed.revisions[3].graph.objects);
 expect(r4.edges.slice(0,-1)).toEqual(seed.revisions[3].graph.edges);
 const original=r4.objects.find((r:any)=>r.key===result.authored.originalProductKey);
 expect(Object.hasOwn(original.values,'products.evolution_note')).toBe(false);
 expect(result.authored.product.values['products.evolution_note']).toBe('Explicit newly authored String');
 const present=result.inputs[3].records.find((r:any)=>r.instanceId===result.authored.product.key);
 expect(present.values.find((v:any)=>v.field.element==='products.evolution_note')).toEqual({field:{module:'domain',element:'products.evolution_note'},state:'present',value:{string:'Explicit newly authored String'}});
});
test('byte mismatch refuses before producer import or output',async()=>{
 const output='/private/tmp/ashlar-presence-refusal-'+crypto.randomUUID()+'.json';
 await expect(run('/nonexistent-producer',directory+'commerce-evolution-preparation-20261009/original-model.json',directory+'commerce-public-revision-preservation-20261009/public-revisions.json.gz',output)).rejects.toThrow('custody');
 expect(await Bun.file(output).exists()).toBe(false);
});
