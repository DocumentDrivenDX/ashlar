import {test,expect} from 'bun:test';
import {gunzipSync} from 'node:zlib';
import {admitRetainedBytes} from '../tools/check_commerce_revision_preservation';
const root=new URL('../docs/helix/04-build/evidence/commerce-evolution-preparation-20261009/',import.meta.url);
const candidate=await Bun.file(new URL('candidate.json',root)).bytes(),datasets=new Uint8Array(gunzipSync(await Bun.file(new URL('public-datasets.json.gz',root)).bytes()));
test('exact retained inputs preserve all five source revisions and original complete/negative receipts',()=>{const admitted=admitRetainedBytes(candidate,datasets);expect(admitted.candidate.revisions.length).toBe(5);expect(admitted.datasets.outputs.flatMap((r:any)=>r.controls).length).toBe(15);});
test('changed lexical/model or retained receipt bytes refuse before public API invocation',()=>{for(const original of [candidate,datasets]){const changed=new Uint8Array(original);changed[20]^=1;expect(()=>admitRetainedBytes(original===candidate?changed:candidate,original===datasets?changed:datasets)).toThrow('byte custody');}});
test('oversized or missing inputs refuse before parsing and no source/model reinterpretation',()=>{expect(()=>admitRetainedBytes(new Uint8Array(4_000_001),datasets)).toThrow('byte custody');expect(()=>admitRetainedBytes(candidate,new Uint8Array())).toThrow('byte custody');});
