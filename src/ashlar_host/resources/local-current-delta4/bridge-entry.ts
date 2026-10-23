// SPDX-License-Identifier: MIT OR Apache-2.0
// Narrow browser bridge built from the frozen 0.5.6 candidate source modules.
import {readDocument} from '../../../src/model/document.ts';
import {realizeSparkDelta4DDL} from '../../../src/adapters/delta/ddl.ts';
if(!(globalThis as any).TextEncoder){
  (globalThis as any).TextEncoder=class {encode(value:string){
    const raw=unescape(encodeURIComponent(value));
    return Uint8Array.from(raw,(character:string)=>character.charCodeAt(0));
  }};
}
(globalThis as any).__ashlarUmfSparkDelta4={realize:(sourceUtf8:string,target:{kind:'delta-path';path:string})=>
  realizeSparkDelta4DDL(readDocument(sourceUtf8,'json'),target)};
