// Derived from public UMF 0.5.8 producer-source APIs pinned at commit 855f6c.
import {readDocument} from './src/model/document.ts';
import {realizeSparkDelta4DDL} from './src/adapters/delta/ddl.ts';
if (!(globalThis as any).TextEncoder) {
  (globalThis as any).TextEncoder = class {
    encode(value: string) {
      const raw = unescape(encodeURIComponent(value));
      return Uint8Array.from(raw, (character: string) => character.charCodeAt(0));
    }
  };
}
(globalThis as any).__ashlarUmfSparkDelta4Sdk058 = {
  realize(sourceUtf8: string, target: {kind: 'delta-path'; path: string}) {
    return realizeSparkDelta4DDL(readDocument(sourceUtf8, 'json'), target);
  },
};
