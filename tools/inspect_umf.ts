/** Host tooling only: invoke the actual pinned UMF reader and retain input bytes. */
import {resolve, join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';

const [rootArg, revision, input] = process.argv.slice(2);
if (!rootArg || !revision || !input || !/^[0-9a-f]{40}$/.test(revision))
  throw Error('Usage: bun tools/inspect_umf.ts UMF_SOURCE EXACT_GIT_REVISION DOCUMENT.json');
const root = resolve(rootArg);
function git(...args: string[]) {
  const result = Bun.spawnSync(['git', '-C', root, ...args]);
  if (result.exitCode !== 0) throw Error('Unable to verify UMF source');
  return new TextDecoder().decode(result.stdout).trim();
}
if (git('rev-parse', 'HEAD') !== revision || git('status', '--porcelain'))
  throw Error('UMF source must be clean and match the pinned revision');
const bytes = new Uint8Array(await Bun.file(input).arrayBuffer());
const text = new TextDecoder('utf-8', {fatal: true}).decode(bytes);
const {readDocument} = await import(pathToFileURL(join(root, 'src/model/document.ts')).href);
const {validateDocument} = await import(pathToFileURL(join(root, 'src/validation/document.ts')).href);
const document = readDocument(text, 'json');
const validation = validateDocument(document);
if (!validation.valid) throw Error('UMF rejected schema structure');
if (git('rev-parse', 'HEAD') !== revision || git('status', '--porcelain'))
  throw Error('UMF source changed during inspection');
console.log(JSON.stringify({
  format: 'ashlar-schema-intake/0.1',
  sourceBase64: Buffer.from(bytes).toString('base64'),
  sourceSha256: createHash('sha256').update(bytes).digest('hex'),
  sourceBytes: bytes.length,
  umfCoreVersion: document.umf,
  documentId: document.id,
  validatorRevision: revision,
  validatedStructure: validation.valid,
  completeInterpretation: validation.complete,
  validation,
  qualification: 'UMF intake validation only; no target catalog acceptance or enforcement claim',
}, null, 2));
