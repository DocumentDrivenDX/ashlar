/** Finite original graph carrier transposition into the public UMF dataset API.
 * No storage binding, source authorization or native/query admission is implied.
 */
import {resolve, join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
import {writeFile} from 'node:fs/promises';

export const PRODUCER = 'e44cd15f336dfb33db35acf20eee13dd120a1a28';
const PACKS = '1f7b5f5d2a355c4b476e3a96b289b9048f03f567';
const INVENTORY = '795261c25448998fdd1bf80fe42cd7df25dff72f7aca0e4149501ffc82fbe8a2';
const LIMIT = 4_000_000;
const hash = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex');
export class CarrierRefusal extends Error {}
export async function writeFreshReceipt(output: string, bytes: string) {
  await writeFile(output, bytes, {flag: 'wx'});
}

/** Select carriers, never validate or normalize their semantic values. */
export function datasetInput(source: any, graph: any, scopeId: string) {
  const refuse = (message: string): never => { throw new CarrierRefusal(message); };
  if (source.umf !== '0.8.0' || graph.format !== 'umf.domain-graph' || graph.version !== '1.0.0')
    refuse('Declared original model/graph profile is outside this transposition');
  if (!Array.isArray(graph.objects) || !Array.isArray(graph.edges) || graph.objects.length > 1000 || graph.edges.length > 2000)
    refuse('Finite graph carrier bounds exceeded');
  const elements = new Map<string, any>();
  for (const module of source.modules) for (const element of module.elements)
    elements.set(JSON.stringify([module.id, element.id]), element);
  const element = (ref: any) => elements.get(JSON.stringify([ref.module, ref.element]));
  const objects = new Map<string, any>();
  for (const object of graph.objects) {
    if (objects.has(object.key)) refuse('Duplicate original object identity');
    objects.set(object.key, object);
  }
  const literal = (ref: any, object: any): any => {
    if (!Object.hasOwn(object.values, ref.element)) refuse('Missing relationship key carrier');
    const token = object.values[ref.element];
    if (token === null) return null;
    if (typeof token !== 'string') refuse('Expected exact lexical text or explicit null');
    const field = element(ref);
    if (!field) refuse('Field carrier declaration unavailable');
    switch (field.scalarType) {
      case 'string': return {string: token};
      case 'integer': return {integerToken: token};
      case 'decimal': return {decimalToken: token};
      default: return refuse('Unsupported lexical carrier family: ' + String(field.scalarType));
    }
  };
  return {
    scope: {id: scopeId, closure: 'supplied-dataset-only'},
    context: {fixtureFormat: graph.format, fixtureVersion: graph.version, qualification: graph.qualification},
    records: graph.objects.map((object: any) => {
      if (object.type.document !== source.id) refuse('Original object document differs');
      const identity = {module: object.type.module, element: object.type.element};
      const record = element(identity);
      if (!record || !Array.isArray(record.members)) refuse('Record carrier declaration unavailable');
      const names = new Set(record.members.map((ref: any) => ref.element));
      if (names.size !== record.members.length) refuse('Unqualified graph property cannot distinguish repeated Field names');
      if (Object.keys(object.values).some(name => !names.has(name))) refuse('Unmapped original property remains');
      return {instanceId: object.key, identity, values: record.members.map((ref: any) =>
        Object.hasOwn(object.values, ref.element)
          ? {field: ref, state: 'present', value: literal(ref, object)}
          : {field: ref, state: 'absent'})};
    }),
    relationships: graph.edges.map((edge: any) => {
      if (edge.relationship.document !== source.id) refuse('Original relationship document differs');
      const module = source.modules.find((m: any) => m.id === edge.relationship.module);
      const relationship = module?.relationships?.find((r: any) => r.id === edge.relationship.id);
      const target = objects.get(edge.target);
      if (!relationship || !target) refuse('Original relationship or endpoint unavailable');
      const targets = relationship.target.filter((t: any) => t.module === target.type.module && t.element === target.type.element);
      if (targets.length !== 1) refuse('Unique declared target key selection unavailable');
      const record = element(target.type);
      const key = record?.keys?.find((k: any) => k.id === targets[0].key);
      if (!key) refuse('Declared target key unavailable');
      return {instanceId: edge.key, identity: {module: module.id, id: relationship.id},
        sourceInstanceId: edge.source, target: {identity: {module: target.type.module, element: target.type.element, key: key.id},
          values: key.fields.map((ref: any) => literal(ref, target))}};
    }),
  };
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  if (args.length !== 3) throw Error('Usage: bun tools/check_domain_pack_datasets.ts CLEAN_UMF_PRODUCER PINNED_PACK_REPO FRESH_OUTPUT');
  const [producer, packs, output] = args.map(p => resolve(p));
  const git = (repo: string, args: string[]) => {
    const result = Bun.spawnSync(['git', '-C', repo, ...args]);
    if (result.exitCode) throw Error('Pinned Git source unavailable');
    return result.stdout;
  };
  const clean = () => {
    if (new TextDecoder().decode(git(producer, ['rev-parse', 'HEAD'])).trim() !== PRODUCER || git(producer, ['status', '--porcelain']).length)
      throw Error('Exact clean public producer required');
  };
  clean();
  if (await Bun.file(output).exists()) throw Error('Fresh output required');
  const inventoryBytes = await Bun.file(resolve(import.meta.dir, '../examples/domain-packs/inventory.json')).bytes();
  if (hash(inventoryBytes) !== INVENTORY) throw Error('Exact original inventory required');
  const load = (path: string) => import(pathToFileURL(join(producer, path)).href);
  const [{readDocument}, {readJsonValue}, {validateCoreDatasetValuesCompact, verifyCoreDatasetValuesCompact}, {UmfError}] = await Promise.all([
    load('src/model/document.ts'), load('src/model/serialization.ts'), load('src/model/dataset-values-compact.ts'), load('src/model/types.ts')]);
  const inventory = readJsonValue(new TextDecoder().decode(inventoryBytes), 'json');
  const results: any[] = [];
  for (const pack of inventory.packs) {
    if (!pack.ontology || !pack.graph) {
      results.push({pack: pack.id, status: 'unavailable', reason: 'No original finite ontology/graph pair', datasetAdmission: false});
      continue;
    }
    const retain = (path: string) => {
      if (!/^[A-Za-z0-9_./-]+$/.test(path) || path.startsWith('/') || path.split('/').some(p => p === '.' || p === '..')) throw Error('Normalized original path required');
      const raw = git(packs, ['show', PACKS + ':spec/domain-packs/' + pack.directory + '/' + path]);
      const entry = pack.files.find((f: any) => f.path === path);
      if (raw.length > LIMIT || !entry || entry.sha256 !== hash(raw) || entry.bytes !== raw.length) throw Error('Bounded original inventory custody differs');
      return {path, sha256: hash(raw), bytes: raw.length, base64: Buffer.from(raw).toString('base64')};
    };
    const model = retain(pack.ontology.path), graphSource = retain(pack.graph.path);
    const original = {pack: pack.id, model, graph: graphSource};
    const graph = readJsonValue(new TextDecoder('utf-8', {fatal: true}).decode(Buffer.from(graphSource.base64, 'base64')), 'json');
    if (graph.pack.sha256 !== pack.pack_sha256) {
      results.push({...original, status: 'quarantined', reason: 'Original graph names a different manifest digest', datasetAdmission: false});
      continue;
    }
    try {
      const source = readDocument(new TextDecoder('utf-8', {fatal: true}).decode(Buffer.from(model.base64, 'base64')), 'json');
      const input = datasetInput(source, graph, 'ashlar-original-' + pack.id + '-classification');
      const receipt = validateCoreDatasetValuesCompact(source, input);
      verifyCoreDatasetValuesCompact(receipt, source, input);
      const complete = receipt.datasetValidation.valid === true && receipt.datasetValidation.complete === true;
      const controls: any[] = [];
      if (complete) {
        if (receipt.records.length !== graph.objects.length || receipt.relationships.length !== graph.edges.length) throw Error('Complete original occurrence inventory differs');
        for (const edge of graph.edges) {
          const actual = receipt.relationships.find((r: any) => r.instanceId === edge.key);
          if (!actual || actual.sourceInstanceId !== edge.source || actual.targetInstanceId !== edge.target) throw Error('Original endpoint identity differs');
        }
        for (const name of ['duplicate-key', 'unresolved-target']) {
          const changed = structuredClone(input);
          if (name === 'duplicate-key') {
            if (!changed.records.length) throw Error('Duplicate-key control requires a record');
            const record = structuredClone(changed.records[0]);
            record.instanceId = 'ashlar-separately-authored-duplicate-key-control';
            if (changed.records.some(r => r.instanceId === record.instanceId)) throw Error('Control identity collides');
            changed.records.push(record);
          } else {
            if (!changed.relationships.length) throw Error('Endpoint control requires an occurrence');
            changed.relationships[0].target.values[0] = {string: 'ashlar-separately-authored-missing-target-control'};
          }
          const result = validateCoreDatasetValuesCompact(source, changed);
          verifyCoreDatasetValuesCompact(result, source, changed);
          if (result.datasetValidation.valid !== false) throw Error('Adversarial finite dataset unexpectedly admitted: ' + name);
          controls.push({name, input: changed, receipt: result});
        }
      }
      results.push({...original, status: complete ? 'admitted-finite-dataset' : 'public-incomplete-or-invalid', datasetAdmission: complete, input, receipt, controls});
    } catch (error: any) {
      if (!(error instanceof CarrierRefusal) && !(error instanceof UmfError)) throw error;
      results.push({...original, status: error instanceof CarrierRefusal ? 'carrier-refused' : 'public-refused', datasetAdmission: false, diagnostic: {code: error.code, path: error.path, message: error.message}});
    }
  }
  clean();
  if (await Bun.file(output).exists()) throw Error('Fresh output required');
  await writeFreshReceipt(output, JSON.stringify({profile: 'ashlar-original-pack-dataset-classification/0.1', producerRevision: PRODUCER,
    packRevision: PACKS, inventorySha256: INVENTORY, results,
    qualification: 'Public compact supplied-dataset ABI: fragments retain sourceRef to the full original source, not standalone documents. Finite supplied original dataset admission only. No source authority, canonical binding, storage publication, Weft, native engine or external dataset support.'}) + '\n');
  console.log(JSON.stringify({packs: results.length, statuses: results.reduce((counts, r) => ({...counts, [r.status]: (counts[r.status] ?? 0) + 1}), {} as Record<string, number>)}));
}
