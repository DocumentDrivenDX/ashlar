/** Full Ashlar typed-graph correspondence using public UMF SDK 0.5.8 APIs. */
import {createHash} from 'node:crypto';
import {lstat, readFile} from 'node:fs/promises';
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';

export const SDK058_PRODUCER_REVISION =
  'umf-sdk-0.5.8/071544fcc83c5abcf1e280890e1516613f679a58cf245b31a7bf16f0b6dd24fd';
const refusal = () => { throw new Error('Selected UMF SDK 0.5.8 typed graph admission refused'); };
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const keys = (value, wanted) => value !== null && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).sort().join(',') === [...wanted].sort().join(',');
const text = value => typeof value === 'string' && value.length > 0 && !value.includes('\0');
const validSha = value => typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);
const from64 = value => {
  if (typeof value !== 'string' || value.length > 1398104) refusal();
  const bytes = Buffer.from(value, 'base64');
  if (bytes.toString('base64') !== value || bytes.length > 1048576) refusal();
  return bytes;
};

/** Return the established typed-graph receipt shape after complete source checks. */
export function checkTypedGraph(modelBytes, requestBytes, sdk) {
  if (!(modelBytes instanceof Uint8Array) || modelBytes.length === 0 || modelBytes.length > 65536
      || !(requestBytes instanceof Uint8Array) || requestBytes.length === 0 || requestBytes.length > 1048576
      || !sdk || typeof sdk.readDocument !== 'function' || typeof sdk.readJsonValue !== 'function'
      || typeof sdk.validateCoreRecordValues !== 'function'
      || typeof sdk.validateCoreDatasetValuesCompact !== 'function'
      || typeof sdk.verifyCoreDatasetValuesCompact !== 'function') refusal();

  const decoder = new TextDecoder('utf-8', {fatal: true, ignoreBOM: true});
  const model = sdk.readDocument(decoder.decode(modelBytes), 'json');
  if (model.umf !== '0.10.0') refusal();
  const input = sdk.readJsonValue(decoder.decode(requestBytes), 'json');
  const requestKeys = ['profile', 'configurationSha256', 'sourceSha256', 'bindingSha256', 'modelSha256',
    'documentRevision', 'sourceSystem', 'feed', 'epoch', 'groups'];
  if (!keys(input, requestKeys) || input.profile !== 'ashlar-typed-graph-check-request/0.1'
      || !['configurationSha256', 'sourceSha256', 'bindingSha256', 'modelSha256'].every(k => validSha(input[k]))
      || input.modelSha256 !== sha(modelBytes)
      || !['documentRevision', 'sourceSystem', 'feed', 'epoch'].every(k => text(input[k]))
      || !Array.isArray(input.groups) || input.groups.length < 1 || input.groups.length > 4) refusal();

  const sourceDigest = createHash('sha256');
  let offset = 0, eventCount = 0, objectCount = 0;
  const readLine = raw => {
    if (raw.length === 0 || raw[raw.length - 1] !== 10 || raw.subarray(0, -1).includes(10)) refusal();
    return sdk.readJsonValue(decoder.decode(raw), 'json');
  };
  const groups = [];
  for (const group of input.groups) {
    if (!keys(group, ['batch', 'events', 'records', 'dataset'])
        || !Array.isArray(group.events) || !Array.isArray(group.records)) refusal();
    const batch = group.batch;
    const batchKeys = ['feed', 'epoch', 'batchId', 'recordCount', 'recordsSha256', 'cursorBefore', 'cursorAfter', 'beginBase64', 'commitBase64'];
    if (!keys(batch, batchKeys) || batch.feed !== input.feed || batch.epoch !== input.epoch || !text(batch.batchId)
        || !Number.isSafeInteger(batch.recordCount) || batch.recordCount !== group.events.length
        || !validSha(batch.recordsSha256) || batch.cursorBefore !== String(offset)) refusal();
    const begin = from64(batch.beginBase64), commit = from64(batch.commitBase64);
    const start = readLine(begin), end = readLine(commit);
    if (!keys(start, ['kind', 'batch_id']) || start.kind !== 'begin' || start.batch_id !== batch.batchId
        || !keys(end, ['kind', 'batch_id', 'record_count', 'records_sha256']) || end.kind !== 'commit'
        || end.batch_id !== batch.batchId || end.record_count !== batch.recordCount
        || end.records_sha256 !== batch.recordsSha256) refusal();
    sourceDigest.update(begin); offset += begin.length;
    const recordDigest = createHash('sha256'), objects = [], identities = new Set(), deliveries = new Set();
    for (const event of group.events) {
      if (++eventCount > 1000 || !keys(event, ['deliveryId', 'recordSha256', 'rawBase64'])
          || !text(event.deliveryId) || !validSha(event.recordSha256) || deliveries.has(event.deliveryId)) refusal();
      deliveries.add(event.deliveryId);
      const raw = from64(event.rawBase64), value = readLine(raw), length = Buffer.alloc(8);
      length.writeBigUInt64BE(BigInt(raw.length)); recordDigest.update(length); recordDigest.update(raw);
      sourceDigest.update(raw); offset += raw.length;
      if (sha(raw) !== event.recordSha256 || value.kind !== 'event' || value.delivery_id !== event.deliveryId
          || value.source_profile !== 'ashlar-whole-entity/0.1' || value.source_system !== input.sourceSystem
          || value.schema_revision !== input.documentRevision || !['object', 'edge'].includes(value.entity_kind)
          || !['create', 'replace', 'delete'].includes(value.operation)
          || !['type_id', 'id', 'entity_version'].every(k => typeof value[k] === 'string' && /^-?(0|[1-9][0-9]*)$/.test(value[k]))
          || typeof value.props_json !== 'string' || typeof value.retained_json !== 'string') refusal();
      const eventKeys = ['kind', 'delivery_id', 'source_profile', 'source_system', 'entity_kind', 'type_id', 'id',
        'entity_version', 'schema_revision', 'operation', 'props_json', 'retained_json'];
      if (value.entity_kind === 'edge') eventKeys.push('endpoints');
      if (!keys(value, eventKeys)) refusal();
      const identity = JSON.stringify([value.entity_kind, value.type_id, value.id]);
      if (identities.has(identity)) refusal();
      identities.add(identity);
      if (value.entity_kind === 'edge') {
        if (!keys(sdk.readJsonValue(value.props_json, 'json'), []) || !Array.isArray(value.endpoints)
            || value.endpoints.length !== 2 || value.endpoints.some(endpoint => !keys(endpoint, ['type_id', 'id'])
              || !['type_id', 'id'].every(k => text(endpoint[k])))) refusal();
      } else objects.push({event, value});
    }
    if (recordDigest.digest('hex') !== batch.recordsSha256 || objects.length !== group.records.length) refusal();
    sourceDigest.update(commit); offset += commit.length;
    if (offset > 1048576 || batch.cursorAfter !== String(offset)) refusal();

    const records = group.records.map((item, index) => {
      const {event, value} = objects[index];
      if (++objectCount > 1000 || !keys(item, ['deliveryId', 'recordSha256', 'typeId', 'propsSha256', 'identity', 'values'])
          || item.deliveryId !== event.deliveryId || item.recordSha256 !== event.recordSha256
          || !Number.isSafeInteger(item.typeId) || item.typeId < 1 || item.typeId >= 2147483648
          || String(item.typeId) !== value.type_id
          || item.propsSha256 !== sha(new TextEncoder().encode(value.props_json))) refusal();
      const result = sdk.validateCoreRecordValues(model, item.identity, item.values);
      const owner = model.modules.find(m => m.id === item.identity.module)?.elements.find(e => e.id === item.identity.element);
      const warnings = [];
      if (owner?.keys?.length) warnings.push({code: 'RECORD_KEY_CONTEXT_REQUIRED', path: '/identity',
        message: 'Dataset key equality/uniqueness needs a separately qualified check', severity: 'warning'});
      if (model.modules.some(m => Array.isArray(m.relationships) && m.relationships.length)) warnings.push({
        code: 'RECORD_RELATIONSHIP_CONTEXT_REQUIRED', path: '/identity',
        message: 'Relationship/endpoint constraints need a separately qualified dataset check', severity: 'warning'});
      if (result.validation.valid !== true || result.validation.complete !== (warnings.length === 0)
          || result.validation.diagnostics.length !== warnings.length
          || result.validation.diagnostics.some((diagnostic, i) => !keys(diagnostic, ['code', 'path', 'message', 'severity'])
            || Object.entries(warnings[i]).some(([key, value]) => diagnostic[key] !== value))
          || !result.fields.every(field => field.validation.valid === true && field.validation.complete === true
            && field.validation.diagnostics.length === 0)) refusal();
      return {request: item, result};
    });
    const dataset = sdk.validateCoreDatasetValuesCompact(model, group.dataset);
    sdk.verifyCoreDatasetValuesCompact(dataset, model, group.dataset);
    if (dataset.datasetValidation.valid !== true || dataset.datasetValidation.complete !== true
        || dataset.datasetValidation.diagnostics.length !== 0 || dataset.residuals.length !== 0
        || dataset.obligations.some(obligation => obligation.state !== 'satisfied')) refusal();
    groups.push({records, dataset});
  }
  if (objectCount === 0 || sourceDigest.digest('hex') !== input.sourceSha256) refusal();
  return {
    format: 'ashlar-typed-graph-check/0.1', producerRevision: SDK058_PRODUCER_REVISION,
    sourceBase64: Buffer.from(modelBytes).toString('base64'), sourceSha256: sha(modelBytes),
    requestBase64: Buffer.from(requestBytes).toString('base64'), requestSha256: sha(requestBytes), groups,
    qualification: 'Public UMF SDK 0.5.8 Core record and complete supplied-dataset-only compact checks after Ashlar source/event correspondence. Explicit development IDs; no source, catalog, native publication, query or ACK authority.',
  };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const [modelPath, requestPath, sdkEntry] = process.argv.slice(2);
  if (!modelPath || !requestPath || !sdkEntry) refusal();
  const [modelStat, requestStat] = await Promise.all([lstat(modelPath), lstat(requestPath)]);
  if (!modelStat.isFile() || modelStat.isSymbolicLink() || modelStat.size === 0 || modelStat.size > 65536
      || !requestStat.isFile() || requestStat.isSymbolicLink() || requestStat.size === 0 || requestStat.size > 1048576) refusal();
  const [model, request] = await Promise.all([readFile(modelPath), readFile(requestPath)]);
  if (model.length !== modelStat.size || request.length !== requestStat.size) refusal();
  const sdk = await import(pathToFileURL(sdkEntry).href);
  process.stdout.write(JSON.stringify(checkTypedGraph(model, request, sdk)));
}
