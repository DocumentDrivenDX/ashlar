#!/usr/bin/env node
import {createHash} from 'node:crypto';
import {readFile, writeFile} from 'node:fs/promises';
import {isDeepStrictEqual} from 'node:util';
import {runInNewContext} from 'node:vm';
import {pathToFileURL} from 'node:url';
import {dirname, resolve} from 'node:path';

const [typedRootArg, carrierRootArg, requestArg, outputArg, sdkEntryArg, oldBridgeArg,
  legacyReceiptArg, sdkArchiveArg, producerClosureArg, producerReceiptArg] = process.argv.slice(2);
if ([typedRootArg, carrierRootArg, requestArg, outputArg, sdkEntryArg, oldBridgeArg,
  legacyReceiptArg, sdkArchiveArg, producerClosureArg, producerReceiptArg].some(x => !x)) {
  throw new Error('usage: qualify_release.mjs typed-input-dir carrier-input-dir request.json output-dir sdk-entry old-bridge-iife legacy-receipt sdk-archive producer-closure producer-receipt');
}
const typedRoot = resolve(typedRootArg), carrierRoot = resolve(carrierRootArg);
const requestPath = resolve(requestArg), outputRoot = resolve(outputArg);
const sdkEntry = resolve(sdkEntryArg), oldBridgePath = resolve(oldBridgeArg);
const legacyReceiptPath = resolve(legacyReceiptArg), sdkArchivePath = resolve(sdkArchiveArg);
const producerClosurePath = resolve(producerClosureArg), producerReceiptPath = resolve(producerReceiptArg);
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const plain = value => JSON.parse(JSON.stringify(value));
const read = path => readFile(path);
const readJson = async path => JSON.parse((await read(path)).toString('utf8'));
const bundleBytes = await read(oldBridgePath);
const legacyBridgeSha256 = hash(bundleBytes);
const closureBytes = await read(producerClosurePath), closure = JSON.parse(closureBytes.toString('utf8'));
const producerReceiptBytes = await read(producerReceiptPath);
const archiveBytes = await read(sdkArchivePath), sdkEntryBytes = await read(sdkEntry);
const packageJsonPath = resolve(dirname(sdkEntry), '..', 'package.json');
const sdkPackageBytes = await read(packageJsonPath), sdkPackage = JSON.parse(sdkPackageBytes.toString('utf8'));
const expected = {
  producerSourceCommit: '855f6c48649496fc60b3303798790d0ddb55d38f',
  producerSourceTree: '53f2bfffbc0c5780e79d76d8fd3d202239d088b2',
  archiveSha256: '2aaf24741aee1c135fade5b7631db90d710e6211977fb9ca7fcc080ea7fdf58d',
  closureSha256: 'e90c44cb79cc5ce39921274cc155c8634232635fc9ba02565d43846f6ee1152f',
  producerReceiptSha256: '1ddd2b37623ec1daaffbb592200adafc9f8bb27522c65f1b5584f0a9aec945c4',
  installedSdkTreeSha256: '7227738ae23966239b67d3e9df72f848c101761719ef4b39477fe86de6ef519c',
  entrySha256: '071544fcc83c5abcf1e280890e1516613f679a58cf245b31a7bf16f0b6dd24fd',
  packageVersion: '0.5.8',
  oldBridgeSha256: '34deb5c79b5dcb1ace8093e66a4b1213a98cbcc1fb50bfd24fdc4a21721c76b5',
};
const preflight = {
  closureStatus: closure.status === 'passed',
  sourceCommit: closure.sourceCommit === expected.producerSourceCommit,
  sourceTree: closure.sourceTree === expected.producerSourceTree,
  archiveHash: hash(archiveBytes) === expected.archiveSha256 && closure.selectedSoftware?.sdkArchive?.sha256 === expected.archiveSha256,
  closureHash: hash(closureBytes) === expected.closureSha256,
  producerReceiptHash: hash(producerReceiptBytes) === expected.producerReceiptSha256,
  installedTree: closure.selectedSoftware?.installedSdkPackage?.treeSha256 === expected.installedSdkTreeSha256,
  installedEntryHash: hash(sdkEntryBytes) === expected.entrySha256,
  installedPackageVersion: sdkPackage.version === expected.packageVersion,
  oldBridgeHash: legacyBridgeSha256 === expected.oldBridgeSha256,
};
if (Object.values(preflight).some(ok => !ok)) {
  throw new Error(`selected UMF SDK/bridge preflight refused: ${JSON.stringify(preflight)}`);
}
const sdk = await import(pathToFileURL(sdkEntry).href);
const legacyContext = {};
runInNewContext(bundleBytes.toString('utf8'), legacyContext, {timeout: 5000});
const legacy = legacyContext.__ashlarUmfSparkDelta4;
if (typeof legacy?.realize !== 'function') throw new Error('existing `.4` bridge entrypoint absent');

const requestBytes = await read(requestPath);
const request = JSON.parse(requestBytes.toString('utf8'));
const legacyReceiptBytes = await read(legacyReceiptPath);
const legacyReceipt = JSON.parse(legacyReceiptBytes.toString('utf8'));
const modelBytes = await read(resolve(typedRoot, 'typed-graph-model.umf.json'));
const model = sdk.readDocument(modelBytes.toString('utf8'), 'json');
if (model.umf !== '0.10.0') throw new Error('typed graph source is not semantic UMF 0.10.0');
const coreGroups = [];
for (const [groupIndex, group] of request.groups.entries()) {
  const recordResults = group.records.map(item => ({
    request: item,
    result: sdk.validateCoreRecordValues(model, item.identity, item.values),
  }));
  const dataset = sdk.validateCoreDatasetValuesCompact(model, group.dataset);
  sdk.verifyCoreDatasetValuesCompact(dataset, model, group.dataset);
  coreGroups.push({groupIndex, recordResults, dataset});
}
const coreOutputBytes = Buffer.from(JSON.stringify({
  profile: 'ashlar.g03-sdk058-public-core-validation/1',
  producer: {sdk: '0.5.8', semanticCore: model.umf, entrySha256: hash(await read(sdkEntry))},
  modelSha256: hash(modelBytes), requestSha256: hash(requestBytes), groups: coreGroups,
}, null, 2) + '\n');
await writeFile(resolve(outputRoot, 'typed-graph-core-results.json'), coreOutputBytes, {flag: 'wx', mode: 0o600});

const typedGraphGroupParity = legacyReceipt.groups.map((oldGroup, groupIndex) => {
  const current = coreGroups[groupIndex];
  const oldRecordResults = oldGroup.records.map(record => record.result);
  const currentRecordResults = current.recordResults.map(record => record.result);
  return {
    groupIndex,
    recordCount: currentRecordResults.length,
    recordResultsExactlyEqual: isDeepStrictEqual(plain(oldRecordResults), plain(currentRecordResults)),
    compactDatasetResultExactlyEqual: isDeepStrictEqual(plain(oldGroup.dataset), plain(current.dataset)),
  };
});
if (legacyReceipt.groups.length !== coreGroups.length ||
    typedGraphGroupParity.some(x => !x.recordResultsExactlyEqual || !x.compactDatasetResultExactlyEqual)) {
  throw new Error('selected release Core outputs differ from the historical typed_graph_values.ts receipt');
}

const roles = ['object_current', 'edge_current', 'tombstone', 'whole_source_history', 'publication_manifest', 'publication_attempt_phase'];
const outputs = [];
for (const role of roles) {
  const sourcePath = resolve(carrierRoot, role + '.umf.json');
  const sourceBytes = await read(sourcePath);
  const source = sdk.readDocument(sourceBytes.toString('utf8'), 'json');
  const path = `/virtual/ashlar-g03-umf058/${role}`;
  const target = {kind: 'delta-path', path};
  const releaseProposal = JSON.parse(JSON.stringify(sdk.realizeSparkDelta4DDL(source, target)));
  const existingProposal = JSON.parse(JSON.stringify(legacy.realize(sourceBytes.toString('utf8'), target)));
  outputs.push({
    role,
    sourceBytes: sourceBytes.length,
    sourceSha256: hash(sourceBytes),
    release: releaseProposal,
    existingBridge: existingProposal,
    equal: isDeepStrictEqual(releaseProposal, existingProposal),
    fieldOrderEqual: isDeepStrictEqual(releaseProposal.schemaJson, existingProposal.schemaJson),
    sqlEqual: releaseProposal.sql === existingProposal.sql,
  });
}
const carrierBytes = Buffer.from(JSON.stringify({
  profile: 'ashlar.g03-sdk058-vs-existing-delta4-carrier/1',
  sdk: {distribution: 'UMF SDK 0.5.8', entry: sdkEntry, entrySha256: hash(sdkEntryBytes)},
  existingBridge: {path: oldBridgePath, sha256: hash(bundleBytes)},
  roles: outputs,
}, null, 2) + '\n');
await writeFile(resolve(outputRoot, 'carrier-proposals.json'), carrierBytes, {flag: 'wx', mode: 0o600});

const report = {
  profile: 'ashlar.g03-sdk058-effect-free-qualification/1',
  runtime: {node: process.version, argv0: process.execPath, nodeSha256: hash(await read(process.execPath)), nodeSizeBytes: (await read(process.execPath)).length},
  release: {distribution: 'UMF SDK 0.5.8', semanticCore: model.umf,
    exported: ['readDocument','validateCoreRecordValues','validateCoreDatasetValuesCompact','verifyCoreDatasetValuesCompact','realizeSparkDelta4DDL']},
  producerClosure: {
    preflight,
    status: closure.status,
    sourceCommit: closure.sourceCommit,
    sourceTree: closure.sourceTree,
    archiveSha256: hash(archiveBytes), archiveSizeBytes: archiveBytes.length,
    closureSha256: hash(closureBytes),
    producerReceiptSha256: hash(producerReceiptBytes),
    installedSdkTreeSha256: closure.selectedSoftware?.installedSdkPackage?.treeSha256,
  },
  inputs: {
    modelSha256: hash(modelBytes), requestSha256: hash(requestBytes),
    coreResultSha256: hash(coreOutputBytes), carrierOutputSha256: hash(carrierBytes),
    oldBridgeSha256: hash(bundleBytes),
  },
  typedGraph: {
    groups: request.groups.length,
    eventCount: request.groups.reduce((n, group) => n + group.events.length, 0),
    recordValidationCount: coreGroups.reduce((n, group) => n + group.recordResults.length, 0),
    everyGroupDatasetVerifiable: coreGroups.length === request.groups.length,
    historicalReceiptComparison: {
      producerRevision: legacyReceipt.producerRevision,
      receiptSha256: hash(legacyReceiptBytes),
      groups: typedGraphGroupParity,
      allGroupsExactlyEqual: typedGraphGroupParity.every(x => x.recordResultsExactlyEqual && x.compactDatasetResultExactlyEqual),
      scope: 'Core record results and compact dataset result objects on the frozen request; historical producer identity and source authority are not transferred.',
    },
  },
  carrier: {roles: roles.length, allProposalsExactlyEqual: outputs.every(x => x.equal),
    differences: outputs.filter(x => !x.equal).map(x => ({role:x.role,
      schemaEqual:x.fieldOrderEqual,sqlEqual:x.sqlEqual,
      releaseSchemaSha256:x.release.schemaSha256,existingSchemaSha256:x.existingBridge.schemaSha256,
      releaseSqlSha256:hash(Buffer.from(x.release.sql)),existingSqlSha256:hash(Buffer.from(x.existingBridge.sql))}))},
  qualification: 'Effect-free public UMF source checks and local Delta 4 CREATE proposals only. Typed-graph parity is limited to public Core record and compact-dataset result objects on the frozen request. No Spark, table, publication, query, managed UC, or source ACK activity.',
};
const reportBytes = Buffer.from(JSON.stringify(report, null, 2) + '\n');
await writeFile(resolve(outputRoot, 'qualification.json'), reportBytes, {flag: 'wx', mode: 0o600});
console.log(JSON.stringify({status: report.carrier.allProposalsExactlyEqual ? 'PASS' : 'DIFFERENCE',
  groups: report.typedGraph.groups, eventCount: report.typedGraph.eventCount,
  roles: report.carrier.roles, differences: report.carrier.differences}));
