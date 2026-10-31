import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {isDeepStrictEqual} from 'node:util';
import {createContext,runInContext} from 'node:vm';
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';

const [sdkPackageRoot, repositoryRoot = process.cwd(), sdkTarball] = process.argv.slice(2);
if (!sdkPackageRoot || !sdkTarball) throw new Error('Usage: node qualify_sdk010_bridge.mjs /path/to/extracted/package /path/to/ashlar /path/to/umf-sdk.tgz');
const root = resolve(repositoryRoot);
const packet = resolve(root, 'docs/helix/04-build/evidence/current-upstream-release-20261029/g03-sdk058-delta4-realization-candidate-20261030');
const sdkPackage = resolve(sdkPackageRoot);
const tarballSha256 = createHash('sha256').update(await readFile(sdkTarball)).digest('hex');
if (tarballSha256 !== '2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c') throw new Error('SDK 0.10.0 tarball digest differs');
const packageJson = JSON.parse(await readFile(resolve(sdkPackage, 'package.json'), 'utf8'));
if (packageJson.version !== '0.10.0') throw new Error('SDK package must be version 0.10.0');
const sdkEntry = resolve(sdkPackage, 'dist/umf.js');
const sdkEntryBytes = await readFile(sdkEntry);
const sdkEntrySha256 = createHash('sha256').update(sdkEntryBytes).digest('hex');
if (sdkEntrySha256 !== 'b32bce2fdcf928bc5b9fd8376b4c7cab9beb1b63d90cb1d20b9ad0ec94595951') throw new Error('SDK entry digest differs');
const sdk = await import(pathToFileURL(sdkEntry).href);
if (typeof sdk.readDocument !== 'function' || typeof sdk.realizeSparkDelta4DDL !== 'function') throw new Error('SDK public API missing required functions');
const bridgePath = resolve(import.meta.dirname, 'bridge.iife.js');
const bridgeBytes = await readFile(bridgePath);
const bridgeSha256 = createHash('sha256').update(bridgeBytes).digest('hex');
if (bridgeSha256 !== '4d0ce3902368d0d1a73312626ec153125992327ee812cd579282445514067735') throw new Error('SDK 0.10.0 source bridge digest differs');
const bridgeEntryBytes = await readFile(resolve(import.meta.dirname, 'bridge-entry.ts'));
const bridgeEntrySha256 = createHash('sha256').update(bridgeEntryBytes).digest('hex');
if (bridgeEntrySha256 !== '739c9579cb4662fbb48123e828ea9b7125eeed085b14f26019880a2d46b1462e') throw new Error('SDK 0.10.0 bridge entry digest differs');
const context = createContext({console});
runInContext(bridgeBytes.toString('utf8'), context, {timeout: 30000});
const baseline = JSON.parse(await readFile(resolve(packet, 'outputs/carrier-proposals.json'), 'utf8'));
const roles = ['object_current','edge_current','tombstone','whole_source_history','publication_manifest','publication_attempt_phase'];
const results = [];
for (const role of roles) {
  const sourceBytes = await readFile(resolve(packet, 'inputs/carriers', `${role}.umf.json`));
  const sourceText = sourceBytes.toString('utf8');
  const target = {kind: 'delta-path', path: `/virtual/ashlar-g03-umf058/${role}`};
  const fromPublicPackage = JSON.parse(JSON.stringify(sdk.realizeSparkDelta4DDL(sdk.readDocument(sourceText, 'json'), target)));
  const fromCandidateBridge = JSON.parse(JSON.stringify(context.__ashlarUmfSparkDelta4Sdk010.realize(sourceText, target)));
  const oldReceiptProposal = baseline.roles.find(item => item.role === role)?.release;
  if (!oldReceiptProposal) throw new Error(`Missing frozen proposal for ${role}`);
  results.push({role, sourceBytes: sourceBytes.length,
    sourceSha256: createHash('sha256').update(sourceBytes).digest('hex'),
    publicPackageEqualsOldReceipt: isDeepStrictEqual(fromPublicPackage, oldReceiptProposal),
    candidateBridgeEqualsOldReceipt: isDeepStrictEqual(fromCandidateBridge, oldReceiptProposal)});
}
const receipt = {profile: 'ashlar-local-current-delta4-umf-source-sdk010/0.1',
  runner: {node: process.version},
  sdk: {version: packageJson.version, tag: 'umf-v0.10.0', commit: '96230e0eef44b255d33575ba496402555b76800e', tarballSha256, sdkEntrySha256},
  bridge: {entrySha256: bridgeEntrySha256, bundleSha256: bridgeSha256, builder: 'Bun 1.4.3'},
  roles: results,
  allPublicPackageEqual: results.every(x => x.publicPackageEqualsOldReceipt),
  allCandidateBridgeEqual: results.every(x => x.candidateBridgeEqualsOldReceipt)};
if (!receipt.allPublicPackageEqual || !receipt.allCandidateBridgeEqual) throw new Error('SDK 0.10.0 Delta 4 output diverged from the frozen proposal set');
console.log(JSON.stringify(receipt, null, 2));
