"""Static independent wheel/installed RECORD review; no product/SDK imports."""
from pathlib import Path, PurePosixPath
from email.parser import BytesParser
import base64
import configparser
import csv
import hashlib
import io
import json
import os
import stat
import sys
import urllib.parse
import zipfile

ROOT = Path('/private/tmp/ashlar-otel-installed-preparation-20261010-a')
OUT = Path('/private/tmp/astra-otel-installed-record-review-20261010-a.json')
INVENTORY = Path('/private/tmp/astra-otel-installed-record-review-20261010-a-inventory.json')
FREEZE_SHA = '633280aecb8baed0278887010ddba28723f0824b5cd0da5f783397a127ca0ef8'
WHEEL_SHA = 'eea05f5c712b94a6b6a1ae32de6742a459be16fb9b2f3be8c5890478f9db7de5'
observed = {}
assertion_count = 0

def check(test, message):
    global assertion_count
    assertion_count += 1
    if not test:
        raise AssertionError(message)

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def read(path):
    path = Path(path)
    raw = path.read_bytes()
    descriptor = {'path': str(path), 'bytes': len(raw), 'sha256': sha(raw)}
    previous = observed.setdefault(str(path), descriptor)
    check(previous == descriptor, 'file changed between reads: ' + str(path))
    return raw

def desc(path):
    raw = read(path)
    return {'path': str(path), 'bytes': len(raw), 'sha256': sha(raw)}

def pinned(item):
    check(desc(item['path']) == item, 'pin mismatch: ' + item['path'])

def is_under(path, root):
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False

def normalized(name):
    return name.lower().replace('_', '-').replace('.', '-')

def regular(path):
    check(stat.S_ISREG(path.lstat().st_mode), 'not a regular non-symlink file: ' + str(path))

freeze_path = ROOT/'freeze-b.json'
freeze_raw = read(freeze_path)
check(sha(freeze_raw) == FREEZE_SHA, 'wrong freeze B')
freeze = json.loads(freeze_raw)
check(len({x['path'] for x in freeze['files']}) == len(freeze['files']), 'duplicate freeze paths')
for item in freeze['files']:
    pinned(item)
pinned(freeze['wheel'])
check(freeze['wheel']['sha256'] == WHEEL_SHA and freeze['wheel']['bytes'] == 272871, 'wrong Ashlar wheel')
inputs = json.loads(read(ROOT/'inputs.json'))
for item in inputs:
    pinned(item)
custody = json.loads(read(ROOT/'installed-custody.json'))
dependency_custody = json.loads(read(ROOT/'dependency-payload-custody.json'))
site = Path(custody['site'])
environment = ROOT/'environment'
check(site == environment/'lib/python3.11/site-packages', 'unexpected site location')
check(site.resolve() == site, 'site ancestor symlink')
check(custody['installedVersion'] == '0.1.0.dev0', 'custody version mismatch')
download_path = Path('/private/tmp/ashlar-otel-wheel-inputs-20261010-c/download.json')
download = json.loads(read(download_path))
wheel_inputs = {x['path']: x for x in inputs if x['path'].endswith('.whl')}
download_wheels = []
for item in download['files']:
    p = download_path.parent/item['filename']
    check(str(p) in wheel_inputs, 'unfrozen dependency wheel')
    check(wheel_inputs[str(p)]['sha256'] == item['sha256'], 'download wheel digest mismatch')
    download_wheels.append({'path': str(p), 'name': item['name'], 'version': item['version']})
check(set(wheel_inputs) == {x['path'] for x in download_wheels}, 'dependency input set mismatch')
check(len(download_wheels) == len({x['path'] for x in download_wheels}) == 16, 'dependency wheel duplication/count')
wheels = [{'path': freeze['wheel']['path'], 'name': 'ashlar-graph-toolkit', 'version': '0.1.0.dev0'}] + download_wheels

payloads = {}
record_paths = set()
record_files = []
wheel_summaries = []
dependency_copies = []
ashlar_copies = []
distribution_names = set()
console_scripts = {}
generated_paths = set()
all_owned_paths = set()
record_summary = []
requirements = {}

for selected in wheels:
    wheel_path = Path(selected['path'])
    wheel_raw = read(wheel_path)
    with zipfile.ZipFile(io.BytesIO(wheel_raw)) as archive:
        infos = archive.infolist()
        names = [i.filename for i in infos]
        check(len(names) == len(set(names)), 'duplicate zip member')
        check(archive.testzip() is None, 'CRC mismatch')
        files = {}
        for info in infos:
            name = info.filename
            check(not info.flag_bits & 1, 'encrypted zip member')
            check(not name.startswith('/') and '\\' not in name and '..' not in PurePosixPath(name).parts, 'unsafe zip member')
            check(str(PurePosixPath(name)) == name.rstrip('/'), 'noncanonical zip member')
            check(not any(x.endswith('.data') for x in PurePosixPath(name).parts), 'unhandled wheel relocation')
            mode = info.external_attr >> 16
            check(stat.S_IFMT(mode) in (0, stat.S_IFREG, stat.S_IFDIR), 'nonregular zip member type')
            if not info.is_dir():
                files[name] = archive.read(info)
        metadata_names = [n for n in files if n.endswith('.dist-info/METADATA')]
        check(len(metadata_names) == 1, 'metadata count')
        dist_info = metadata_names[0].rsplit('/', 1)[0]
        check(dist_info not in distribution_names, 'duplicate distribution')
        distribution_names.add(dist_info)
        meta = BytesParser().parsebytes(files[metadata_names[0]])
        check(normalized(meta['Name']) == normalized(selected['name']) and meta['Version'] == selected['version'], 'embedded name/version mismatch')
        installed_meta = read(site/metadata_names[0])
        check(installed_meta == files[metadata_names[0]], 'installed metadata mismatch')
        requirements[normalized(meta['Name'])] = {'version': meta['Version'], 'requiresPython': meta['Requires-Python'], 'requiresDist': meta.get_all('Requires-Dist', [])}
        wheel_meta = BytesParser().parsebytes(files[dist_info+'/WHEEL'])
        check(wheel_meta['Wheel-Version'] == '1.0', 'unsupported wheel version')
        wheel_record = dist_info+'/RECORD'
        rows = list(csv.reader(io.StringIO(files[wheel_record].decode('utf-8'), newline='')))
        check(all(len(row) == 3 for row in rows), 'wheel RECORD row width')
        check(len(rows) == len({row[0] for row in rows}), 'duplicate wheel RECORD paths')
        check({row[0] for row in rows} == set(files), 'wheel RECORD closure mismatch')
        for name, digest, size in rows:
            if name == wheel_record:
                check(digest == size == '', 'wheel RECORD self row')
            else:
                raw = files[name]
                check(digest == 'sha256='+base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b'=').decode(), 'wheel RECORD hash mismatch')
                check(size == str(len(raw)), 'wheel RECORD size mismatch')
        copies = []
        for name, raw in files.items():
            if name == wheel_record:
                continue
            p = site/name
            check(p.resolve() == p and is_under(p, site), 'relocated or symlink installed payload')
            regular(p)
            check(read(p) == raw, 'wheel/installed byte mismatch: '+name)
            check(str(p) not in payloads, 'overlapping wheel ownership')
            payloads[str(p)] = {'path': str(p), 'bytes': len(raw), 'sha256': sha(raw), 'wheel': wheel_path.name, 'member': name}
            copies.append(payloads[str(p)])
            if selected['name'] == 'ashlar-graph-toolkit':
                ashlar_copies.append(payloads[str(p)])
            else:
                dependency_copies.append(payloads[str(p)])
        ep_path = dist_info+'/entry_points.txt'
        if ep_path in files:
            parser = configparser.ConfigParser()
            parser.read_string(files[ep_path].decode('utf-8'))
            for key, value in parser.items('console_scripts') if parser.has_section('console_scripts') else []:
                check(key not in console_scripts, 'duplicate console script')
                console_scripts[key] = value
        installed_record_path = site/wheel_record
        regular(installed_record_path)
        installed_record = read(installed_record_path)
        record_paths.add(str(installed_record_path))
        installed_rows = list(csv.reader(io.StringIO(installed_record.decode('utf-8'), newline='')))
        check(all(len(row) == 3 for row in installed_rows), 'installed RECORD row width')
        check(len(installed_rows) == len({row[0] for row in installed_rows}), 'duplicate installed RECORD rows')
        distribution_owned = set()
        hashed = 0
        for name, digest, size in installed_rows:
            path = (site/name).resolve()
            check(is_under(path, environment), 'RECORD escapes environment')
            regular(path)
            check(str(path) not in distribution_owned and str(path) not in all_owned_paths, 'duplicate installed ownership')
            distribution_owned.add(str(path))
            all_owned_paths.add(str(path))
            raw = read(path)
            if name == wheel_record:
                check(digest == size == '', 'installed self RECORD row')
            else:
                check(digest == 'sha256='+base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b'=').decode(), 'installed RECORD digest mismatch')
                check(size == str(len(raw)), 'installed RECORD size mismatch')
                record_files.append({'path': str(path), 'bytes': len(raw), 'sha256': sha(raw)})
                hashed += 1
        wheel_owned = {str(site/n) for n in files}
        check(wheel_owned.issubset(distribution_owned), 'installed RECORD missing wheel member')
        additions = distribution_owned-wheel_owned
        expected_additions = {str(site/dist_info/x) for x in ('INSTALLER', 'REQUESTED', 'direct_url.json')}
        local_entry_points = {}
        if ep_path in files:
            parser = configparser.ConfigParser()
            parser.read_string(files[ep_path].decode('utf-8'))
            local_entry_points = dict(parser.items('console_scripts')) if parser.has_section('console_scripts') else {}
            expected_additions.update(str(environment/'bin'/name) for name in local_entry_points)
        check(additions == expected_additions, 'unexpected installation additions')
        generated_paths.update(additions)
        check(read(site/dist_info/'INSTALLER') == b'pip\n', 'unexpected installer')
        check(read(site/dist_info/'REQUESTED') == b'', 'unexpected REQUESTED')
        direct = json.loads(read(site/dist_info/'direct_url.json'))
        expected_direct = {'archive_info': {'hash': 'sha256='+sha(wheel_raw), 'hashes': {'sha256': sha(wheel_raw)}}, 'url': wheel_path.as_uri()}
        check(direct == expected_direct, 'direct_url mismatch')
        for name, target in local_entry_points.items():
            module, attr = target.split(':')
            expected_script = ('#!'+str(environment/'bin/python')+'\n# -*- coding: utf-8 -*-\nimport re\nimport sys\nfrom '+module+' import '+attr+"\nif __name__ == '__main__':\n    sys.argv[0] = re.sub(r'(-script\\.pyw|\\.exe)?$', '', sys.argv[0])\n    sys.exit("+attr+'())\n').encode()
            check(read(environment/'bin'/name) == expected_script, 'unexpected console script')
        wheel_summaries.append({'wheel': desc(wheel_path), 'name': meta['Name'], 'version': meta['Version'], 'distInfo': dist_info, 'members': len(files), 'payloadMembers': len(copies), 'wheelRecordRows': len(rows), 'wheelMetadata': {'rootIsPurelib': wheel_meta['Root-Is-Purelib'], 'tags': wheel_meta.get_all('Tag', [])}, 'installedRecord': desc(installed_record_path), 'installedRows': len(installed_rows), 'installedHashedRows': hashed, 'generatedFiles': len(additions)})

actual_site_files = set()
directories = set()
for base, dirs, names in os.walk(site, followlinks=False):
    base = Path(base)
    for name in dirs:
        path = base/name
        check(stat.S_ISDIR(path.lstat().st_mode), 'site directory symlink')
        directories.add(str(path))
    for name in names:
        path = base/name
        regular(path)
        actual_site_files.add(str(path))
expected_site_files = {p for p in all_owned_paths if is_under(Path(p), site)}
check(actual_site_files == expected_site_files, 'unrecorded or missing site files')
expected_directories = set()
for filename in expected_site_files:
    parent = Path(filename).parent
    while parent != site:
        expected_directories.add(str(parent))
        parent = parent.parent
check(directories == expected_directories, 'unaccounted site directories')
check(not any(p.endswith(('.pyc', '.pth')) or '__pycache__' in Path(p).parts for p in actual_site_files), 'runtime bytecode or path injection artifact')
check({p.name for p in site.glob('*.dist-info')} == distribution_names, 'distribution set mismatch')
check(console_scripts == {'ashlar': 'ashlar.cli:main', 'normalizer': 'charset_normalizer.cli:cli_detect'}, 'console entry point set')

def as_map(items):
    result = {x['path']: x for x in items}
    check(len(result) == len(items), 'duplicate reported custody path')
    return result

check(as_map(record_files) == as_map(custody['recordFiles']), 'reported installed RECORD vector mismatch')
check(as_map(dependency_copies) == as_map(dependency_custody['installedSDKWheelMemberCopies']), 'reported dependency payload vector mismatch')
check(as_map([desc(p) for p in sorted(record_paths)]) == as_map(dependency_custody['installedRECORDs']), 'reported RECORD file vector mismatch')
actual_versions = {selected['name']: selected['version'] for selected in download_wheels}
check(custody['dependencyVersions'] == actual_versions, 'reported versions mismatch')
for name, expected_path in custody['modulePaths'].items():
    check(expected_path == str(site/Path(*name.split('.')).with_suffix('.py')) if name != 'ashlar' else expected_path == str(site/'ashlar/__init__.py'), 'reported module path mismatch')
    check(expected_path in payloads, 'module path outside wheel')
source_files = []
for item in custody['installedSourceFiles']:
    payload = payloads.get(item['path'])
    check(payload is not None and payload['wheel'] == Path(freeze['wheel']['path']).name, 'source not owned by Ashlar wheel')
    check(item['sourcePath'] == 'src/'+payload['member'], 'sourcePath mapping')
    check(item['bytes'] == payload['bytes'] and item['sha256'] == payload['sha256'], 'source custody hash mismatch')
    source_files.append(item['path'])
check(len(source_files) == len(set(source_files)), 'duplicate source files')
check(set(source_files) == {p for p,v in payloads.items() if v['wheel'] == Path(freeze['wheel']['path']).name and v['member'].startswith(('ashlar/', 'ashlar_host/'))}, 'Ashlar source vector completeness')

pyvenv = read(environment/'pyvenv.cfg').decode()
cfg = dict(line.split(' = ', 1) for line in pyvenv.splitlines())
check(cfg['include-system-site-packages'] == 'false' and cfg['version'] == '3.11.17', 'venv configuration')
binary = environment/'bin/python'
check(binary.is_symlink(), 'expected environment interpreter symlink')
check(binary.resolve() == Path('/Users/erik/.local/share/uv/python/cpython-3.11.17-macos-aarch64-none/bin/python3.11'), 'interpreter target drift')
check(sha(read(binary)) == '5ebf120b62e8d02ab22485ca328da471db27fd278069df3de01c957cc02d9cd6', 'interpreter binary drift')
expected_bin = {'Activate.ps1', 'activate', 'activate.csh', 'activate.fish', 'python', 'python3', 'python3.11', 'ashlar', 'normalizer'}
check({p.name for p in (environment/'bin').iterdir()} == expected_bin, 'bin closure')
environment_support = []
for path in sorted((environment/'bin').iterdir()):
    if path.name in ('ashlar', 'normalizer'):
        continue
    descriptor = desc(path)
    if path.is_symlink():
        descriptor['linkTarget'] = os.readlink(path)
        descriptor['resolvedTarget'] = str(path.resolve())
    environment_support.append(descriptor)
environment_support.append(desc(environment/'pyvenv.cfg'))
check({p.name for p in environment.iterdir()} == {'bin', 'include', 'lib', 'pyvenv.cfg'}, 'environment root closure')
check(not any(p.is_file() or p.is_symlink() for p in (environment/'include').rglob('*')), 'unexpected include payload')
check({p.name for p in (environment/'lib').iterdir()} == {'python3.11'}, 'lib closure')
check({p.name for p in (environment/'lib/python3.11').iterdir()} == {'site-packages'}, 'python lib closure')
check({p.name for p in (ROOT/'wheels').iterdir()} == {Path(freeze['wheel']['path']).name}, 'wheel output closure')
check(not list((ROOT/'tmp').iterdir()), 'temporary output not empty')

# Re-read every observed file before issuing this bounded snapshot receipt.
for path, before in list(observed.items()):
    raw = Path(path).read_bytes()
    check(before == {'path': path, 'bytes': len(raw), 'sha256': sha(raw)}, 'closing file changed: '+path)

inventory = {'format': 'astra-static-installed-inventory/0.1', 'wheelPayloadFiles': sorted(payloads.values(), key=lambda x:x['path']), 'installedHashedRecordFiles': sorted(record_files, key=lambda x:x['path']), 'installedRecords': [desc(p) for p in sorted(record_paths)], 'environmentSupportFiles': environment_support, 'observedInputAndOutputFiles': sorted(observed.values(), key=lambda x:x['path'])}
INVENTORY.write_text(json.dumps(inventory, indent=2, sort_keys=True)+'\n')
receipt = {
 'format': 'astra-independent-installed-record-review/0.1',
 'verdict': 'approved-static-installed-payload-custody-only',
 'scope': 'Read-only static wheel archives, installed files, metadata, RECORD integrity and selected environment closure. No product or SDK import, worker/provider startup, receiver, network or native operation was performed by this reviewer.',
 'freeze': desc(freeze_path),
 'sourceCommitClaimInFreeze': freeze['sourceCommit'],
 'reviewScript': desc(Path(__file__)),
 'reviewInterpreter': sys.version,
 'inventory': desc(INVENTORY),
 'wheels': wheel_summaries,
 'counts': {'selectedWheels': len(wheels), 'dependencyWheels': len(download_wheels), 'ashlarWheelNonRecordMembers': len(ashlar_copies), 'ashlarSourceMembers': len(source_files), 'dependencyWheelNonRecordMembers': len(dependency_copies), 'allWheelNonRecordMembers': len(payloads), 'installedRecords': len(record_paths), 'installedRecordRows': len(all_owned_paths), 'installedHashedRecordRows': len(record_files), 'installedRecordSelfRows': len(record_paths), 'siteFiles': len(actual_site_files), 'siteDirectories': len(directories), 'generatedInstallFilesIncludingLaunchers': len(generated_paths), 'recordOwnedLaunchers': len(console_scripts), 'assertions': assertion_count},
 'checks': [
  'Exact freeze B digest, all 22 frozen file pins, the Ashlar wheel pin and all selected preparation input pins rehashed.',
  'All 17 ZIP archives have unique canonical member paths, no traversal, encryption, links or unsupported .data relocation, valid ZIP CRCs and exact wheel RECORD coverage.',
  'Every non-RECORD member independently compared byte-for-byte with its installed path; no overlapping wheel ownership.',
  'Embedded and installed distribution names/versions and WHEEL metadata inspected; all 17 wheel/installed METADATA files identical.',
  'All installed RECORD rows independently parsed; each non-self row has exact sha256/size, no duplicate ownership and no escape from the selected environment.',
  'Exactly one unhashed self RECORD per distribution; generated RECORDs independently hashed and compared with owner evidence.',
  'Only the expected generated INSTALLER, REQUESTED, direct_url and two console launchers are added; all direct_url wheel hashes and local paths match; both launchers exactly implement declared entry points.',
  'Package-directory files and directories equal RECORD ownership closure, with no unrecorded files, symlinks, pyc or pth files.',
  'Fresh environment support-file inventory, interpreter link target and binary, no-system-site flag, bin/lib/include shape, single wheel output and empty preparation tmp directory checked.',
  'Owner installed source, dependency payload, RECORD content and RECORD-file evidence vectors reproduce exactly from independent archive and filesystem enumeration.',
  'All observed inputs and installed output bytes rehashed at review close.'
 ],
 'metadataRequirements': requirements,
 'findings': [],
 'limitations': [
  'Static package and selected environment custody only; no adapter runtime, actual receiver, DNS/HTTP deadline, native/ACK or full C006 qualification.',
  'Source-to-Git and preparation command review belong to the parent review and are not established by this receipt.',
  'Requires-Dist and Requires-Python declarations are retained as observed metadata; this review does not execute a dependency resolver or prove every optional extra.',
  'Exact selected interpreter binary and virtual-environment inventory do not establish operating-system, standard-library, dynamic-library or whole-machine hermetic closure.',
  'RECORD self entries are conventionally unhashed in RECORD; their full file bytes are separately pinned in this receipt and inventory.',
  'No software vulnerability, provenance-signature, reproducible-build or broad platform/interpreter compatibility claim.'
 ]
}
OUT.write_text(json.dumps(receipt, indent=2, sort_keys=True)+'\n')
print(json.dumps({'receipt': desc(OUT), 'inventory': desc(INVENTORY), 'counts': receipt['counts'], 'verdict': receipt['verdict']}, sort_keys=True))
