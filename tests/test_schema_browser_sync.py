"""The browser copy boundary refuses changed custody, not changed UMF meaning."""
import json
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def copied(tmp_path):
    for folder in ('website/static/model/schema-browser', 'website/schema-browser',
                   'docs/helix/02-design/models/ashlar-delta-runtime'):
        shutil.copytree(ROOT / folder, tmp_path / folder)
    for name in ('tools/build_schema_browser.py', 'website/scripts/check_schema_browser.py'):
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    return tmp_path


def check(root):
    return subprocess.run([sys.executable, str(root / 'website/scripts/check_schema_browser.py')],
                          capture_output=True).returncode


def test_original_browser_custody_passes(tmp_path):
    assert check(copied(tmp_path)) == 0


def test_changed_browser_inputs_and_copy_refuse(tmp_path):
    root = copied(tmp_path)
    paths = ('upstream/assets/explorer.js', 'upstream/assets/explorer.css', 'upstream/index.js', 'schema-catalog.json', 'index.html', 'host.css', 'host.js')
    for name in paths:
        p = root / 'website/static/model/schema-browser' / name
        original = p.read_bytes()
        p.write_bytes(original + b'changed')
        assert check(root) != 0, name
        p.write_bytes(original)
    p = root / 'docs/helix/02-design/models/ashlar-delta-runtime/object_current.umf.json'
    p.write_bytes(p.read_bytes() + b' ')
    assert check(root) != 0


def test_missing_or_changed_pin_and_generator_refuse(tmp_path):
    root = copied(tmp_path)
    p = root / 'website/static/model/schema-browser/provenance.json'
    original = p.read_bytes()
    for revision in (None, 'cea3fa03480de1f3437ecd6d23e500bea618e0f3'):
        value = json.loads(original)
        if revision is None:
            del value['upstream']['revision']
        else:
            value['upstream']['revision'] = revision
        p.write_text(json.dumps(value))
        assert check(root) != 0
    p.write_bytes(original)
    generator = root / 'tools/build_schema_browser.py'
    generator.write_bytes(generator.read_bytes() + b'\n# changed\n')
    assert check(root) != 0


def test_missing_upstream_git_pin_leaves_original_outputs(tmp_path):
    root = copied(tmp_path / 'consumer')
    upstream = tmp_path / 'unqualified-upstream'
    upstream.mkdir()
    output = root / 'website/static/model/schema-browser'
    before = {str(p.relative_to(output)): p.read_bytes() for p in output.rglob('*') if p.is_file()}
    result = subprocess.run([sys.executable, str(root / 'tools/build_schema_browser.py'),
                             str(upstream)], capture_output=True)
    assert result.returncode != 0
    assert {str(p.relative_to(output)): p.read_bytes() for p in output.rglob('*') if p.is_file()} == before


def test_missing_lazy_asset_and_extra_runtime_refuse(tmp_path):
    root = copied(tmp_path)
    assets = root / 'website/static/model/schema-browser/upstream/assets'
    chunk = next(assets.glob('chunk-*.js'))
    original = chunk.read_bytes(); chunk.unlink()
    assert check(root) != 0
    chunk.write_bytes(original)
    (assets / 'unknown.js').write_text('changed')
    assert check(root) != 0
