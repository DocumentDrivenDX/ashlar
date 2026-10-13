"""Read-only probe: python probe.py AUTHENTIC_WHEEL ORIGINAL_REQUEST OUTPUT_FILE."""
import hashlib, json, sys, tempfile, zipfile
from pathlib import Path
wheel, request, output = map(Path, sys.argv[1:])
assert hashlib.sha256(wheel.read_bytes()).hexdigest() == 'a977fa6d6954fe7277ac6e2c30eaa42c0f0ab5fe8f9d59b7b9691274d5493df9'
with tempfile.TemporaryDirectory() as directory:
    with zipfile.ZipFile(wheel) as archive:
        assert archive.namelist() == ['weft_sql-0.4.17.dist-info/METADATA', 'weft_sql-0.4.17.dist-info/WHEEL', 'weft/__init__.py', 'weft/weft.abi3.so', 'weft_sql-0.4.17.dist-info/RECORD']
        archive.extractall(directory)
    sys.path.insert(0, directory)
    import weft
    response = weft.compile_json(request.read_bytes().decode('utf-8'))
    with output.open('x') as stream:
        stream.write(response)
    print(json.dumps({'exports': [n for n in dir(weft) if not n.startswith('_') and callable(getattr(weft, n))], 'status': json.loads(response)['status']}))
