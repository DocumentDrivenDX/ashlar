"""Pure argument declarations shared by installed and host query entrypoints."""
from pathlib import Path


def add_arguments(parser):
    """Require the same explicit selections without importing runtime owners."""
    for name in ('configuration','weft-root','java-home','jars','output'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--weft-module-sha256',required=True)
    parser.add_argument('--weft-shim-sha256',required=True)
