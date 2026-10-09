"""Repository convenience command for the installed original commerce adapter."""
from pathlib import Path
from ashlar.commerce_source import SOURCE_SHA, GRAPH_SHA, build_transaction, encode, write_candidate
ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/commerce/upstream'

def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-system',required=True)
    parser.add_argument('--binding-profile',choices=['ashlar-commerce-development-bindings/0.1','ashlar-commerce-development-bindings/0.2'],default='ashlar-commerce-development-bindings/0.2')
    parser.add_argument('--output',type=Path,required=True,help='Fresh candidate fixture directory')
    args=parser.parse_args()
    write_candidate(ROOT/'ontology.json', ROOT/'graph/fixture.json', args.output, source_system=args.source_system, binding_profile=args.binding_profile)
    print('Created candidate commerce transaction: 11 objects, 10 edges; development bindings only')


if __name__=='__main__':main()
