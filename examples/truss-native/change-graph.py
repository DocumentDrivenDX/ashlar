#!/usr/bin/env python3
"""Update then delete the original cohort pair using current authorized Truss reads."""
import argparse,sys
from ashlar_host.truss_graph_change import run_change_truss_graph_command

run=run_change_truss_graph_command

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--configuration',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args()
    try:run(args.configuration,args.output)
    except Exception:
        print('Graph change example refused; inspect commit outcomes before retrying.',file=sys.stderr)
        raise SystemExit(2)from None
