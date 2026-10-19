#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Create two cohort objects and their independent edge through public Truss groups."""
import argparse,sys
from ashlar_host.truss_graph_mutation import run_mutate_truss_graph_command

run=run_mutate_truss_graph_command

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--configuration',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args()
    try:run(args.configuration,args.output)
    except Exception:
        print('Graph mutation example refused; inspect commit outcome before retrying.',file=sys.stderr)
        raise SystemExit(2)from None
