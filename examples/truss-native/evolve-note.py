#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Accept a compatible Note revision, then use its new field through public Truss."""
import argparse,sys
from ashlar_host.truss_note_evolution import run_evolve_truss_note_command

run=run_evolve_truss_note_command

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--configuration',required=True);parser.add_argument('--output',required=True);args=parser.parse_args()
    try:run(args.configuration,args.output)
    except Exception:
        print('Note evolution refused; inspect durable outcomes before retrying.',file=sys.stderr);raise SystemExit(2)from None
