#!/usr/bin/env python3
"""Create and edit a Note's authored non-key text through public Truss groups."""
import argparse,sys
from ashlar_host.truss_note_mutation import run_mutate_truss_note_command

run=run_mutate_truss_note_command

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--configuration',required=True);parser.add_argument('--output',required=True);args=parser.parse_args()
    try:run(args.configuration,args.output)
    except Exception:
        print('Object edit example refused; inspect commit outcomes before retrying.',file=sys.stderr);raise SystemExit(2)from None
