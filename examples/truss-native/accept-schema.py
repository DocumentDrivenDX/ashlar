#!/usr/bin/env python3
"""Register original UMF documents in an EXISTING compatible Truss installation."""
import argparse,sys
from ashlar_host.truss_schema_acceptance import run_accept_truss_schema_command

run=run_accept_truss_schema_command

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--configuration',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args()
    try:run(args.configuration,args.output)
    except Exception:
        # Driver errors can contain credentials/SQL; don't print them publicly.
        print('Schema acceptance example refused; no automatic retry. Inspect the database outcome before retrying.',file=sys.stderr)
        raise SystemExit(2)from None
