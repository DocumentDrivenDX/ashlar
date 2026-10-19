#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bootstrap one fresh local Note installation; never provisions a remote database."""
import argparse,sys
from ashlar_host.truss_local_setup import run_setup_truss_local_command

run=run_setup_truss_local_command

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--configuration',required=True);parser.add_argument('--output',required=True);args=parser.parse_args()
    try:run(args.configuration,args.output)
    except Exception:
        print('Local setup refused; partial local data retained. Select no automatic retry/reset.',file=sys.stderr);raise SystemExit(2)from None
