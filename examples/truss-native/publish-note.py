#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Publish a few real Note feed pages into a fresh private local Delta destination."""
import argparse,sys
from ashlar_host.truss_note_cli import run_publish_truss_note_command

# Preserve the original example library entrypoint; installed CLI shares it.
run=run_publish_truss_note_command

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--configuration',required=True);parser.add_argument('--output',required=True);args=parser.parse_args()
    try:run(args.configuration,args.output)
    except Exception:
        print('Note publication refused; inspect retained destination attempts and source checkpoint before recovery.',file=sys.stderr);raise SystemExit(2)from None
