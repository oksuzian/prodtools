#!/usr/bin/env python3
import os, sys
# Allow running this file directly: make package root importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
from utils.prod_utils import write_fcl, run

def main():
    p = argparse.ArgumentParser(description='Generate FCL from dataset name or target file')
    p.add_argument('--dataset', help='Dataset name (art: dts.mu2e.RPCInternalPhysical.MDC2020az.art or jobdef: cnf.mu2e.ExtractedCRY.MDC2020av.tar)')
    p.add_argument('--proto', default='root')
    p.add_argument('--loc', default='tape')
    p.add_argument('--index', type=int, default=0)
    p.add_argument('--target', help='Target file (e.g., dts.mu2e.RPCInternalPhysical.MDC2020az.001202_00000296.art)')
    p.add_argument('--local-jobdef', help='Direct path to local job definition file')
    args = p.parse_args()

    # Require either dataset or target, unless using --local-jobdef
    if not args.dataset and not args.target and not args.local_jobdef:
        p.error("Either --dataset or --target must be provided, or use --local-jobdef")

    if args.local_jobdef:
        # Local mode: work with existing local files
        jobdef = args.local_jobdef
        if not os.path.exists(jobdef):
            p.error(f"Job definition file not found: {jobdef}")
        
        print(f"Using local job definition: {jobdef}")
        write_fcl(jobdef, args.loc, args.proto, args.index, args.target)
        
    else:
        # Production mode: use mdh to copy files
        source = args.dataset or args.target
        
        # Extract fields from dataset name directly
        parts = source.split('.')
        if len(parts) < 5:
            p.error(f"Invalid dataset: {source}")
        
        # Remove "Triggered" or "Triggerable" suffix from the description field (index 2)
        # ONLY for 'dig' datasets (index 0)
        if len(parts) >= 3 and parts[0] == 'dig':
            desc = parts[2]
            original_desc = desc
            if desc.endswith('Triggered'):
                parts[2] = desc[:-9]  # Remove "Triggered"
            elif desc.endswith('Triggerable'):
                parts[2] = desc[:-11]  # Remove "Triggerable"
            
            # Debug output to show the transformation
            if parts[2] != original_desc:
                print(f"Transformed description: '{original_desc}' -> '{parts[2]}' (dig dataset)")
        
        # Construct job definition filename: owner, desc, dsconf
        jobdef = f"cnf.{parts[1]}.{parts[2]}.{parts[3]}.0.tar"
        
        # Copy jobdef to local directory
        run(f"mdh copy-file -e 3 -o -v -s disk -l local {jobdef}", shell=True)    
        write_fcl(jobdef, args.loc, args.proto, args.index, args.target)
        os.remove(jobdef) if os.path.exists(jobdef) else None

if __name__ == '__main__':
    main()