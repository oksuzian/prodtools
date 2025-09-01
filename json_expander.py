#!/usr/bin/env python3
import os, sys
# Allow running this file directly: make package root importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import argparse
import json
from utils.mixing_utils import expand_configs

def main():
    p = argparse.ArgumentParser(description='Expand JSON configurations by generating all combinations of list parameters')
    p.add_argument('--json', required=True, help='Path to input JSON template configuration')
    p.add_argument('--output', required=True, help='Path to output JSON file')
    p.add_argument('--mixing', action='store_true', help='Add mixing-specific fields to job configurations')
    args = p.parse_args()

    # Load JSON config
    with open(args.json) as f:
        configs = json.load(f)

    # Expand configurations
    all_jobs = expand_configs(configs, args.mixing)

    # Write output JSON
    with open(args.output, 'w') as f:
        json.dump(all_jobs, f, indent=2)
    
    print(f"Generated {len(all_jobs)} job configurations")
    print(f"Wrote to {args.output}")

if __name__ == '__main__':
    main()