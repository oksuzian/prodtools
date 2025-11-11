#!/usr/bin/env python3
"""
json2jobdef.py: JSON to jobdef generator.

Usage:
  - As module:   python3 -m mu2e_poms_util.json2jobdef --help
  - Direct file: python3 mu2e_poms_util/json2jobdef.py --help
"""
import os, sys
# Allow running this file directly: make package root importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import subprocess
from pathlib import Path
from utils.prod_utils import *
from utils.mixing_utils import *
from utils.jobquery import Mu2eJobPars
from utils.jobdef import create_jobdef
from utils.job_common import get_owner as _get_owner

def _create_inputs_file(dataset):
    """Helper: create inputs.txt file from dataset."""
    from utils.samweb_wrapper import list_files
    result = list_files(f"dh.dataset={dataset} and event_count>0")
    with open('inputs.txt', 'w') as f:
        f.write('\n'.join(result))

def get_parfile_name(config):
    """Generate consistent parfile name from config."""
    return f"cnf.{config['owner']}.{config['desc']}.{config['dsconf']}.0.tar"

def get_fcl_name(config):
    """Generate consistent FCL filename from config."""
    return f"cnf.{config['owner']}.{config['desc']}.{config['dsconf']}.0.fcl"

def validate_required_fields(config, required_fields=None):
    """Validate that config has all required fields."""
    if required_fields is None:
        required_fields = ('simjob_setup', 'fcl', 'dsconf', 'outloc')
    
    for req in required_fields:
        if not config.get(req):
            sys.exit(f"Missing required field: {req}")

def determine_job_type(config):
    """Determine the job type based on config contents.
    
    Returns:
        'resampler' - Resampling jobs with resampler_name
        'merge'     - File merging jobs with merge_factor 
        'mixing'    - Pileup mixing jobs with pbeam
        'stage1'    - Primary simulation jobs (cosmic, beam, etc.)
    """
    if 'resampler_name' in config:
        return 'resampler'
    elif 'merge_factor' in config:
        return 'merge'
    elif 'pbeam' in config:
        return 'mixing'
    else:
        return 'stage1'

def build_jobdef(config, job_args, json_output=False):
    # Create jobdef using the embed approach with custom template to preserve fcl_overrides
    # For mixing jobs, template.fcl is already created by build_pileup_args
    # For non-mixing jobs, create the template here
    fcl_path = config['fcl']
    job_type = determine_job_type(config)

    if job_type != 'mixing':
        write_fcl_template(fcl_path, config.get('fcl_overrides', {}))
    
    # Add MaxEventsToSkip parameter for resampler jobs after template is written
    if job_type == 'resampler':
        with open('template.fcl', 'a') as f:
            f.write(f"physics.filters.{config['resampler_name']}.mu2e.MaxEventsToSkip: {config['_max_events_to_skip']}\n")
    
    # Build the Perl commands that would be equivalent (always build for potential display)
    cmd_parts = [
        'mu2ejobdef',
        '--setup', config['simjob_setup'],
        '--dsconf', config['dsconf'],
        '--desc', config['desc'],
        '--dsowner', config['owner']
    ]
    
    # Only add --run-number if it's present in config
    if 'run' in config:
        cmd_parts.extend(['--run-number', str(config['run'])])
    
    # Only add --events-per-job if it's present in config
    if 'events' in config:
        cmd_parts.extend(['--events-per-job', str(config['events'])])
    
    # Add job_args and template
    cmd_parts.extend(job_args)
    cmd_parts.extend(['--embed', 'template.fcl'])
    
    # Always show the mu2ejobdef equivalent command when verbose logging is enabled
    import logging
    if logging.getLogger().level <= logging.DEBUG:
        print(f"🐪 mu2ejobdef equivalent command: {' '.join(cmd_parts)}")
    
    # Now create jobdef using the template.fcl
    create_jobdef(config, fcl_path='template.fcl', job_args=job_args, embed=True, quiet=json_output)
    
    # Get the parfile name for both modes
    parfile_name = get_parfile_name(config)
    fcl_file = get_fcl_name(config)
    
    if json_output:
        # Return structured data for machine consumption
        result = {
            'success': True,
            'perl_commands': [
                {
                    'type': 'mu2ejobdef',
                    'command': ' '.join(cmd_parts),
                    'desc': config['desc'],
                    'simjob_setup': config['simjob_setup']
                },
                {
                    'type': 'mu2ejobfcl',
                    'command': f"mu2ejobfcl --jobdef {parfile_name} --default-location tape --default-protocol root --index 0 > {fcl_file}",
                    'desc': config['desc'],
                    'index': 0
                }
            ]
        }
        return result
    else:
        # Human-readable output (current behavior)
        print(f"Python mu2ejobdef equivalent command: {' '.join(cmd_parts)}")
        print(f"Running Perl equivalent of: mu2ejobfcl --jobdef {parfile_name} --default-location tape --default-protocol root --index 0 > {fcl_file}")
        return None

def append_jobdef(config, jobdefs_file=None):
    """
    Append job information to a jobdefs file in JSON format.
    Handles both simple and complex outloc structures.
    """
    parfile_name = get_parfile_name(config)
    
    # Query job count if njobs is -1
    njobs = config['njobs']
    if njobs == -1:
        jp = Mu2eJobPars(parfile_name)
        njobs = jp.njobs()
        print(f"Queried job count: {njobs}")
    
    # Create JSON structure for the job definition
    jobdef_entry = {
        "tarball": parfile_name,
        "njobs": njobs,
        "inloc": config['inloc'],
        "outputs": []
    }
    
    # Handle outloc - must be dict with dataset-specific locations
    outloc = config['outloc']
    
    if not isinstance(outloc, dict):
        print(f"Warning: outloc must be a dictionary with dataset-specific locations for {config.get('desc', 'unknown')}")
        return
    
    # Add each dataset with its location
    for dataset_name, location in outloc.items():
        jobdef_entry["outputs"].append({
            "dataset": dataset_name,
            "location": location
        })
    
    # Write JSON entry to file
    _write_jobdef_json_entry(jobdef_entry, jobdefs_file)

def _write_jobdef_json_entry(jobdef_entry, jobdefs_file=None):
    """Helper function to write jobdef entries in JSON format."""
    # Use provided jobdefs file or default to jobdefs_list.json
    if jobdefs_file:
        dsconf_file = Path(jobdefs_file)
    else:
        dsconf_file = Path("jobdefs_list.json")
    
    # Check if file exists and load existing entries
    existing_entries = []
    if dsconf_file.exists():
        try:
            existing_content = dsconf_file.read_text()
            if existing_content.strip():
                existing_entries = json.loads(existing_content)
                if not isinstance(existing_entries, list):
                    existing_entries = [existing_entries]
        except json.JSONDecodeError:
            print(f"Warning: Could not parse existing {dsconf_file}, starting fresh")
            existing_entries = []
    
    # Check for duplicate tarball entries
    tarball_name = jobdef_entry["tarball"]
    for existing in existing_entries:
        if existing.get("tarball") == tarball_name:
            print(f"Entry already exists in {dsconf_file}")
            return
    
    # Add new entry and write back to file
    existing_entries.append(jobdef_entry)
    
    with open(dsconf_file, 'w') as f:
        json.dump(existing_entries, f, indent=2)
    
    print(f"Added JSON entry for {tarball_name} to {dsconf_file}")

def main():
    p = argparse.ArgumentParser(description='Generate Mu2e job definitions from JSON configuration')
    p.add_argument('--json', required=True, help='Input JSON file')
    p.add_argument('--desc', type=str, help='Dataset descriptor')
    p.add_argument('--dsconf', type=str, help='Dataset configuration')
    p.add_argument('--index', type=int, help='Entry index in JSON list')
    p.add_argument('--pushout', action='store_true', help='Enable SAM pushOutput')
    p.add_argument('--verbose', action='store_true', help='Verbose logging')
    p.add_argument('--no-cleanup', action='store_true', help='Keep temporary files (inputs.txt, template.fcl, *Cat.txt)')
    p.add_argument('--jobdefs', help='Custom filename for jobdefs list (default: jobdefs_list.txt)')
    p.add_argument('--json-output', action='store_true', help='Output structured JSON instead of human-readable text')
    args = p.parse_args()

    setup_logging(args.verbose)
    
    # Load and expand the JSON configuration once
    expanded_configs = load_json(Path(args.json))
    
    # If both desc and dsconf are specified, process single entry
    if args.desc and args.dsconf and args.index is None:
        config = find_json_entry(expanded_configs, args.desc, args.dsconf, None)
        process_single_entry(config, json_output=True, pushout=args.pushout, no_cleanup=True, jobdefs_list=args.jobdefs)
    # If dsconf is specified but no desc and no index, process all entries for that dsconf
    elif args.dsconf and args.desc is None and args.index is None:
        process_all_for_dsconf(expanded_configs, args.dsconf, args)
    # If only index is specified, process single entry by index
    elif args.index is not None and args.desc is None and args.dsconf is None:
        config = find_json_entry(expanded_configs, None, None, args.index)
        process_single_entry(config, json_output=True, pushout=args.pushout, no_cleanup=True, jobdefs_list=args.jobdefs)
    else:
        # No filtering specified, show usage
        sys.exit("Please specify either --desc AND --dsconf, --dsconf only, or --index only")

def process_single_entry(config, json_output=True, pushout=False, no_cleanup=True, jobdefs_list=None):
    """Process a single configuration entry (original behavior)"""
    validate_required_fields(config)
    config['owner'] = _get_owner(config)
    config['inloc'] = config.get('inloc', 'none')
    config['njobs'] = config.get('njobs', -1)
    
    # Store the original FCL path for source type detection
    original_fcl_path = config['fcl']
    
    # Create inputs.txt first if needed
    if config.get('input_data'):
        _create_inputs_file(config['input_data'])
    
    job_args = []
    
    job_type = determine_job_type(config)
    
    if job_type == 'resampler':
        # Resampler jobs: calculate MaxEventsToSkip parameter
        try:
            nfiles, nevts = get_def_counts(config['input_data'])
            skip = nevts // nfiles
            # Store skip value for later addition to template
            config['_max_events_to_skip'] = skip
        except Exception as e:
            print(f"Warning: Could not calculate MaxEventsToSkip for {config['input_data']}: {e}")
        job_args = ['--auxinput', f"{config.get('merge_factor',1)}:physics.filters.{config['resampler_name']}.fileNames:inputs.txt"]
        
    elif job_type == 'merge':
        # Merge jobs: simple file merging
        merge_factor = calculate_merge_factor(config)
        job_args = ['--inputs','inputs.txt','--merge-factor', str(merge_factor)]
        
    elif job_type == 'mixing':
        # Mixing jobs: add MaxEventsToSkip parameter to template
        merge_factor = calculate_merge_factor(config)
        job_args = ['--inputs','inputs.txt','--merge-factor', str(merge_factor)]
        job_args += build_pileup_args(config)
        
    else:
        # Stage1 jobs: no special processing needed
        job_args = []
    
    # build_jobdef handles FCL template creation for non-mixing jobs
    # Always fcl-analyze the ORIGINAL FCL file for job structure understanding
    result = build_jobdef(config, job_args, json_output=json_output)
    
    append_jobdef(config, jobdefs_list)    
    # Get the parfile name for pushout operations
    parfile_name = get_parfile_name(config)
    
    # Handle pushout regardless of json_output setting
    if pushout:
        # First check if the local file exists before attempting SAM operations
        if not Path(parfile_name).exists():
            print(f"Warning: Local file {parfile_name} not found, skipping pushout")
        else:
            # Push file to SAM if it doesn't already exist there
            from utils.samweb_wrapper import locate_file
            
            # Check if file exists on SAM
            loc = locate_file(parfile_name)
            if not loc:
                # File doesn't exist on SAM, push it
                print(f"Pushing {parfile_name} to SAM...")
                with open('outputs.txt', 'w') as f:
                    f.write(f"disk {parfile_name} none\n")
                run('pushOutput outputs.txt', shell=True)
            else:
                # File exists on SAM, don't push
                print(f"File {parfile_name} already exists on SAM, skipping push")
    
    if not json_output:
        # Only output human-readable text when not using JSON output
        print(json.dumps(config, indent=2, sort_keys=True))
        
        write_fcl(parfile_name, config.get('inloc', 'tape'), 'root')
        print(f"Generated: {parfile_name}")
    
    # Clean up temporary files AFTER job definition is created (unless --no-cleanup is specified)
    if not no_cleanup:
        temp_files = ['inputs.txt', 'template.fcl', 'mubeamCat.txt', 'elebeamCat.txt', 'neutralsCat.txt', 'mustopCat.txt']
        for temp_file in temp_files:
            temp_path = Path(temp_file)
            if temp_path.exists():
                temp_path.unlink()
                print(f"Cleanup: {temp_file}")
    
    return result

def is_already_expanded(configs):
    """Check if the configuration is already expanded (has scalar values, not lists)"""
    if not isinstance(configs, list) or len(configs) == 0:
        return False
    
    # Check all entries, not just the first one
    for i, config in enumerate(configs):
        if not isinstance(config, dict):
            raise ValueError(f"Entry {i} is not a dictionary: {type(config)}")
        
        # Check if this config has lists (needs expansion)
        values = list(config.values())
        has_lists = any(isinstance(v, list) for v in values)
        
        # If any config has lists, the whole configuration needs expansion
        if has_lists:
            return False
    
    # If no configs have lists, they're all already expanded
    return True

def load_json(json_path):
    """Load and expand JSON configuration if needed"""
    json_text = json_path.read_text()
    configs = json.loads(json_text)
    
    # Only expand mixing configurations, not stage1 or resampler
    if json_path.name == 'mix.json':
        from utils.mixing_utils import expand_mix_config
        if is_already_expanded(configs):
            return configs
        else:
            return expand_mix_config(json_path)
    else:
        # For non-mixing configs, just return as-is
        return configs

def find_json_entry(configs, desc=None, dsconf=None, index=None):
    """Find a matching JSON entry from configuration list"""
    if index is not None:
        try: 
            return configs[index]
        except IndexError: 
            sys.exit(f"Index {index} out of range.")
    
    matches = [e for e in configs if e.get('desc') == desc and e.get('dsconf') == dsconf]
    if len(matches) != 1:
        sys.exit(f"Expected 1 match for desc={desc}, dsconf={dsconf}; found {len(matches)}.")
    return matches[0]

def process_all_for_dsconf(expanded_configs, dsconf, args):
    """Process all entries matching the specified dsconf and generate job definitions for all permutations"""
    
    # Filter to only entries matching the specified dsconf (partial match)
    matching_configs = [config for config in expanded_configs if config.get('dsconf', '').startswith(dsconf)]
    
    if not matching_configs:
        sys.exit(f"No entries found matching dsconf: {dsconf}")
    
    print(f"Found {len(matching_configs)} entries matching dsconf: {dsconf}")
    
    # Process each matching configuration using the existing process_single_entry function
    for i, config in enumerate(matching_configs):
        print(f"\nProcessing entry {i+1}/{len(matching_configs)}: {config.get('desc', 'Unknown')}")
        
        # Check required fields before calling process_single_entry
        try:
            validate_required_fields(config)
        except SystemExit as e:
            print(f"Warning: {e}, skipping entry")
            continue
        
        # Use the existing process_single_entry function
        process_single_entry(config, json_output=True, pushout=args.pushout, no_cleanup=True, jobdefs_list=args.jobdefs)
        
        # Clean up template.fcl for next iteration (since process_single_entry cleans up)
        if Path('template.fcl').exists():
            Path('template.fcl').unlink()

if __name__ == '__main__':
    main()
