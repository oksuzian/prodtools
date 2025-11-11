#!/usr/bin/env python3
"""
Mixing utilities for Mu2e production scripts.
"""

import copy
import json
import sys
import itertools
from .prod_utils import *
from .samweb_wrapper import list_files

def _get_first_if_list(value):
    """Helper: get first element if value is a list, otherwise return value."""
    return value[0] if isinstance(value, list) and value else value

def _create_pileup_catalog(dataset, filename):
    """Helper: create pileup catalog file from dataset."""
    result = list_files(f"dh.dataset={dataset} and event_count>0")
    with open(filename, 'w') as f:
        f.write('\n'.join(result))

# Pileup mixer configurations
PILEUP_MIXERS = {
    'mubeam': 'MuBeamFlashMixer',
    'elebeam': 'EleBeamFlashMixer',
    'neutrals': 'NeutralsFlashMixer',
    'mustop': 'MuStopPileupMixer',
}

# Mixing-specific FCL includes
MIXING_FCL_INCLUDES = {
    "Mix1BB": "Production/JobConfig/mixing/OneBB.fcl",
    "Mix2BB": "Production/JobConfig/mixing/TwoBB.fcl", 
    "MixLow": "Production/JobConfig/mixing/LowIntensity.fcl",
    "MixSeq": "Production/JobConfig/mixing/NoPrimaryPBISequence.fcl"
}

def build_pileup_args(config):
    """Build command-line arguments for pileup mixing configuration."""
    args = []
    
    # Always create template.fcl fresh for mixing jobs
    with open('template.fcl', 'w') as f:
        # Write base include directive
        f.write(f'#include "{config["fcl"]}"\n')
        
        # Process pileup datasets
        for key, mixer in PILEUP_MIXERS.items():
            ds = config.get(f"{key}_dataset")
            cnt = config.get(f"{key}_count", 0)
            if not ds or cnt <= 0:
                continue
            pileup_list = f"{key}Cat.txt"
            # Get ALL pileup files from the dataset and write them to the catalog
            _create_pileup_catalog(ds, pileup_list)
            nfiles, nevts = get_def_counts(ds)
            skip = nevts // nfiles if nfiles > 0 else 0
            print(f"physics.filters.{mixer}.mu2e.MaxEventsToSkip: {skip}", file=f)
            # Use the JSON count parameter - mu2ejobdef will select the first cnt files from the full list
            args += ['--auxinput', f"{cnt}:physics.filters.{mixer}.fileNames:{pileup_list}"]
        
        # Add FCL overrides
        fcl_overrides = _get_first_if_list(config.get('fcl_overrides', {}))
        
        if fcl_overrides:
            for key, val in fcl_overrides.items():
                if key == '#include':
                    includes = val if isinstance(val, list) else [val]
                    for inc in includes:
                        f.write(f'#include "{inc}"\n')
                else:
                    f.write(f'{key}: {json.dumps(val) if isinstance(val, str) else val}\n')
        
        # Add pbeam-specific FCL include based on the pbeam field
        pbeam = _get_first_if_list(config.get('pbeam'))
        if pbeam and pbeam in MIXING_FCL_INCLUDES:
            f.write(f'#include "{MIXING_FCL_INCLUDES[pbeam]}"\n')
        
        # Add output filename overrides for mixing jobs (after base FCL include)
        # This ensures they override the default values from the base templates
        owner = _get_first_if_list(config.get('owner', 'mu2e'))
        desc = _get_first_if_list(config.get('desc', 'unknown'))
        dsconf = _get_first_if_list(config.get('dsconf', 'unknown'))
        f.write(f'outputs.TriggeredOutput.fileName: "dig.{owner}.{desc}Triggered.{dsconf}.sequencer.art"\n')
        f.write(f'outputs.TriggerableOutput.fileName: "dig.{owner}.{desc}Triggerable.{dsconf}.sequencer.art"\n')
    
    return args

def prepare_fields_for_mixing(config):
    """Prepare job configuration for mixing by adding mixing-specific fields."""
    # Create a copy of the config to modify
    modified_config = copy.deepcopy(config)
    
    # Extract desc field from input_data and pbeam
    input_data = _get_first_if_list(config['input_data'])
    dsdesc = input_data.split('.')[2] if input_data else "unknown"
    
    pbeam = _get_first_if_list(config['pbeam'])
    modified_config['desc'] = dsdesc + pbeam
    
    # Add pbeam-specific FCL includes
    if pbeam not in MIXING_FCL_INCLUDES:
        raise ValueError(f"pbeam value '{pbeam}' is not supported. Supported values: {list(MIXING_FCL_INCLUDES.keys())}")
    
    # Note: pbeam-specific FCL include is now handled in build_pileup_args
    
    return modified_config



def expand_configs(configs, mixing=False):
    """
    Expand configurations into individual job configurations.
    
    Args:
        configs: List of configuration dictionaries
        mixing: Whether to apply mixing-specific modifications
        
    Returns:
        List of expanded job configurations
    """
    # Generate jobs for each configuration
    all_jobs = []
    
    for i, config in enumerate(configs):
        # Validate that each config is a dictionary
        if not isinstance(config, dict):
            raise ValueError(f"Configuration at index {i} is not a dictionary: {type(config)} - {config}")
            
        # Separate list and non-list fields
        list_fields = {k: v for k, v in config.items() if isinstance(v, list)}
        non_list_fields = {k: v for k, v in config.items() if not isinstance(v, list)}
        
        if list_fields:
            # Validate list fields are non-empty
            for key, value in list_fields.items():
                if len(value) == 0:
                    raise ValueError(f"List for key '{key}' is empty. All lists must have at least one value.")
            
            # Generate combinations for list fields, keeping non-list fields constant
            param_names = list(list_fields.keys())
            param_values = list(list_fields.values())
            
            for combination in itertools.product(*param_values):
                # Create job with this combination
                job = dict(zip(param_names, combination))
                # Add the non-list fields (create deep copy to avoid reference issues)
                job.update(copy.deepcopy(non_list_fields))
                
                # Ensure fcl_overrides is completely fresh for each job
                if 'fcl_overrides' in job:
                    job['fcl_overrides'] = copy.deepcopy(_get_first_if_list(config.get('fcl_overrides', {})))
                
                # Modify job for mixing if requested
                if mixing:
                    job = prepare_fields_for_mixing(job)
                
                all_jobs.append(job)
        else:
            # All values are non-list, just add directly
            if mixing:
                config = prepare_fields_for_mixing(config)
            all_jobs.append(config)

    return all_jobs

def expand_mix_config(json_path):
    """Expand mixing configuration using expand_configs."""
    # Load JSON config
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")
    
    try:
        with json_path.open() as f:
            configs = json.load(f)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse JSON file {json_path}: {e}")
    
    # Validate JSON structure
    if not isinstance(configs, list):
        raise ValueError(f"JSON file {json_path} must contain a list of configurations, got {type(configs)}")
    
    if len(configs) == 0:
        raise ValueError(f"JSON file {json_path} contains no configurations")
    
    # Expand configurations with mixing enabled
    return expand_configs(configs, mixing=True)


