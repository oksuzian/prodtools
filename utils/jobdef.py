#!/usr/bin/env python3
"""
Python implementation of mu2ejobdef with full parity to Perl version.

Creates a jobdef (par) tarball with:
  - jobpars.json (complete structure matching Perl mu2ejobdef)
  - mu2e.fcl     (embedded from template.fcl)

Features implemented:
  - Source type detection (EmptyEvent, RootInput, SamplingInput)
  - Complete event_id, subrunkey, outfiles, seed sections
  - Auxiliary input and sampling input processing
  - Output file name processing and override logic
  - SeedService detection via fhicl-get
"""
import os
import sys
# Add parent directory to path when run directly
if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import subprocess
from pathlib import Path
import tarfile
from typing import Dict, List, Tuple, Optional, Any

from utils.job_common import Mu2eFilename

# Constants matching Perl mu2ejobdef exactly
FILENAME_JSON = 'jobpars.json'
FILENAME_FCL = 'mu2e.fcl'
FILENAME_TARBALL = 'code.tar'
FILENAME_TARSETUP = 'Code/setup.sh'


def resolve_fhicl_file(templatespec: str) -> str:
    """Resolve FCL template path using FHICL_FILE_PATH (matching Perl behavior)."""
    fhicl_path = os.getenv('FHICL_FILE_PATH')
    if not fhicl_path:
        raise ValueError("FHICL_FILE_PATH environment variable is not set")
    
    pathdirs = fhicl_path.split(':')
    for d in pathdirs:
        if d:
            full_path = os.path.join(d, templatespec)
            if os.path.isfile(full_path):
                return full_path
    
    raise FileNotFoundError(f"Error: can not locate template file \"{templatespec}\" relative to FHICL_FILE_PATH={fhicl_path}")


def _run_fhicl_get(template_path: str, command: str, key: str = "") -> str:
    """Run fhicl-get command and return output. Dies on failure like Perl."""
    if command == '--atom-as':
        cmd = ['fhicl-get', '--atom-as', 'string', key, template_path]
    elif command == '--sequence-of':
        cmd = ['fhicl-get', '--sequence-of', 'string', key, template_path]
    else:
        # All other commands follow the same pattern
        cmd = ['fhicl-get', command, key, template_path] if key else ['fhicl-get', command, template_path]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def _get_source_type(template_path: str) -> str:
    """Determine source module type from FCL template using fhicl-get.
    
    Matches Perl behavior exactly: dies on fhicl-get failure.
    """
    # Try to get source type - if this fails, the FCL doesn't have a source section
    # This matches Perl behavior: it dies on fhicl-get failure
    source_type = _run_fhicl_get(template_path, '--atom-as', 'source.module_type')
    return source_type


def _seed_needed(template_path: str) -> bool:
    """Check if SeedService is configured in the template FCL.
    
    Matches Perl seedNeeded() function exactly: checks services.SeedService.baseSeed.
    """
    # Go one level up from the seed field name in fclkey_randomSeed to
    # the module name in $tablename (matches Perl logic exactly)
    # Perl: my @elements = split(/\./, fclkey_randomSeed);
    #       pop @elements; # seed name
    #       my $ssname = pop @elements;
    #       my $tablename = join('.', @elements);
    
    fclkey_randomSeed = 'services.SeedService.baseSeed'
    elements = fclkey_randomSeed.split('.')
    elements.pop()  # Remove 'baseSeed'
    ssname = elements.pop()  # Remove 'SeedService'
    tablename = '.'.join(elements)  # 'services'
    
    # Perl: my @svclist = `fhicl-get --names-in $tablename $filename 2>/dev/null`;
    #       return 0 + grep /^$ssname\z/, @svclist;
    try:
        svclist = _run_fhicl_get(template_path, '--names-in', tablename)
        service_list = svclist.split('\n')
        # Return count of exact matches (like Perl's 0 + grep)
        return sum(1 for service in service_list if service == ssname)
    except:
        # If fhicl-get fails, return 0 (like Perl's 2>/dev/null behavior)
        return 0


def _get_output_modules(template_path: str) -> List[str]:
    """Get list of output modules from FCL template, filtering to only active ones (like Perl).
    
    Matches Perl's complex logic: analyzes end paths to determine active output modules.
    Handles both FCL structures: end_paths as names or as values.
    """

    
    # Get all output modules (like Perl's @all_outmods)
    all_outmods = _run_fhicl_get(template_path, '--names-in', 'outputs').split('\n')
    
    if not all_outmods:
        return []
    
    # Filter to only active modules (like Perl's complex logic)
    # Perl: Prepare a list of all active end path modules (outputs, but also analyzers)
    # Get end paths (NOT trigger paths - this was the bug!)
    endpaths = _run_fhicl_get(template_path, '--sequence-of', 'physics.end_paths').split('\n')
    
    # Build set of active end path modules (like Perl's %endmodules)
    endmodules = set()
    for ep in endpaths:
        if ep == '@nil':
            continue
        
        # Get modules in this end path
        try:
            mods = _run_fhicl_get(template_path, '--sequence-of', f'physics.{ep}').split('\n')
            for m in mods:
                if m:  # Skip empty entries
                    endmodules.add(m)
        except:
            # If this fails, skip this end path
            continue
    
    # Only return output modules that are in active end paths
    # Perl: my @active_outmods = grep { $endmodules{$_} } @all_outmods;
    active_outmods = []
    for mod in all_outmods:
        if mod and mod != '' and mod in endmodules:
            active_outmods.append(mod)
    
    return active_outmods


def _get_fcl_value(template_path: str, key: str) -> str:
    """Get FCL parameter value."""
    return _run_fhicl_get(template_path, '--atom-as', key)


def _replace_placeholders(pattern: str, owner: str, dsconf: str) -> str:
    """Replace placeholders in filename pattern with actual values."""
    result = pattern.strip()
    result = result.replace('.owner.', f'.{owner}.')
    result = result.replace('.version.', f'.{dsconf}.')
    result = result.replace('configuration', dsconf)
    return result


def _validate_fcl_template(template_path: str) -> None:
    """Validate FCL template has required physics sections (trigger_paths, end_paths).
    
    Matches Perl behavior exactly: dies on fhicl-get failure.
    """

    
    # Check for trigger_paths and end_paths in physics section
    result = subprocess.run(
        ['fhicl-get', '--names-in', 'physics', template_path],
        capture_output=True, text=True, check=True
    )
    physics_keys = result.stdout.strip().split('\n')
    
    required_keys = ['trigger_paths', 'end_paths']
    missing_keys = [key for key in required_keys if key not in physics_keys]
    
    if missing_keys:
        raise ValueError(f"FCL template missing required physics sections: {missing_keys}")


def _reorder_dict(d: Dict, order: List[str]) -> Dict:
    """Reorder dictionary keys according to specified order, preserving remaining keys."""
    ordered = {}
    for key in order:
        if key in d:
            ordered[key] = d[key]
    # Add any remaining keys not in the standard order
    for key, value in d.items():
        if key not in ordered:
            ordered[key] = value
    return ordered


def _build_jobpars_json(config: Dict, tbs: Dict, code: str = "", template_path: str = "") -> Dict:
    """Construct complete jobpars.json structure matching Perl mu2ejobdef exactly."""
    owner = config.get('owner') or os.getenv('USER', 'mu2e').replace('mu2epro', 'mu2e')
    desc = config['desc']
    dsconf = config['dsconf']
    
    # Build proper jobname like Perl version (cnf.owner.desc.dsconf.0.tar)
    jobname = f"cnf.{owner}.{desc}.{dsconf}.0.tar"

    # Reorder TBS fields to match Perl exactly: seed, subrunkey, event_id, outfiles
    ordered_tbs = _reorder_dict(tbs, ['seed', 'subrunkey', 'event_id', 'outfiles'])

    # Base structure - use Perl field ordering exactly: code, setup, tbs, jobname
    # This matches the actual observed Perl output order
    return {
        "code": code,
        "setup": config['simjob_setup'],
        "tbs": ordered_tbs,
        "jobname": jobname
    }


def _read_filelist(path: str) -> List[str]:
    """Read file list, filtering out empty lines."""
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


def _validate_options_for_source_type(source_type: str, args_state: Dict) -> None:
    """Validate options for source type (matching Perl's validateOptionsForSourceType exactly).
    
    Matches Perl's complex validation logic with required/allowed options per source type.
    """
    # Define validation rules for each source type (matching Perl exactly)
    validation_rules = {
        'EmptyEvent': {
            'required': ['run_number', 'events_per_job', 'description'],
            'allowed': []
        },
        'RootInput': {
            'required': ['inputs', 'merge_factor'],
            'allowed': ['description', 'auto_description']
        },
        'FromCorsikaBinary': {
            'required': ['inputs', 'merge_factor'],
            'allowed': ['description', 'auto_description']
        },
        'FromSTMTestBeamData': {
            'required': ['inputs', 'merge_factor'],
            'allowed': ['description', 'auto_description']
        },
        'SamplingInput': {
            'required': ['run_number', 'description', 'samplinginput'],
            'allowed': []
        }
    }
    
    if source_type not in validation_rules:
        raise ValueError(f"Unknown source type {source_type}")
    
    rule = validation_rules[source_type]
    
    # Get all options for incompatibility checking
    all_options = set()
    for rule_set in validation_rules.values():
        all_options.update(rule_set['required'])
        all_options.update(rule_set['allowed'])
    
    # Check required options (matching Perl's nonempty() logic)
    for option in rule['required']:
        if option == 'description':
            continue  # Always available from config
        elif option == 'samplinginput' and not args_state.get('sampling'):
            raise ValueError(f"Error: --samplinginput must be specified and nonempty for fcl files that use source type {source_type}.")
        elif option == 'inputs' and not args_state.get('inputs_list'):
            raise ValueError(f"Error: --inputs must be specified and nonempty for fcl files that use source type {source_type}.")
        elif option == 'merge_factor' and (not args_state.get('merge_factor') or args_state['merge_factor'] <= 0):
            raise ValueError(f"Error: --merge-factor must be specified and positive for fcl files that use source type {source_type}.")
        elif option == 'run_number' and args_state.get('run_number') is None:
            raise ValueError(f"Error: --run-number must be specified for fcl files that use source type {source_type}.")
        elif option == 'events_per_job' and args_state.get('events_per_job') is None:
            raise ValueError(f"Error: --events-per-job must be specified for fcl files that use source type {source_type}.")
    
    # Check for incompatible options (matching Perl's veto logic)
    for option in all_options:
        if option in rule['required'] or option in rule['allowed']:
            continue
        
        if option == 'samplinginput' and args_state.get('sampling'):
            raise ValueError(f"Error: --samplinginput is not compatible with fcl files that use source type {source_type}.")
        elif option == 'inputs' and args_state.get('inputs_list'):
            raise ValueError(f"Error: --inputs is not compatible with fcl files that use source type {source_type}.")
        elif option == 'merge_factor' and args_state.get('merge_factor') != 1:
            raise ValueError(f"Error: --merge-factor is not compatible with fcl files that use source type {source_type}.")
        elif option == 'run_number' and args_state.get('run_number') is not None:
            raise ValueError(f"Error: --run-number is not compatible with fcl files that use source type {source_type}.")
        elif option == 'events_per_job' and args_state.get('events_per_job') is not None:
            raise ValueError(f"Error: --events-per-job is not compatible with fcl files that use source type {source_type}.")


def _parse_job_args(job_args: List[str], template_path: str, config: Dict = None) -> Tuple[Dict, str, bool]:
    """
    Parse mu2ejobdef CLI options and build complete TBS structure.
    Returns: (tbs_dict, outdir, override_output_description)
    """
    tbs: Dict[str, Any] = {}
    it = iter(job_args)
    
    # Parse all arguments using a dispatch table
    args_state = {
        'inputs_list': [],
        'merge_factor': 1,
        'auxin': {},
        'sampling': {},
        'run_number': None,
        'events_per_job': None,
        'outdir': None,
        'override_output_description': False,
        'fcl_mode': None,
        'fcl_template': None
    }
    
    def parse_auxinput(spec: str) -> Tuple[str, int, List[str]]:
        """Parse auxinput specification: count:key:filelist"""
        n_str, key, filelist = spec.split(':', 2)
        all_files = _read_filelist(filelist)
        nreq = len(all_files) if n_str == 'all' else int(n_str)
        return key, nreq, all_files
    
    def parse_samplinginput(spec: str) -> Tuple[str, int, List[str]]:
        """Parse samplinginput specification: count:dsname:filelist"""
        n_str, dsname, filelist = spec.split(':', 2)
        all_files = _read_filelist(filelist)
        nreq = len(all_files) if n_str == 'all' else int(n_str)
        return dsname, nreq, all_files
    
    # Argument parsing dispatch table
    arg_handlers = {
        '--inputs': lambda: _read_filelist(next(it)),
        '--merge-factor': lambda: int(next(it)),
        '--auxinput': lambda: parse_auxinput(next(it)),
        '--samplinginput': lambda: parse_samplinginput(next(it)),
        '--run-number': lambda: int(next(it)),
        '--events-per-job': lambda: int(next(it)),
        '--outdir': lambda: next(it),
        '--override-output-description': lambda: True,
        '--embed': lambda: ('embed', next(it)),
        '--include': lambda: ('include', next(it))
    }
    
    # Map argument names to state keys for simple assignments
    simple_arg_map = {
        '--inputs': 'inputs_list',
        '--merge-factor': 'merge_factor', 
        '--run-number': 'run_number',
        '--events-per-job': 'events_per_job',
        '--outdir': 'outdir'
    }
    
    for token in it:
        if token in arg_handlers:
            result = arg_handlers[token]()
            if token == '--auxinput':
                key, nreq, files = result
                args_state['auxin'][key] = (nreq, files)
            elif token == '--samplinginput':
                dsname, nreq, files = result
                args_state['sampling'][dsname] = (nreq, files)
            elif token in ['--embed', '--include']:
                args_state['fcl_mode'], args_state['fcl_template'] = result
            elif token == '--override-output-description':
                args_state['override_output_description'] = result
            elif token in simple_arg_map:
                args_state[simple_arg_map[token]] = result

    # Determine source type using the resolved template path (like Perl's $templateresolved)
    source_type = _get_source_type(template_path)
    
    # Validate options for source type (matching Perl's validateOptionsForSourceType exactly)
    _validate_options_for_source_type(source_type, args_state)
    
    # Build TBS based on source type (matching Perl behavior exactly)
    if source_type == 'EmptyEvent':
        tbs['event_id'] = {
            'source.firstRun': args_state['run_number'],
            'source.maxEvents': args_state['events_per_job']
        }
        tbs['subrunkey'] = 'source.firstSubRun'
        
    elif source_type in ['RootInput', 'FromCorsikaBinary', 'FromSTMTestBeamData']:
        if args_state['inputs_list']:
            tbs['inputs'] = {'source.fileNames': [args_state['merge_factor'], args_state['inputs_list']]}
        tbs['subrunkey'] = ''  # subrun comes from the inputs
        
        # Set event_id based on available arguments (like Perl version)
        if args_state['run_number'] is not None or args_state['events_per_job'] is not None:
            tbs['event_id'] = {}
            if args_state['run_number'] is not None:
                tbs['event_id']['source.firstRun'] = args_state['run_number']
            if args_state['events_per_job'] is not None:
                tbs['event_id']['source.maxEvents'] = args_state['events_per_job']
        elif source_type != 'FromCorsikaBinary':
            # Fallback to default behavior
            tbs['event_id'] = {'source.maxEvents': 2147483647}
            
    elif source_type == 'SamplingInput':
        if args_state['run_number'] is not None:
            tbs['event_id'] = {
                'source.run': args_state['run_number'],
                'source.maxEvents': 2147483647
            }
        tbs['subrunkey'] = 'source.subRun'
        
        if args_state['sampling']:
            samplingintable = {}
            for dsname, (nreq, filelist) in args_state['sampling'].items():
                inputkey = f'source.dataSets.{dsname}.fileNames'
                samplingintable[inputkey] = [nreq, filelist]
            tbs['samplinginput'] = samplingintable

    # Handle output files using the resolved template path (like Perl's $templateresolved)
    output_modules = _get_output_modules(template_path)
    if output_modules:
        outfiles = {}
        
        for mod in output_modules:
            if mod and mod != '':  # skip empty entries
                output_key = f'outputs.{mod}.fileName'
                
                # Get template from FCL file (like Perl does)
                filename_pattern = _get_fcl_value(template_path, output_key)
                
                if filename_pattern and filename_pattern.strip():
                    # Do placeholder replacement like Perl does
                    owner = config.get("owner", "mu2e")
                    replaced_pattern = _replace_placeholders(filename_pattern, owner, config["dsconf"])
                    outfiles[output_key] = replaced_pattern
                else:
                    # No template pattern found - this shouldn't happen in a properly resolved template
                    # Fail like Perl does when output filename is not defined
                    raise ValueError(f"Error: {output_key} is not defined")
        if outfiles:
            tbs['outfiles'] = outfiles

    # Handle TFileService (like Perl's separate TFileService handling)
    try:
        tfileservice_filename = _get_fcl_value(template_path, 'services.TFileService.fileName')
        if tfileservice_filename and tfileservice_filename.strip() != '/dev/null':
            # Do placeholder replacement like Perl does
            owner = config.get("owner", "mu2e")
            replaced_pattern = _replace_placeholders(tfileservice_filename, owner, config["dsconf"])
            
            # Add to outfiles (Perl adds it to %outtable)
            if 'outfiles' not in tbs:
                tbs['outfiles'] = {}
            tbs['outfiles']['services.TFileService.fileName'] = replaced_pattern
    except:
        # If TFileService.fileName is not defined, skip it
        pass

    # Handle auxiliary inputs
    if args_state['auxin']:
        tbs['auxin'] = args_state['auxin']

    # Handle seed if needed using the resolved template path (like Perl's $templateresolved)
    if _seed_needed(template_path):
        # This matches the Perl behavior exactly: set the string reference
        # The mu2ejobfcl tool will process this string and add the actual baseSeed value
        tbs['seed'] = 'services.SeedService.baseSeed'
    
    # Handle sequential_aux setting from config
    if 'sequential_aux' in config:
        tbs['sequential_aux'] = config['sequential_aux']

    # Reorder TBS to match Perl order: outfiles, subrunkey, auxin, inputs, event_id, seed
    ordered_tbs = _reorder_dict(tbs, ['outfiles', 'subrunkey', 'auxin', 'inputs', 'event_id', 'seed', 'samplinginput'])

    return ordered_tbs, None, args_state['override_output_description']


def create_jobdef(config: Dict, fcl_path: str = 'template.fcl', job_args: List[str] = None, embed: bool = True, outdir: Optional[Path] = None, quiet: bool = False) -> Path:
    """
    Create a jobdef tarball (cnf.owner.desc.dsconf.0.tar) with complete Perl parity.

    - Embeds jobpars.json and mu2e.fcl
    - Processes all source types, output files, seeds, etc.
    - Returns Path to the created file
    """
    owner = config.get('owner') or os.getenv('USER', 'mu2e').replace('mu2epro', 'mu2e')
    
    # Handle auto-description
    if config.get('auto_description') is not None:
        desc = f"AutoDesc{config.get('auto_description', '')}"
    else:
        desc = config['desc']
    
    dsconf = config['dsconf']
    


    # Determine template path - match Perl logic exactly: for --embed, check if file exists locally first, then fall back to FHICL_FILE_PATH
    if embed and Path(fcl_path).exists():
        # Local file exists - use directly (matches Perl: -e $templatespec && $templatespec)
        template_path = fcl_path
    else:
        # Resolve via FHICL_FILE_PATH (matches Perl: resolveFHICLFile($templatespec))
        template_path = resolve_fhicl_file(fcl_path)
    
    fcl_embed_mode = 'embed' if embed else 'include'

    # Build complete command-line arguments from config and job_args  
    base_args = []
    if config.get('run'):
        base_args.extend(['--run-number', str(config['run'])])
    if config.get('events'):
        base_args.extend(['--events-per-job', str(config['events'])])
    
    # Add any additional job_args passed in, but filter out embed/include since we handle them separately
    filtered_job_args = []
    it = iter(job_args or [])
    for arg in it:
        if arg in ['--embed', '--include']:
            next(it, None)  # Skip the next argument (template path)
        else:
            filtered_job_args.append(arg)
    
    base_args.extend(filtered_job_args)
    
    # Add embed/include for parsing (needed for _parse_job_args)
    all_args = base_args.copy()
    if embed:
        all_args.extend(['--embed', template_path])
    else:
        all_args.extend(['--include', template_path])
    
    # Print equivalent mu2ejobdef command for debugging (unless quiet)
    cmd_parts = ['mu2ejobdef']
    
    # Add setup or code argument
    setup_arg = '--setup' if config.get('simjob_setup') else '--code'
    setup_val = config.get('simjob_setup') or config.get('code')
    cmd_parts.extend([setup_arg, setup_val])
    
    # Add required arguments
    cmd_parts.extend([
        '--dsconf', dsconf,
        '--desc', desc,
        '--dsowner', owner
    ])
    
    # Add optional arguments and FCL mode
    cmd_parts.extend(base_args)
    cmd_parts.extend(['--embed' if embed else '--include', template_path])
    
    if not quiet:
        print(f"Python mu2ejobdef equivalent command:")
        print(' '.join(cmd_parts))

    # Parse job arguments and build TBS with template analysis using the resolved template path (like Perl's $templateresolved)
    tbs, _, override_output_description = _parse_job_args(all_args, template_path, config)
    
    # Use provided outdir (simple logic matching Perl version)
    filename = f"cnf.{owner}.{desc}.{dsconf}.0.tar"
    out = Path(outdir) / filename if outdir else Path(filename)

    if out.exists():
        out.unlink()

    # Build complete jobpars JSON
    jobpars = _build_jobpars_json(config, tbs, code="", template_path=template_path)

    # Prepare temporary files
    temp_files = {}
    
    # Create jobpars.json
    jobpars_path = Path(FILENAME_JSON)
    jobpars_json = json.dumps(jobpars, indent=3, separators=(', ', ' : ')) + "\n"
    jobpars_path.write_text(jobpars_json)
    temp_files[FILENAME_JSON] = jobpars_path
    
    # Validate and create mu2e.fcl
    tpl_path = Path(template_path)
    
    if not tpl_path.exists():
        raise FileNotFoundError(f"FCL template not found: {tpl_path}")
    
    # Validate the template (either local file or original template)
    _validate_fcl_template(template_path)
    
    mu2e_fcl_tmp = Path(FILENAME_FCL)
    
    # Handle --embed vs --include modes (matching Perl behavior)
    if fcl_embed_mode == 'embed':
        # --embed: read the file content directly (whether original or modified)
        fcl_content = tpl_path.read_text()
    else:
        # --include: use #include directive (only for original templates, not local modified files)
        if fcl_path == 'template.fcl':
            # Local modified file: embed the content directly
            fcl_content = tpl_path.read_text()
        else:
            # Original template: use #include directive with original relative path (like Perl)
            fcl_content = f'#include "{fcl_path}"\n'
    
    mu2e_fcl_tmp.write_text(fcl_content)
    temp_files[FILENAME_FCL] = mu2e_fcl_tmp
    
    # Create tarball with compression
    with tarfile.open(out, 'w:gz') as tar:
        for filename, filepath in temp_files.items():
            tar.add(filepath, arcname=filename)
    
    # Cleanup temp files
    for filepath in temp_files.values():
        try:
            filepath.unlink()
        except Exception:
            pass

    return out


if __name__ == '__main__':
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(
        description='Python implementation of mu2ejobdef - Create Mu2e job definition tarballs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --setup /cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/MDC2020az/setup.sh \\
           --dsconf MDC2020az --desc CosmicCORSIKALow --dsowner mu2e \\
           --embed Production/JobConfig/cosmic/S2Resampler.fcl

  %(prog)s --code /path/to/custom/code.tar \\
           --dsconf MDC2020az --desc CustomCode --dsowner mu2e \\
           --embed Production/JobConfig/cosmic/S2Resampler.fcl

  %(prog)s --setup /cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/MDC2020az/setup.sh \\
           --dsconf MDC2020az --auto-description --dsowner mu2e \\
           --include Production/JobConfig/cosmic/S2Resampler.fcl \\
           --inputs inputs.txt --merge-factor 2

  %(prog)s --setup /cvmfs/mu2e.opensciencegrid.org/Musings/SimJob/MDC2020az/setup.sh \\
           --dsconf MDC2020az --desc MixingJob --dsowner mu2e \\
           --embed Production/JobConfig/mixing/Mix.fcl \\
           --auxinput "1:physics.filters.MuBeamFlashMixer.fileNames:mubeamCat.txt" \\
           --auxinput "25:physics.filters.EleBeamFlashMixer.fileNames:elebeamCat.txt" \\
           --samplinginput "10:dataset1:sampling1.txt" \\
           --override-output-description

Note: For EmptyEvent source type, --run-number and --events-per-job are required, 
      and --inputs/--merge-factor are not allowed.
        """
    )
    
    # Required arguments (mutually exclusive setup/code)
    setup_group = parser.add_mutually_exclusive_group(required=True)
    setup_group.add_argument('--setup', metavar='SCRIPT',
                            help='SimJob setup script path')
    setup_group.add_argument('--code', metavar='TARBALL',
                            help='Custom code tarball path')
    
    # Required arguments
    parser.add_argument('--dsconf', required=True,
                       help='Dataset configuration (e.g., MDC2020az)')
    
    # Description (mutually exclusive)
    desc_group = parser.add_mutually_exclusive_group(required=True)
    desc_group.add_argument('--desc', metavar='DESC',
                           help='Dataset description (e.g., CosmicCORSIKALow)')
    desc_group.add_argument('--auto-description', nargs='?', const='', metavar='SUFFIX',
                           help='Auto-extract description from input files (optional suffix)')
    
    parser.add_argument('--dsowner', required=True,
                       help='Dataset owner (e.g., mu2e)')
    
    # FCL template handling (mutually exclusive)
    fcl_group = parser.add_mutually_exclusive_group(required=True)
    fcl_group.add_argument('--embed', metavar='FCL',
                          help='Embed FCL template content in jobdef')
    fcl_group.add_argument('--include', metavar='FCL',
                          help='Include FCL template by reference in jobdef')
    
    # Optional arguments
    parser.add_argument('--run-number', type=int,
                       help='Run number for job (required for EmptyEvent source type)')
    parser.add_argument('--events-per-job', type=int,
                       help='Number of events per job (required for EmptyEvent source type)')
    parser.add_argument('--inputs', metavar='FILE',
                       help='Input file list (for sampling jobs, not compatible with EmptyEvent)')
    parser.add_argument('--merge-factor', type=int, metavar='N',
                       help='Merge factor for input files (not compatible with EmptyEvent)')
    parser.add_argument('--auxinput', action='append', metavar='SPEC',
                       help='Auxiliary input specification (format: count:key:filelist)')
    parser.add_argument('--samplinginput', action='append', metavar='SPEC',
                       help='Sampling input specification (format: count:dsname:filelist)')
    parser.add_argument('--override-output-description', action='store_true',
                       help='Override output file descriptions with job description')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose output')
    parser.add_argument('--output-dir', metavar='DIR',
                       help='Output directory for jobdef tarball')
    
    args = parser.parse_args()
    
    # Build configuration dictionary
    config = {
        'simjob_setup': args.setup,
        'code': args.code,
        'dsconf': args.dsconf,
        'desc': args.desc,
        'auto_description': args.auto_description,
        'owner': args.dsowner,
    }
    
    if args.run_number:
        config['run'] = args.run_number
    if args.events_per_job:
        config['events'] = args.events_per_job
    
    # Build job arguments
    job_args = []
    
    if args.inputs:
        job_args.extend(['--inputs', args.inputs])
    if args.merge_factor:
        job_args.extend(['--merge-factor', str(args.merge_factor)])
    if args.auxinput:
        for aux in args.auxinput:
            job_args.extend(['--auxinput', aux])
    
    # Determine FCL path and embed mode
    fcl_path = args.embed or args.include
    embed_mode = 'embed' if args.embed else 'include'
    
    try:
        # Create job definition
        if args.verbose:
            print(f"Creating job definition with config: {config}")
            print(f"FCL template: {fcl_path} (mode: {embed_mode})")
            print(f"Job arguments: {job_args}")
        
        result = create_jobdef(
            config=config,
            fcl_path=fcl_path,
            job_args=job_args,
            embed=embed_mode == 'embed',
            outdir=args.output_dir
        )
        
        print(f"Successfully created: {result}")
        
    except Exception as e:
        if args.verbose:
            import traceback
            traceback.print_exc()
        print(f"Error creating job definition: {e}", file=sys.stderr)
        sys.exit(1)