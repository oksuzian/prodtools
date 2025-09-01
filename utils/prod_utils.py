import subprocess
import sys
import logging
import json
import os
from pathlib import Path
from .jobfcl import Mu2eJobFCL

def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="[%(levelname)s] %(message)s"
    )
    
    # Suppress debug messages from external libraries when verbose is enabled
    if verbose:
        # Suppress requests library debug messages
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        # Suppress samweb_client debug messages
        logging.getLogger("samweb_client").setLevel(logging.WARNING)

def run(cmd, capture=False, shell=False):
    """
    Run a shell command. If capture=True, return stdout. If shell=True, cmd is a string.
    """
    print(f"Running: {cmd}")
    try:
        # Always capture output to see errors, even when not in capture mode
        res = subprocess.run(cmd, shell=shell, capture_output=True, text=True, check=True)
        if not capture:
            if res.stdout:
                print(f"STDOUT: {res.stdout}")
        return res.stdout.strip() if capture else None
    except subprocess.CalledProcessError as e:
        print(f"=== COMMAND FAILED ===")
        print(f"Command: {cmd}")
        print(f"Exit code: {e.returncode}")
        print(f"Working directory: {os.getcwd()}")
        print(f"STDOUT length: {len(e.stdout) if e.stdout else 0}")
        print(f"STDERR length: {len(e.stderr) if e.stderr else 0}")
        if e.stdout:
            print(f"STDOUT: {e.stdout}")
        if e.stderr:
            print(f"STDERR: {e.stderr}")
        print(f"=== END ERROR REPORT ===")
        # Don't exit - let the error propagate naturally to show full traceback
        raise




def write_fcl(jobdef, inloc='tape', proto='root', index=0, target=None):
    """
    Generate and write an FCL file using mu2ejobfcl.
    """
    # Extract fcl filename from jobdef and write to current directory
    jobdef_name = Path(jobdef).name  # Get just the filename, not the full path
    fcl = jobdef_name.replace('.0.tar', f'.{index}.fcl')  # cnf.mu2e.RPCInternalPhysical.MDC2020az.{index}.fcl
    
    # Print Perl equivalent command
    perl_cmd = f"mu2ejobfcl --jobdef {jobdef} --default-location {inloc} --default-protocol {proto}"
    if target:
        perl_cmd += f" --target {target}"
    else:
        perl_cmd += f" --index {index}"
    perl_cmd += f" > {fcl}"
    print(f"Running Perl equivalent of:")
    print(f"{perl_cmd}")
    
    # Use Python mu2ejobfcl implementation
    try:
        job_fcl = Mu2eJobFCL(jobdef, inloc=inloc, proto=proto)
        
        # Find job index
        if target:
            job_index = job_fcl.find_index(target=target)
        else:
            job_index = job_fcl.find_index(index=index)
        
        # Generate FCL content
        result = job_fcl.generate_fcl(job_index)
        
        print(f"Wrote {fcl}")
        with open(fcl, 'w') as f:
            f.write(result + '\n')
        
        # Print the FCL content
        print(f"\n--- {fcl} content ---")
        print(result + '\n')

        return fcl
    
    except Exception as e:
        print(f"Error generating FCL: {e}")
        raise

def get_def_counts(dataset, include_empty=False):
    """Get file count and event count for a dataset."""
    from .samweb_wrapper import count_files, list_files
    
    # Count files
    query = f"defname: {dataset}" if include_empty else f"defname: {dataset} and event_count>0"
    nfiles = count_files(query)
    
    # Count events
    result = list_files(f"dh.dataset={dataset}", summary=True)
    nevts = 0
    if isinstance(result, dict):
        nevts = result.get('total_event_count', 0) or 0
    elif isinstance(result, list):
        # Handle list result (when summary=False)
        nevts = len(result)  # Fallback to file count
    else:
        # Handle string result (fallback)
        for line in result.splitlines():
            parts = line.split()
            if len(parts) >= 3 and parts[0] == "Event":
                nevts = int(parts[2])
                break
    
    if nfiles == 0:
        sys.exit(f"No files found in dataset {dataset}")
    return nfiles, nevts

def calculate_merge_factor(fields):
    """Calculate merge factor based on dataset counts and requested merge_events.
    
    This matches the Perl logic: MERGE_FACTOR = MERGE_EVENTS/npevents + 1
    where npevents = nevts/nfiles (events per file)
    """
    nfiles, nevts = get_def_counts(fields['input_data'])
    if nfiles == 0:
        return 1
    
    # Calculate events per file
    npevents = nevts // nfiles
    
    # Calculate merge factor: MERGE_EVENTS/npevents + 1
    # This ensures we get enough files to cover the requested merge_events
    merge_factor = fields['merge_events'] // npevents + 1
    
    return merge_factor

# Removed duplicate find_json_entry; use json2jobdef.load_json + json2jobdef.find_json_entry

def write_fcl_template(base, overrides):
    """
    Write FCL template file with just an include directive and overrides.
    
    Args:
        base: Base FCL file to include
        overrides: Dictionary of FCL overrides
    """
    with open('template.fcl', 'w') as f:
        # Write just the include directive for the base FCL
        f.write(f'#include "{base}"\n')
        
        # Add overrides
        for key, val in overrides.items():
            if key == '#include':
                includes = val if isinstance(val, list) else [val]
                for inc in includes:
                    f.write(f'#include "{inc}"\n')
            else:
                f.write(f'{key}: {json.dumps(val) if isinstance(val, str) else val}\n')

def parse_jobdef_fields(jobdefs_file, index=None):
    """
    Extract job definition fields from a jobdefs file and index.
    
    Args:
        jobdefs_file: Path to the jobdefs file
        index: Index of the job definition to extract (optional, will extract from fname env var if not provided)
        
    Returns:
        tuple: (tarfile, job_index, inloc, outloc)
    """

    # Optional debug noise, enable with environment variable PRODTOOLS_DEBUG=1
    debug = os.getenv("PRODTOOLS_DEBUG") == "1"
    if debug:
        try:
            run("httokendecode -H", shell=True)
        except SystemExit:
            print("Warning: Token validation failed. Please check your token.")
        run("pwd", shell=True)
        run("ls -ltr", shell=True)

    # Extract index from fname environment variable if not provided
    if index is None:
        fname = os.getenv("fname")
        if not fname:
            print("Error: fname environment variable is not set.")
            sys.exit(1)
        try:
            index = int(fname.split('.')[4].lstrip('0') or '0')
        except (IndexError, ValueError) as e:
            print("Error: Unable to extract index from filename.")
            sys.exit(1)

    if not os.path.exists(jobdefs_file):
        print(f"Error: Jobdefs file {jobdefs_file} does not exist.")
        sys.exit(1)
    
    jobdefs_list = make_jobdefs_list(Path(jobdefs_file))
    
    if index < 0 or index >= len(jobdefs_list):
        print(f"Error: Index {index} out of range; {len(jobdefs_list)} definitions available.")
        sys.exit(1)
    
    # Get the index-th job definition (adjusting for Python's 0-index).
    jobdef = jobdefs_list[index]
    if debug:
        print(f"The {index}th job definition is: {jobdef}")

    # Split the job definition into fields (parfile job_index inloc outloc).
    fields = jobdef.split()
    if len(fields) != 4:
        print(f"Error: Expected 4 fields (parfile job_index inloc outloc) in the job definition, but got: {jobdef}")
        sys.exit(1)

    # Return the fields: (tarfile, job_index, inloc, outloc)
    if debug:
        print(f"IND={fields[1]} TARF={fields[0]} INLOC={fields[2]} OUTLOC={fields[3]}")
    return fields[0], int(fields[1]), fields[2], fields[3]

def make_jobdefs_list(input_file):
    """
    Create a list of individual job definitions from a jobdef map file.
    
    Args:
        input_file: Path to jobdef map file
        
    Returns:
        List of individual job definition strings: parfile job_index inloc outloc
    """
    if not input_file.exists():
        sys.exit(f"Input file not found: {input_file}")
    
    jobdefs_list = []
    for raw_line in input_file.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) != 4:
            sys.exit(f"Invalid line in {input_file}: '{raw_line}'. Expected 4 fields.")
        parfile, njobs_str, inloc, outloc = parts
        try:
            njobs = int(njobs_str)
        except ValueError:
            sys.exit(f"Invalid njobs value '{njobs_str}' in {input_file} line: '{raw_line}'")
        for i in range(njobs):
            jobdefs_list.append(f"{parfile} {i} {inloc} {outloc}")
    print(f"Generated the list of {len(jobdefs_list)} jobdefs from {input_file}")
    return jobdefs_list

def replace_file_extensions(input_str, first_field, last_field):
    """Replace the first and last fields in a dot-separated string."""
    fields = input_str.split('.')
    fields[0] = first_field
    fields[-1] = last_field
    return '.'.join(fields)

def create_index_definition(output_index_dataset, job_count, input_index_dataset):
        
    from .samweb_wrapper import delete_definition, create_definition, describe_definition
    
    idx_name = f"i{output_index_dataset}"
    idx_format = f"{job_count:07d}"
    
    # Check if definition exists before trying to delete it
    try:
        describe_definition(idx_name)
        print(f"Definition {idx_name} exists, attempting to delete...")
        delete_definition(idx_name)
        print(f"Successfully deleted {idx_name}")
    except Exception as e:
        print(f"Definition {idx_name} does not exist, skipping deletion")

    # Create the new definition
    print(f"Creating definition {idx_name}...")
    create_definition(idx_name, f"dh.dataset {input_index_dataset} and dh.sequencer < {idx_format}")
    describe_definition(idx_name)