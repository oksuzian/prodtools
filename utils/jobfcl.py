#!/usr/bin/env python3
"""
Python port of mu2ejobfcl Perl script.
Generates FCL configuration files for Mu2e jobs.
"""

import argparse
import json
import os
import sys
import tarfile
from pathlib import Path
from typing import Dict, List, Optional, Union
import hashlib
import re

# Allow running this file directly: make package root importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.job_common import Mu2eFilename, Mu2eJobBase

class Mu2eJobFCL(Mu2eJobBase):
    """Python port of mu2ejobfcl functionality."""
    
    def __init__(self, jobdef: str, inloc: str = 'tape', proto: str = 'file'):
        """Initialize with job definition file."""
        super().__init__(jobdef)
        self.inloc = inloc
        self.proto = proto
        self.json_data = self._extract_json()
        
        # Extract owner and dsconf directly from JSON fields
        # Use same default logic as mu2ejobdef.py for consistency
        from .job_common import get_owner
        self.owner = get_owner(self.json_data)
        self.dsconf = self.json_data.get('dsconf', 'unknown')
        
        # Read sequential_aux setting from job definition, default to False if not specified
        tbs = self.json_data.get('tbs', {})
        self.sequential_aux = tbs.get('sequential_aux', False)
    
    def _extract_fcl(self) -> str:
        """Extract mu2e.fcl from tar file."""
        with tarfile.open(self.jobdef, 'r') as tar:
            # Find mu2e.fcl member
            fcl_member = None
            for member in tar.getmembers():
                if member.name.endswith('mu2e.fcl'):
                    fcl_member = member
                    break
            
            if not fcl_member:
                raise ValueError(f"mu2e.fcl not found in {self.jobdef}")
            
            # Extract and return FCL content
            fcl_file = tar.extractfile(fcl_member)
            return fcl_file.read().decode('utf-8')
    

    
    def _locate_file(self, filename: str) -> str:
        """Locate a file using samweb and return its physical path."""
        # Check if we're using a local directory (dir: prefix)
        if self.inloc.startswith('dir:'):
            # Extract the local directory path
            local_dir = self.inloc[4:]  # Remove 'dir:' prefix
            # Remove trailing slash if present
            local_dir = local_dir.rstrip('/')
            return f"{local_dir}/{filename}"
        
        # Use SAM to locate the file
        from .samweb_wrapper import locate_file
        
        location = locate_file(filename)
        if not location:
            raise ValueError(f"Could not locate file: {filename}")
        
        # Handle dict location (extract path)
        if isinstance(location, dict):
            path = location.get('location') or location.get('path')
            if not path:
                raise ValueError(f"Could not determine location for file: {filename}")
            return path
        
        # Handle string location
        if isinstance(location, str):
            return location
        
        # Unexpected location type
        raise ValueError(f"Unexpected location type for file {filename}: {type(location)}")
    
    def _format_filename(self, filename: str) -> str:
        """Format filename according to protocol."""
        if self.proto == 'file':
            return self._locate_file(filename)
        
        if self.proto != 'root':
            return filename
        
        # For root protocol, transform to xroot://
        physical_path = self._locate_file(filename)
        
        # Clean up location format prefixes
        clean_path = physical_path
        if physical_path.startswith('enstore:'):
            clean_path = physical_path[8:]  # Remove 'enstore:'
        elif physical_path.startswith('dcache:'):
            clean_path = physical_path[7:]  # Remove 'dcache:'
        
        # Remove file location suffix like (2290@fm4794l8) if present
        clean_path = re.sub(r'\([^)]+\)$', '', clean_path)
        
        # Add filename if not already present
        if not clean_path.endswith(filename):
            clean_path = clean_path + '/' + filename
        
        # Apply xroot transformation to /pnfs/ paths
        if clean_path.startswith('/pnfs/'):
            return clean_path.replace(
                '/pnfs/', 
                'xroot://fndcadoor.fnal.gov//pnfs/fnal.gov/usr/', 
                1
            )
        
        return clean_path
    
    def job_primary_inputs(self, index: int) -> Dict[str, List[str]]:
        """Get primary input files for job index."""
        tbs = self.json_data.get('tbs', {})
        inputs = tbs.get('inputs')
        
        if not inputs:
            return {}
        
        result = {}
        # inputs is a dict with one key-value pair
        for dataset, (merge, filelist) in inputs.items():
            nf = len(filelist)
            first = index * merge
            last = min(first + merge - 1, nf - 1)
            
            if first > last:
                raise ValueError(f"job_primary_inputs(): invalid index {index}")
            
            result[dataset] = filelist[first:last + 1]
        
        return result
    
    def job_aux_inputs(self, index: int) -> Dict[str, List[str]]:
        """Get auxiliary input files for job index."""
        tbs = self.json_data.get('tbs', {})
        auxin = tbs.get('auxin')
        
        if not auxin:
            return {}
        
        result = {}
        # Process auxiliary inputs in JSON order (which should be the correct order)
        for dataset, (nreq, infiles) in auxin.items():
            # Zero means take all files
            if nreq == 0:
                nreq = len(infiles)
            
            if self.sequential_aux:
                # Sequential selection with rollover (like primary inputs)
                nf = len(infiles)
                first = index * nreq
                last = min(first + nreq - 1, nf - 1)
                
                if first >= nf:
                    # Roll over: start from the beginning
                    first = first % nf
                    last = min(first + nreq - 1, nf - 1)
                
                if first > last:
                    raise ValueError(f"job_aux_inputs(): invalid index {index} for sequential selection")
                
                result[dataset] = infiles[first:last + 1]
            else:
                # Draw nreq "random" files without repetitions (original behavior)
                sample = []
                available_files = infiles.copy()
                
                for count in range(nreq):
                    if not available_files:
                        break
                    
                    # Deterministic random selection
                    rnd = self._my_random(index, *available_files)
                    file_index = rnd % len(available_files)
                    sample.append(available_files[file_index])
                    available_files.pop(file_index)
               
                result[dataset] = sample
        
        return result
    
    def job_sampling_inputs(self, index: int) -> Dict[str, List[str]]:
        """Get sampling input files for job index."""
        tbs = self.json_data.get('tbs', {})
        samplinginput = tbs.get('samplinginput')
        
        if not samplinginput:
            return {}
        
        result = {}
        for dataset, (nreq, filelist) in samplinginput.items():
            # Zero means take all files
            if nreq == 0:
                nreq = len(filelist)
            
            # Sequential selection like primary inputs
            nf = len(filelist)
            first = index * nreq
            last = min(first + nreq - 1, nf - 1)
            
            if first > last:
                raise ValueError(f"job_sampling_inputs(): invalid index {index}")
            
            result[dataset] = filelist[first:last + 1]
        
        return result
    
    def job_inputs(self, index: int) -> Dict[str, List[str]]:
        """Get all input files for job index."""
        primary = self.job_primary_inputs(index)
        aux = self.job_aux_inputs(index)
        sampling = self.job_sampling_inputs(index)
        
        # Merge all input types
        result = {}
        result.update(primary)
        result.update(aux)
        result.update(sampling)
        
        return result
    
    def sequencer(self, index: int) -> str:
        """Get sequencer for job index."""
        primary_inputs = self.job_primary_inputs(index)
        
        if primary_inputs:
            # Get sequencers from primary input files
            sequencers = []
            for dataset, files in primary_inputs.items():
                for filename in files:
                    fn = Mu2eFilename(filename)
                    sequencers.append(fn.sequencer)
            
            # Sort and return first
            sequencers.sort()
            return sequencers[0]
        
        # Check for event_id configuration
        tbs = self.json_data.get('tbs', {})
        event_id = tbs.get('event_id')
        
        if event_id:
            run = event_id.get('source.firstRun') or event_id.get('source.run')
            if not run:
                raise ValueError("Error: get_sequencer(): can not get source.firstRun from event_id")
            subrun = index
            return f"{run:06d}_{subrun:08d}"
        
        raise ValueError("Error: get_sequencer(): unsupported JSON content")
    
    def job_outputs(self, index: int) -> Dict[str, str]:
        """Get output files for job index."""
        tbs = self.json_data.get('tbs', {})
        outfiles = tbs.get('outfiles')
        
        if not outfiles:
            return {}
        
        result = {}
        seq = self.sequencer(index)
        
        for key, template in outfiles.items():
            # The template may still contain placeholders that need to be resolved
            # Replace placeholders with actual values
            resolved_template = template.replace('.owner.', f'.{self.owner}.')
            resolved_template = resolved_template.replace('.version.', f'.{self.dsconf}.')
            resolved_template = resolved_template.replace('.sequencer.', f'.{seq}.')
            
            # Update sequencer in the filename
            fn = Mu2eFilename(resolved_template)
            parts = fn.filename.split('.')
            if len(parts) >= 5:
                parts[4] = seq
            fn.filename = '.'.join(parts)
            result[key] = fn.basename()
        
        return result
    
    def job_event_settings(self, index: int) -> Dict[str, Union[int, str]]:
        """Get event settings for job index."""
        tbs = self.json_data.get('tbs', {})
        event_id = tbs.get('event_id')
        
        if not event_id:
            return {}
        
        result = {}
        for key, value in event_id.items():
            result[key] = value
        
        subrunkey = tbs.get('subrunkey')
        if subrunkey is not None:
            if subrunkey != '':
                result[subrunkey] = index
        else:
            # Old format
            result['source.firstSubRun'] = index
        
        return result
    
    def job_seed(self, index: int) -> Dict[str, int]:
        """Get seed settings for job index."""
        tbs = self.json_data.get('tbs', {})
        seed_key = tbs.get('seed')
        
        if not seed_key:
            return {}
        
        return {seed_key: 1 + index}
    
    def njobs(self) -> int:
        """Get number of jobs."""
        tbs = self.json_data.get('tbs', {})
        inputs = tbs.get('inputs')
        
        if not inputs:
            return 0
        
        # inputs is a dict with one key-value pair
        for dataset, (merge, filelist) in inputs.items():
            nf = len(filelist)
            return (nf + merge - 1) // merge
        
        return 0
    
    def input_datasets(self) -> List[str]:
        """Get list of input datasets."""
        tbs = self.json_data.get('tbs', {})
        
        # Collect all dataset names from different input types
        datasets = set()
        datasets.update(tbs.get('inputs', {}).keys())
        datasets.update(tbs.get('auxin', {}).keys())
        datasets.update(tbs.get('samplinginput', {}).keys())
        
        return list(datasets)
    
    def index_from_sequencer(self, seq: str) -> int:
        """Get job index from sequencer."""
        nj = self.njobs()
        if nj > 0:
            # Finite job set - search exhaustively
            for i in range(nj):
                if self.sequencer(i) == seq:
                    return i
            raise ValueError(f"Outputs with the requested sequencer \"{seq}\" are not produced by this jobset.")
        else:
            # Infinite job set - parse sequencer
            # Format: run_subrun
            parts = seq.split('_')
            if len(parts) != 2:
                raise ValueError(f"Unexpected format of the sequencer \"{seq}\"")
            
            index = int(parts[1])
            # Verify
            test_seq = self.sequencer(index)
            if test_seq != seq:
                raise ValueError(f"Outputs with the requested sequencer \"{seq}\" are not generated by this jobset.")
            return index
    
    def index_from_source_file(self, srcfn: str) -> int:
        """Get job index from source file."""
        tbs = self.json_data.get('tbs', {})
        inputs = tbs.get('inputs')
        
        if inputs:
            for dataset, (merge, filelist) in inputs.items():
                try:
                    fileindex = filelist.index(srcfn)
                    return fileindex // merge
                except ValueError:
                    continue
        
        raise ValueError(f"This jobset does not use \"{srcfn}\" as a primary input file")
    
    def find_index(self, index: Optional[int] = None, target: Optional[str] = None, source: Optional[str] = None) -> int:
        """Find job index from various input methods."""
        if index is not None:
            if target is not None:
                raise ValueError("Error: --index and --target are mutually exclusive")
            if source is not None:
                raise ValueError("Error: --index and --source are mutually exclusive")
            return index
        
        if target is not None:
            if source is not None:
                raise ValueError("Error: --target and --source are mutually exclusive")
            
            fn = Mu2eFilename(target)
            seq = fn.sequencer
            job_index = self.index_from_sequencer(seq)
            
            # Check that the target file matches what this job will produce
            outputs = self.job_outputs(job_index)
            for output_file in outputs.values():
                if output_file == target:
                    return job_index
            
            raise ValueError(f"Error: file \"{target}\" is not produced by any job in this job set.")
        
        if source is not None:
            return self.index_from_source_file(source)
        
        raise ValueError("Error: one of --index, --target, or --source must be specified.")
    
    def generate_fcl(self, index: int) -> str:
        """Generate FCL configuration for job index."""
        # Get base FCL
        base_fcl = self._extract_fcl()
        
        # Generate additional configuration
        config_lines = []
        config_lines.append("#----------------------------------------------------------------")
        config_lines.append(f"# Code added by mu2ejobfcl for job index {index}:")
        
        # Event settings
        event_settings = self.job_event_settings(index)
        for key, value in event_settings.items():
            config_lines.append(f"{key}: {value}")
        
        # Input files with protocol handling
        inputs = self.job_inputs(index)
        for key, file_list in inputs.items():
            if file_list:
                config_lines.append(f"{key}: [")
                for i, filename in enumerate(file_list):
                    formatted_filename = self._format_filename(filename)
                    # Don't add comma for the last element
                    comma = "," if i < len(file_list) - 1 else ""
                    config_lines.append(f'    "{formatted_filename}"{comma}')
                config_lines.append("]")
        
        # Output files (add/replace outputs from job definition)
        outputs = self.job_outputs(index)
        for key, filename in outputs.items():
            # Always add the output from job definition (it may replace template values)
            config_lines.append(f'{key}: "{filename}"')
        
        # Seed settings
        seed_settings = self.job_seed(index)
        for key, value in seed_settings.items():
            config_lines.append(f"{key}: {value}")
        
        config_lines.append("# End code added by mu2ejobfcl:")
        config_lines.append("#----------------------------------------------------------------")
        
        # Combine base FCL with additional configuration
        return base_fcl + "\n" + "\n".join(config_lines)

def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Python port of mu2ejobfcl - generates FCL configuration files for Mu2e jobs'
    )
    parser.add_argument('--jobdef', required=True, help='Job definition file (cnf.tar)')
    parser.add_argument('--index', type=int, help='Job index')
    parser.add_argument('--target', help='Target output file name')
    parser.add_argument('--source', help='Source input file name')
    parser.add_argument('--default-location', '--default-loc', default='tape', 
                       help='Default location for input files (default: tape)')
    parser.add_argument('--default-protocol', '--default-proto', default='file',
                       help='Default protocol for input files (default: file)')
    
    args = parser.parse_args()
    
    # Validate file exists and is readable
    if not os.path.isfile(args.jobdef):
        print(f"Error: {args.jobdef} is not a file", file=sys.stderr)
        sys.exit(1)
    
    if not os.access(args.jobdef, os.R_OK):
        print(f"Error: file {args.jobdef} is not readable", file=sys.stderr)
        sys.exit(1)
    
    try:
        job_fcl = Mu2eJobFCL(args.jobdef, inloc=getattr(args, 'default_location'), proto=getattr(args, 'default_protocol'))
        
        # Find job index
        index = job_fcl.find_index(args.index, args.target, args.source)
        
        # Validate index
        if index < 0:
            print(f"Error: --index must be non-negative, got: {index}", file=sys.stderr)
            sys.exit(1)
        
        # Check if index is in range
        njobs = job_fcl.njobs()
        if njobs > 0 and index >= njobs:
            print(f"Zero based index {index} is too large for njobs = {njobs}", file=sys.stderr)
            sys.exit(1)
        
        # Generate and print FCL
        fcl_content = job_fcl.generate_fcl(index)
        print(fcl_content)
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
