#!/usr/bin/env python3
"""
Python port of mu2ejobiodetail Perl script.
Provides information about input and output files of Mu2e jobs.
"""

import argparse
import json
import os
import sys
import tarfile
from pathlib import Path
from typing import Dict, List, Optional, Union

from .job_common import Mu2eFilename, Mu2eJobBase

class Mu2eJobIO(Mu2eJobBase):
    """Python port of mu2ejobiodetail functionality."""
    
    def __init__(self, jobdef: str):
        """Initialize with job definition file."""
        super().__init__(jobdef)
        self.json_data = self._extract_json()
    
    
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
        for dataset, (nreq, infiles) in auxin.items():
            # Zero means take all files
            if nreq == 0:
                nreq = len(infiles)
            
            # Draw nreq "random" files without repetitions
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
            fn = Mu2eFilename(template)
            # Update sequencer in the filename
            parts = fn.filename.split('.')
            if len(parts) >= 5:
                parts[4] = seq
            fn.filename = '.'.join(parts)
            result[key] = fn.basename()
        
        return result
    
    def jobname(self) -> str:
        """Get job name."""
        jobname = self.json_data.get('jobname')
        if not jobname:
            raise ValueError(f"Error: no jobname in {self.jobdef}")
        return jobname

def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Python port of mu2ejobiodetail - provides information about input and output files of Mu2e jobs'
    )
    parser.add_argument('--jobdef', required=True, help='Job definition file (cnf.tar)')
    parser.add_argument('--index', type=int, help='Job index')
    parser.add_argument('--inputs', action='store_true', help='Print input file basenames')
    parser.add_argument('--outputs', action='store_true', help='Print output file basenames')
    parser.add_argument('--logfile', action='store_true', help='Print log file basename')

    
    args = parser.parse_args()
    
    # Check that exactly one mode is specified
    modes = [args.inputs, args.outputs, args.logfile]
    if sum(modes) != 1:
        print("Error: exactly one mode (--inputs, --outputs, or --logfile) must be specified")
        sys.exit(1)
    
    # Check that index is provided when needed
    if args.logfile and args.index is None:
        print("Error: --index is required for --logfile mode")
        sys.exit(1)
    
    try:
        job_io = Mu2eJobIO(args.jobdef)
        
        if args.inputs:
            inputs = job_io.job_inputs(args.index)
            # Flatten the dictionary values into a single list
            all_files = []
            for file_list in inputs.values():
                all_files.extend(file_list)
            all_files.sort()
            for filename in all_files:
                print(filename)
        
        elif args.outputs:
            outputs = job_io.job_outputs(args.index)
            # Sort output filenames
            output_files = sorted(outputs.values())
            for filename in output_files:
                print(filename)
        
        elif args.logfile:
            jobname = job_io.jobname()
            fn = Mu2eFilename(jobname)
            # Create log filename
            parts = jobname.split('.')
            if len(parts) >= 4:
                # Format: log.owner.description.dsconf.sequencer.log
                log_filename = f"log.{parts[1]}.{parts[2]}.{parts[3]}.{job_io.sequencer(args.index)}.log"
                print(log_filename)
            else:
                print(f"log.{jobname}.{job_io.sequencer(args.index)}.log")
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
