#!/usr/bin/env python3
"""
Shared base classes and utilities for Mu2e production tools.

This module consolidates common functionality that was previously duplicated
across multiple files to reduce code redundancy and ensure consistency.
"""

import json
import tarfile
import hashlib
from typing import Dict


class Mu2eFilename:
    """Parse and manipulate Mu2e filenames.
    
    Consolidated implementation from jobfcl.py and jobiodetail.py.
    Handles the standard Mu2e filename format:
    tier.owner.description.dsconf.sequencer.extension
    Example: dts.mu2e.CosmicCRYExtracted.MDC2020av.001205_00000000.art
    """
    
    def __init__(self, filename: str):
        self.filename = filename
        self._parse()
    
    def _parse(self):
        """Parse filename into components."""
        # Format: tier.owner.description.dsconf.sequencer.extension
        # Example: dts.mu2e.CosmicCRYExtracted.MDC2020av.001205_00000000.art
        parts = self.filename.split('.')
        if len(parts) >= 6:
            self.tier = parts[0]
            self.owner = parts[1]
            self.description = parts[2]
            self.dsconf = parts[3]
            self.sequencer = parts[4]
            self.extension = parts[5] if len(parts) > 5 else ''
        else:
            # Fallback for simpler filenames
            self.tier = parts[0] if len(parts) > 0 else ''
            self.owner = parts[1] if len(parts) > 1 else ''
            self.description = parts[2] if len(parts) > 2 else ''
            self.dsconf = parts[3] if len(parts) > 3 else ''
            self.sequencer = parts[4] if len(parts) > 4 else ''
            self.extension = parts[5] if len(parts) > 5 else ''
    
    def basename(self) -> str:
        """Return the basename of the file."""
        return self.filename
    
    def dataset(self) -> str:
        """Return the dataset name."""
        return f"{self.owner}.{self.description}.{self.dsconf}"
    
    def dsname(self) -> str:
        """Return the dataset name (alias for dataset)."""
        return self.dataset()


class Mu2eJobBase:
    """Base class for Mu2e job handling classes.
    
    Provides common functionality for extracting data from job definition
    tarballs and generating deterministic random numbers.
    """
    
    def __init__(self, jobdef_path: str):
        """Initialize with path to job definition tarball."""
        self.jobdef = jobdef_path
    
    def _extract_json(self) -> dict:
        """Extract jobpars.json from tar file.
        
        Consolidated implementation from jobfcl.py, jobiodetail.py, and jobquery.py.
        """
        with tarfile.open(self.jobdef, 'r') as tar:
            # Find jobpars.json member
            json_member = None
            for member in tar.getmembers():
                if member.name.endswith('jobpars.json'):
                    json_member = member
                    break
            
            if not json_member:
                raise ValueError(f"jobpars.json not found in {self.jobdef}")
            
            # Extract and return JSON content
            json_file = tar.extractfile(json_member)
            return json.load(json_file)
    
    def _my_random(self, *args) -> int:
        """Generate deterministic random number from inputs.
        
        Consolidated implementation from jobfcl.py and jobiodetail.py.
        Uses SHA256 hash to create deterministic pseudo-random numbers.
        """
        h = hashlib.sha256()
        for arg in args:
            h.update(str(arg).encode())
        # Take first 8 hex digits (32 bits)
        return int(h.hexdigest()[:8], 16)


def get_owner(config_or_dict: dict) -> str:
    """Extract owner from config or environment, with mu2epro -> mu2e replacement.
    
    Can be used with either a config dict (from jobdef) or a JSON data dict (from jobfcl).
    """
    import os
    return config_or_dict.get('owner') or os.getenv('USER', 'mu2e').replace('mu2epro', 'mu2e')


def setup_script_path():
    """Standard setup for direct script execution.
    
    Allows running Python files directly by making the package root importable.
    Consolidates the 4-line boilerplate used in multiple script files.
    """
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
