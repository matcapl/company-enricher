#!/usr/bin/env python3
"""
Convenience script to run enrichment with common settings.
"""

import sys
from pathlib import Path
from company_enricher.cli import app

def main():
    # Default arguments for common use case
    default_args = [
        "enrich",
        "data/input/companies.csv",
        "--out", "data/output/enriched.csv",
        "--concurrency", "10",
        "--checkpoint", "500"
    ]
    
    # Use command line args if provided, otherwise use defaults
    if len(sys.argv) > 1:
        app()
    else:
        print("Running with default settings...")
        print(f"Command: company-enricher {' '.join(default_args)}")
        sys.argv.extend(default_args)
        app()

if __name__ == "__main__":
    main()
