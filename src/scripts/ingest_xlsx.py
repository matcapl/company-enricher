#!/usr/bin/env python3
"""
Script to convert Excel files to CSV for processing.
"""

import sys
from pathlib import Path
import polars as pl
from rich.console import Console

console = Console()


def main():
    if len(sys.argv) != 2:
        console.print("[red]Usage: python ingest_xlsx.py <input.xlsx>")
        console.print("[yellow]Converts Excel file to CSV for processing by the enricher")
        sys.exit(1)
    
    input_file = Path(sys.argv[1])
    
    if not input_file.exists():
        console.print(f"[red]Error: File '{input_file}' not found")
        sys.exit(1)
    
    if input_file.suffix.lower() != '.xlsx':
        console.print(f"[red]Error: Expected .xlsx file, got {input_file.suffix}")
        sys.exit(1)
    
    # Output to data/input/companies.csv
    output_dir = Path("data/input")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "companies.csv"
    
    try:
        console.print(f"[blue]Loading {input_file}...")
        df = pl.read_excel(str(input_file))
        
        console.print(f"[blue]Loaded {len(df)} rows, {len(df.columns)} columns")
        
        # Clean column names (remove special characters, spaces)
        new_columns = []
        for col in df.columns:
            # Replace problematic characters
            clean_col = col.replace(".", "_").replace(" ", "_").replace("-", "_")
            clean_col = "".join(c for c in clean_col if c.isalnum() or c == "_")
            new_columns.append(clean_col)
        
        df = df.rename(dict(zip(df.columns, new_columns)))
        
        # Save as CSV
        console.print(f"[blue]Saving to {output_file}...")
        df.write_csv(str(output_file))
        
        console.print(f"[green]✅ Conversion complete!")
        console.print(f"[green]📄 Output: {output_file}")
        console.print(f"[green]📊 Rows: {len(df)}, Columns: {len(df.columns)}")
        
        # Show column names
        console.print(f"\n[cyan]Columns:")
        for i, col in enumerate(df.columns, 1):
            console.print(f"  {i:2d}. {col}")
        
    except Exception as e:
        console.print(f"[red]Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
