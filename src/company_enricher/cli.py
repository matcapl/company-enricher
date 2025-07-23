"""Command-line interface for the company enricher."""

import asyncio
from pathlib import Path
from typing import Optional

import typer
import polars as pl
from rich.console import Console
from rich.table import Table

from .config import settings
from .logging_config import setup_logging, get_logger
from .pipeline.enricher import enrich_dataframe
from .cache import cache_stats, clear_cache

# Initialize CLI app
app = typer.Typer(
    name="company-enricher",
    help="Async pipeline to enrich UK companies with websites, headcount & geo data",
    add_completion=False,
)
console = Console()
logger = get_logger(__name__)


@app.callback()
def main(
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        help="Set logging level",
        case_sensitive=False,
    ),
    cache_dir: str = typer.Option(
        settings.cache_dir,
        "--cache-dir",
        help="Directory for disk cache",
    ),
) -> None:
    """Company Enricher CLI - Enrich UK company data with external sources."""
    # Update settings
    settings.log_level = log_level.upper()
    settings.cache_dir = cache_dir
    
    # Setup logging
    setup_logging()
    
    # Validate required settings
    if not settings.companies_house_key:
        console.print("[red]Error: COMPANIES_HOUSE_KEY environment variable is required")
        console.print("Get your free API key from: https://developer.company-information.service.gov.uk/")
        raise typer.Exit(1)


@app.command()
def enrich(
    input_file: str = typer.Argument(..., help="Input CSV file with company data"),
    output: str = typer.Option(
        "enriched.csv",
        "--out", "-o",
        help="Output file path for enriched data",
    ),
    concurrency: int = typer.Option(
        settings.max_concurrency,
        "--concurrency", "-c",
        help="Maximum concurrent requests",
        min=1,
        max=50,
    ),
    checkpoint: int = typer.Option(
        500,
        "--checkpoint",
        help="Save checkpoint every N companies",
        min=10,
    ),
    resume: bool = typer.Option(
        False,
        "--resume",
        help="Resume from existing output file",
    ),
) -> None:
    """
    Enrich company data from CSV file.
    
    The input CSV should contain columns: CompanyName, CompanyNumber, etc.
    Additional columns will be added: company_url, description, employees_*, manufacturing_location
    """
    # Validate input file
    input_path = Path(input_file)
    if not input_path.exists():
        console.print(f"[red]Error: Input file '{input_file}' not found")
        raise typer.Exit(1)
    
    # Load input data
    try:
        if input_path.suffix.lower() == '.xlsx':
            df = pl.read_excel(str(input_path))
        elif input_path.suffix.lower() == '.csv':
            df = pl.read_csv(str(input_path))
        else:
            console.print(f"[red]Error: Unsupported file format '{input_path.suffix}'")
            console.print("Supported formats: .csv, .xlsx")
            raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error loading input file: {e}")
        raise typer.Exit(1)
    
    # Validate required columns
    required_columns = {"CompanyName", "CompanyNumber"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        console.print(f"[red]Error: Missing required columns: {missing_columns}")
        raise typer.Exit(1)
    
    # Handle resume functionality
    start_row = 0
    if resume and Path(output).exists():
        try:
            existing_df = pl.read_csv(output)
            start_row = len(existing_df)
            console.print(f"[yellow]Resuming from row {start_row}")
            df = df.slice(start_row)
        except Exception as e:
            console.print(f"[yellow]Warning: Could not load existing output for resume: {e}")
    
    # Display input summary
    table = Table(title="Input Data Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Total companies", str(len(df)))
    table.add_row("Input file", str(input_path))
    table.add_row("Output file", output)
    table.add_row("Concurrency", str(concurrency))
    table.add_row("Checkpoint interval", str(checkpoint))
    
    console.print(table)
    
    # Confirm before proceeding
    if not typer.confirm("\nProceed with enrichment?"):
        console.print("Cancelled.")
        raise typer.Exit(0)
    
    # Run enrichment
    try:
        final_df = asyncio.run(
            enrich_dataframe(df, output, concurrency, checkpoint)
        )
        
        # Display summary
        console.print("\n[green]✅ Enrichment completed successfully!")
        console.print(f"[green]📄 Results saved to: {output}")
        console.print(f"[green]📊 Total companies processed: {len(final_df)}")
        
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️  Enrichment interrupted by user")
        console.print(f"[yellow]📄 Partial results may be saved in: {output}")
    except Exception as e:
        console.print(f"\n[red]❌ Enrichment failed: {e}")
        logger.exception("Enrichment failed")
        raise typer.Exit(1)


@app.command()
def info(
    input_file: str = typer.Argument(..., help="Input file to analyze"),
) -> None:
    """Display information about input file."""
    input_path = Path(input_file)
    
    if not input_path.exists():
        console.print(f"[red]Error: File '{input_file}' not found")
        raise typer.Exit(1)
    
    try:
        # Load and analyze file
        if input_path.suffix.lower() == '.xlsx':
            df = pl.read_excel(str(input_path))
        elif input_path.suffix.lower() == '.csv':
            df = pl.read_csv(str(input_path))
        else:
            console.print(f"[red]Error: Unsupported file format '{input_path.suffix}'")
            raise typer.Exit(1)
        
        # Display file info
        table = Table(title=f"File Analysis: {input_path.name}")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("File size", f"{input_path.stat().st_size / 1024:.1f} KB")
        table.add_row("Rows", str(len(df)))
        table.add_row("Columns", str(len(df.columns)))
        
        console.print(table)
        
        # Display column info
        columns_table = Table(title="Columns")
        columns_table.add_column("Name", style="cyan")
        columns_table.add_column("Type", style="yellow")
        columns_table.add_column("Non-null", style="green")
        
        for col in df.columns:
            dtype = str(df[col].dtype)
            non_null_count = df[col].drop_nulls().len()
            columns_table.add_row(col, dtype, f"{non_null_count}/{len(df)}")
        
        console.print(columns_table)
        
        # Check for required columns
        required_columns = {"CompanyName", "CompanyNumber"}
        missing_columns = required_columns - set(df.columns)
        
        if missing_columns:
            console.print(f"\n[red]⚠️  Missing required columns: {missing_columns}")
        else:
            console.print(f"\n[green]✅ All required columns present")
        
        # Show sample data
        if len(df) > 0:
            console.print(f"\n[cyan]Sample data (first 3 rows):")
            sample_df = df.head(3)
            console.print(sample_df)
        
    except Exception as e:
        console.print(f"[red]Error analyzing file: {e}")
        raise typer.Exit(1)


@app.command()
def cache(
    action: str = typer.Argument(..., help="Cache action: 'stats', 'clear'"),
) -> None:
    """Manage the application cache."""
    if action == "stats":
        stats = cache_stats()
        
        table = Table(title="Cache Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Cache entries", str(stats["size"]))
        table.add_row("Cache volume", f"{stats['volume'] / 1024 / 1024:.1f} MB")
        table.add_row("Cache directory", settings.cache_dir)
        
        console.print(table)
        
    elif action == "clear":
        if typer.confirm("Are you sure you want to clear the cache?"):
            clear_cache()
            console.print("[green]✅ Cache cleared successfully")
        else:
            console.print("Cancelled.")
    else:
        console.print(f"[red]Error: Unknown cache action '{action}'")
        console.print("Available actions: stats, clear")
        raise typer.Exit(1)


@app.command()
def config() -> None:
    """Display current configuration."""
    table = Table(title="Configuration")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="green")
    table.add_column("Source", style="yellow")
    
    # Show key settings (mask sensitive values)
    table.add_row(
        "Companies House API Key",
        "***" + settings.companies_house_key[-4:] if settings.companies_house_key else "[red]Not set",
        "Environment"
    )
    
    table.add_row(
        "OpenCage API Key",
        "***" + settings.opencage_key[-4:] if settings.opencage_key else "[yellow]Not set (optional)",
        "Environment"
    )
    
    table.add_row("Max Concurrency", str(settings.max_concurrency), "Config")
    table.add_row("DuckDuckGo Rate Limit", f"{settings.ddg_max_qps:.1f} QPS", "Config")
    table.add_row("Cache Directory", settings.cache_dir, "Config")
    table.add_row("Cache TTL", f"{settings.cache_ttl_days} days", "Config")
    table.add_row("HTTP Timeout", f"{settings.http_timeout}s", "Config")
    table.add_row("Log Level", settings.log_level, "Config")
    
    console.print(table)


if __name__ == "__main__":
    app()
