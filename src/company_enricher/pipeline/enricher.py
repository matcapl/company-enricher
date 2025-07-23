"""Main enrichment pipeline orchestrator."""

import asyncio
from typing import Dict, Any, List, Optional
import httpx
import polars as pl
from rich.console import Console
from rich.progress import Progress, TaskID, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn

from ..config import settings
from ..logging_config import get_logger
from ..cache import get_cache
from ..utils.typing import CompanyRecord, EnrichmentResult
from ..fetchers import companies_house, web_search, website_scraper, geocoder
from .rate_limiter import RateLimiter
from .batch import save_checkpoint, ProgressTracker

logger = get_logger(__name__)
console = Console()


class CompanyEnricher:
    """Main enrichment pipeline for company data."""
    
    def __init__(self, concurrency: int = 10):
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.rate_limiter = RateLimiter(max_rate=settings.ddg_max_qps)
        self.http_client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.http_client = httpx.AsyncClient(
            timeout=settings.http_timeout,
            limits=httpx.Limits(
                max_keepalive_connections=20,
                max_connections=100
            ),
            http2=True
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.http_client:
            await self.http_client.aclose()
    
    async def enrich_single_company(self, company: CompanyRecord) -> EnrichmentResult:
        """
        Enrich a single company record.
        
        Args:
            company: Input company record
            
        Returns:
            Enrichment result with additional data
        """
        async with self.semaphore:
            return await self._perform_enrichment(company)
    
    async def _perform_enrichment(self, company: CompanyRecord) -> EnrichmentResult:
        """Perform the actual enrichment logic."""
        company_number = str(company["CompanyNumber"])
        company_name = company["CompanyName"]
        
        logger.debug(f"Enriching {company_name} ({company_number})")
        
        # Initialize result structure
        result: EnrichmentResult = {
            "company_url": "",
            "description": "",
            "employees_2024": "",
            "employees_2023": "",
            "employees_2022": "",
            "manufacturing_location": "",
        }
        
        try:
            # Step 1: Get basic company profile and address
            profile_task = companies_house.fetch_profile(company_number, self.http_client)
            filings_task = companies_house.fetch_latest_filings(company_number, self.http_client)
            
            profile, filings = await asyncio.gather(
                profile_task, filings_task, return_exceptions=True
            )
            
            # Step 2: Get registered address and geocode it
            if not isinstance(profile, Exception):
                address_parts = profile.get("registered_office_address", {})
                if address_parts:
                    address_str = self._format_address(address_parts)
                    geocoded = await geocoder.to_latlon(address_str, self.http_client)
                    result["manufacturing_location"] = geocoded or address_str
            
            # Step 3: Find company website
            website = await web_search.find_official_site(company_name, self.rate_limiter)
            if website:
                result["company_url"] = website
                
                # Step 4: Extract description from website
                description = await website_scraper.grab_description(website, self.http_client)
                result["description"] = description
            
            # Step 5: Extract employee counts from filings
            if not isinstance(filings, Exception) and filings:
                headcounts = await companies_house.extract_headcount_from_filings(
                    filings, self.http_client
                )
                result.update(headcounts)
            
            logger.debug(f"Completed enrichment for {company_name}")
            return result
            
        except Exception as e:
            logger.warning(f"Error enriching {company_name}: {e}")
            return result
    
    def _format_address(self, address_data: Dict[str, Any]) -> str:
        """Format address data into a string."""
        parts = [
            address_data.get("premises"),
            address_data.get("address_line_1"),
            address_data.get("address_line_2"),
            address_data.get("locality"),
            address_data.get("region"),
            address_data.get("postal_code"),
            address_data.get("country"),
        ]
        return ", ".join(filter(None, parts))


async def enrich_dataframe(
    df: pl.DataFrame,
    output_path: str,
    concurrency: int = 10,
    checkpoint_every: int = 500,
) -> pl.DataFrame:
    """
    Enrich a DataFrame of companies.
    
    Args:
        df: Input DataFrame with company data
        output_path: Path to save enriched results
        concurrency: Number of concurrent requests
        checkpoint_every: Save checkpoint every N companies
        
    Returns:
        Enriched DataFrame
    """
    console.print(f"[blue]Starting enrichment of {len(df)} companies...")
    console.print(f"[blue]Concurrency: {concurrency}, Checkpoints every: {checkpoint_every}")
    
    # Convert DataFrame to list of dictionaries
    companies = df.to_dicts()
    
    # Initialize progress tracking
    progress_tracker = ProgressTracker(len(companies), report_every=50)
    enriched_results: List[EnrichmentResult] = []
    
    async with CompanyEnricher(concurrency=concurrency) as enricher:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            
            task = progress.add_task("Enriching companies...", total=len(companies))
            
            # Process companies in batches for checkpointing
            for i in range(0, len(companies), checkpoint_every):
                batch_end = min(i + checkpoint_every, len(companies))
                batch = companies[i:batch_end]
                
                # Process batch concurrently
                batch_tasks = [
                    enricher.enrich_single_company(company) 
                    for company in batch
                ]
                
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                # Process results
                for j, result in enumerate(batch_results):
                    if isinstance(result, Exception):
                        logger.error(f"Error processing company {i+j}: {result}")
                        result = EnrichmentResult(
                            company_url="", description="", 
                            employees_2024="", employees_2023="", employees_2022="",
                            manufacturing_location=""
                        )
                        progress_tracker.update(success=False)
                    else:
                        progress_tracker.update(success=True)
                    
                    enriched_results.append(result)
                    progress.update(task, advance=1)
                
                # Save checkpoint
                if i + len(batch) < len(companies) or i + len(batch) == len(companies):
                    checkpoint_df = _merge_results(df.slice(0, len(enriched_results)), enriched_results)
                    await save_checkpoint(checkpoint_df, output_path, mode="overwrite")
                    console.print(f"[green]Checkpoint saved: {len(enriched_results)} companies processed")
    
    # Final report
    progress_tracker.final_report()
    
    # Create final merged DataFrame
    final_df = _merge_results(df, enriched_results)
    console.print(f"[green]Enrichment complete! Results saved to {output_path}")
    
    return final_df


def _merge_results(original_df: pl.DataFrame, results: List[EnrichmentResult]) -> pl.DataFrame:
    """Merge original DataFrame with enrichment results."""
    # Convert results to DataFrame
    results_df = pl.DataFrame(results)
    
    # Concatenate horizontally
    return pl.concat([original_df, results_df], how="horizontal")


# Module-level function for backward compatibility
def enrich_batch(
    df: pl.DataFrame,
    concurrency: int = 10,
    checkpoint_every: int = 500,
    out_path: str = "enriched.csv"
) -> pl.DataFrame:
    """Synchronous wrapper for DataFrame enrichment."""
    return asyncio.run(
        enrich_dataframe(df, out_path, concurrency, checkpoint_every)
    )
