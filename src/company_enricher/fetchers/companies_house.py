"""Companies House API client."""

import asyncio
from typing import Optional, Dict, Any, List
import httpx
from ..config import settings
from ..cache import cached
from ..logging_config import get_logger
from ..utils.typing import CompaniesHouseProfile, FilingRecord

logger = get_logger(__name__)


class CompaniesHouseClient:
    """Async client for Companies House API."""
    
    def __init__(self, client: httpx.AsyncClient):
        self.client = client
        self.base_url = settings.ch_base_url
        self.doc_base_url = settings.ch_doc_base_url
    
    async def _get_json(self, url: str) -> Dict[str, Any]:
        """Make authenticated GET request and return JSON."""
        try:
            response = await self.client.get(
                url,
                auth=(settings.companies_house_key, ""),
                timeout=settings.http_timeout
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.warning(f"HTTP error fetching {url}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
            return {}
    
    @cached(key_prefix="ch_profile")
    async def get_company_profile(self, company_number: str) -> CompaniesHouseProfile:
        """Get company profile from Companies House."""
        url = f"{self.base_url}/company/{company_number}"
        return await self._get_json(url)
    
    @cached(key_prefix="ch_address")
    async def get_registered_address(self, company_number: str) -> str:
        """Get formatted registered office address."""
        url = f"{self.base_url}/company/{company_number}/registered-office-address"
        data = await self._get_json(url)
        
        if not data:
            return ""
        
        # Build address string from components
        address_parts = [
            data.get("premises"),
            data.get("address_line_1"),
            data.get("address_line_2"),
            data.get("locality"),
            data.get("region"),
            data.get("postal_code"),
            data.get("country"),
        ]
        
        return ", ".join(filter(None, address_parts))
    
    @cached(key_prefix="ch_filings")
    async def get_filing_history(
        self, 
        company_number: str, 
        category: str = "accounts",
        items_per_page: int = 20
    ) -> List[FilingRecord]:
        """Get filing history for a company."""
        url = f"{self.base_url}/company/{company_number}/filing-history"
        params = {
            "category": category,
            "items_per_page": items_per_page,
        }
        
        try:
            response = await self.client.get(
                url,
                auth=(settings.companies_house_key, ""),
                params=params,
                timeout=settings.http_timeout
            )
            response.raise_for_status()
            data = response.json()
            return data.get("items", [])
        except Exception as e:
            logger.warning(f"Error fetching filing history for {company_number}: {e}")
            return []
    
    async def get_document_content(self, document_id: str) -> bytes:
        """Download document content from document API."""
        try:
            # First get document metadata
            meta_url = f"{self.doc_base_url}/document/{document_id}"
            meta_response = await self.client.get(
                meta_url,
                auth=(settings.companies_house_key, ""),
                timeout=settings.http_timeout
            )
            meta_response.raise_for_status()
            meta_data = meta_response.json()
            
            # Then download the actual document
            doc_url = meta_data["links"]["document"]
            doc_response = await self.client.get(
                doc_url,
                auth=(settings.companies_house_key, ""),
                timeout=settings.http_timeout * 2  # Longer timeout for document download
            )
            doc_response.raise_for_status()
            return doc_response.content
            
        except Exception as e:
            logger.warning(f"Error downloading document {document_id}: {e}")
            return b""


# Module-level functions for backward compatibility
async def fetch_profile(company_number: str, client: Optional[httpx.AsyncClient] = None) -> CompaniesHouseProfile:
    """Fetch company profile."""
    if client is None:
        async with httpx.AsyncClient() as client:
            ch_client = CompaniesHouseClient(client)
            return await ch_client.get_company_profile(company_number)
    else:
        ch_client = CompaniesHouseClient(client)
        return await ch_client.get_company_profile(company_number)


async def fetch_latest_filings(company_number: str, client: Optional[httpx.AsyncClient] = None) -> List[FilingRecord]:
    """Fetch latest filings for a company."""
    if client is None:
        async with httpx.AsyncClient() as client:
            ch_client = CompaniesHouseClient(client)
            return await ch_client.get_filing_history(company_number)
    else:
        ch_client = CompaniesHouseClient(client)
        return await ch_client.get_filing_history(company_number)


async def extract_headcount_from_filings(
    filings: List[FilingRecord], 
    client: Optional[httpx.AsyncClient] = None
) -> Dict[str, str]:
    """Extract employee headcount from filing documents."""
    from ..parsers.filing_ixbrl import extract_employees_from_ixbrl
    from ..parsers.filing_pdf import extract_employees_from_pdf
    
    headcounts = {
        "employees_2024": "",
        "employees_2023": "",
        "employees_2022": "",
    }
    
    if not filings or client is None:
        return headcounts
    
    ch_client = CompaniesHouseClient(client)
    
    for filing in filings:
        try:
            # Extract year from made_up_date
            made_up_date = filing.get("made_up_date", "")
            if not made_up_date or len(made_up_date) < 4:
                continue
                
            year = made_up_date[:4]
            if year not in ["2024", "2023", "2022"]:
                continue
            
            # Skip if we already have data for this year
            if headcounts.get(f"employees_{year}"):
                continue
            
            # Get document ID from filing links
            links = filing.get("links", {})
            doc_metadata_link = links.get("document_metadata", "")
            if not doc_metadata_link:
                continue
            
            document_id = doc_metadata_link.split("/")[-1]
            if not document_id:
                continue
            
            # Download document
            content = await ch_client.get_document_content(document_id)
            if not content:
                continue
            
            # Try to extract employee count
            employee_count = None
            
            # Check if it's iXBRL (XML-based)
            if b"<html xmlns" in content[:2000] or b"xbrl" in content[:2000].lower():
                try:
                    employee_count = extract_employees_from_ixbrl(content.decode("utf-8", errors="ignore"))
                except Exception as e:
                    logger.debug(f"iXBRL parsing failed for {document_id}: {e}")
            
            # Fallback to PDF parsing
            if employee_count is None:
                try:
                    employee_count = extract_employees_from_pdf(content)
                except Exception as e:
                    logger.debug(f"PDF parsing failed for {document_id}: {e}")
            
            # Store result if found
            if employee_count is not None:
                headcounts[f"employees_{year}"] = str(employee_count)
                logger.debug(f"Found {employee_count} employees for year {year}")
                
        except Exception as e:
            logger.warning(f"Error processing filing: {e}")
            continue
    
    return headcounts
