"""Web search functionality using DuckDuckGo."""

import asyncio
import re
from typing import Optional, List
from urllib.parse import urlparse
import httpx
from duckduckgo_search import DDGS
from ..cache import cached
from ..logging_config import get_logger
from ..pipeline.rate_limiter import RateLimiter

logger = get_logger(__name__)

# Common TLDs for business websites
BUSINESS_TLDS = {
    ".com", ".co.uk", ".uk", ".org", ".net", ".io", ".tech", 
    ".biz", ".info", ".eu", ".gov.uk", ".ac.uk"
}

# Keywords that suggest a legitimate business site
BUSINESS_KEYWORDS = {
    "about", "contact", "services", "products", "company", 
    "home", "solutions", "business"
}


def is_valid_business_domain(url: str, company_name: str) -> bool:
    """Check if URL appears to be a legitimate business website."""
    try:
        parsed = urlparse(url.lower())
        domain = parsed.netloc
        
        # Remove www prefix
        if domain.startswith("www."):
            domain = domain[4:]
        
        # Check TLD
        if not any(domain.endswith(tld) for tld in BUSINESS_TLDS):
            return False
        
        # Check if company name tokens appear in domain
        company_tokens = re.findall(r'\b[a-z]{3,}\b', company_name.lower())
        domain_clean = re.sub(r'[^a-z]', '', domain)
        
        # At least one significant token from company name should appear in domain
        for token in company_tokens:
            if len(token) > 3 and token in domain_clean:
                return True
        
        return False
        
    except Exception:
        return False


class DuckDuckGoSearcher:
    """Rate-limited DuckDuckGo search client."""
    
    def __init__(self, rate_limiter: RateLimiter):
        self.rate_limiter = rate_limiter
    
    @cached(key_prefix="ddg_search", ttl_seconds=24*60*60)  # Cache for 24 hours
    async def search_company_website(self, company_name: str) -> Optional[str]:
        """Search for company's official website."""
        try:
            # Wait for rate limit
            await self.rate_limiter.acquire()
            
            # Prepare search query
            query = f'"{company_name}" official website'
            
            # Perform search in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None, 
                lambda: list(DDGS().text(query, max_results=10, region="uk-en"))
            )
            
            # Filter and validate results
            for result in results:
                url = result.get("href", "")
                title = result.get("title", "")
                body = result.get("body", "")
                
                if not url:
                    continue
                
                # Skip social media and directory sites
                if any(skip in url.lower() for skip in [
                    "facebook.com", "twitter.com", "linkedin.com", "instagram.com",
                    "companies-house.gov.uk", "companieshouse.gov.uk",
                    "yell.com", "yelp.com", "trustpilot.com",
                    "wikipedia.org", "wikidata.org"
                ]):
                    continue
                
                # Check if it looks like a business domain
                if is_valid_business_domain(url, company_name):
                    logger.debug(f"Found website for {company_name}: {url}")
                    return url
            
            logger.debug(f"No suitable website found for {company_name}")
            return None
            
        except Exception as e:
            logger.warning(f"Search error for {company_name}: {e}")
            return None


# Module-level function for backward compatibility
async def find_official_site(company_name: str, rate_limiter: RateLimiter) -> Optional[str]:
    """Find official website for a company."""
    searcher = DuckDuckGoSearcher(rate_limiter)
    return await searcher.search_company_website(company_name)
