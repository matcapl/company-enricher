"""Website scraping for company descriptions."""

import re
from typing import Optional
import httpx
from bs4 import BeautifulSoup
from ..cache import cached
from ..logging_config import get_logger
from ..config import settings

logger = get_logger(__name__)


class WebsiteScraper:
    """Scraper for extracting company information from websites."""
    
    def __init__(self, client: httpx.AsyncClient):
        self.client = client
    
    @cached(key_prefix="website_desc", ttl_seconds=7*24*60*60)  # Cache for 7 days
    async def extract_description(self, url: str) -> str:
        """Extract company description from website."""
        if not url:
            return ""
        
        try:
            # Ensure URL has protocol
            if not url.startswith(("http://", "https://")):
                url = f"https://{url}"
            
            # Fetch page content
            response = await self.client.get(
                url,
                timeout=settings.http_timeout,
                follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            )
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Try to extract meta description first (most reliable)
            meta_desc = self._extract_meta_description(soup)
            if meta_desc:
                return meta_desc
            
            # Fallback to first meaningful paragraph
            paragraph_desc = self._extract_first_paragraph(soup)
            if paragraph_desc:
                return paragraph_desc
            
            # Last resort: extract from title and headings
            return self._extract_from_headings(soup)
            
        except httpx.TimeoutException:
            logger.debug(f"Timeout scraping {url}")
            return ""
        except httpx.HTTPError as e:
            logger.debug(f"HTTP error scraping {url}: {e}")
            return ""
        except Exception as e:
            logger.warning(f"Error scraping {url}: {e}")
            return ""
    
    def _extract_meta_description(self, soup: BeautifulSoup) -> str:
        """Extract meta description tag."""
        # Try different meta description variations
        selectors = [
            'meta[name="description"]',
            'meta[property="og:description"]',
            'meta[name="Description"]',
            'meta[property="description"]'
        ]
        
        for selector in selectors:
            meta = soup.select_one(selector)
            if meta and meta.get("content"):
                content = meta["content"].strip()
                if len(content) > 20:  # Ensure it's substantial
                    return self._clean_text(content)[:500]
        
        return ""
    
    def _extract_first_paragraph(self, soup: BeautifulSoup) -> str:
        """Extract first meaningful paragraph from page."""
        # Remove script, style, and navigation elements
        for element in soup(["script", "style", "nav", "header", "footer"]):
            element.decompose()
        
        # Look for paragraphs in likely content areas
        content_areas = soup.find_all(["main", "article", "section", "div"])
        if not content_areas:
            content_areas = [soup]
        
        for area in content_areas:
            paragraphs = area.find_all("p")
            for p in paragraphs:
                text = p.get_text(strip=True)
                
                # Skip short or boilerplate text
                if len(text) < 50:
                    continue
                
                # Skip cookie/privacy notices
                if any(keyword in text.lower() for keyword in [
                    "cookie", "privacy", "gdpr", "accept", "terms",
                    "subscribe", "newsletter", "email"
                ]):
                    continue
                
                # This looks like a good description
                return self._clean_text(text)[:500]
        
        return ""
    
    def _extract_from_headings(self, soup: BeautifulSoup) -> str:
        """Extract description from page title and headings."""
        texts = []
        
        # Get page title
        title = soup.find("title")
        if title:
            texts.append(title.get_text(strip=True))
        
        # Get main headings
        for heading in soup.find_all(["h1", "h2"], limit=3):
            text = heading.get_text(strip=True)
            if len(text) > 10:
                texts.append(text)
        
        if texts:
            combined = " - ".join(texts)
            return self._clean_text(combined)[:300]
        
        return ""
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize extracted text."""
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove common prefixes
        prefixes_to_remove = [
            "welcome to ", "about ", "home - ", "home | "
        ]
        text_lower = text.lower()
        for prefix in prefixes_to_remove:
            if text_lower.startswith(prefix):
                text = text[len(prefix):]
                break
        
        return text.strip()


# Module-level function for backward compatibility
async def grab_description(url: str, client: Optional[httpx.AsyncClient] = None) -> str:
    """Extract description from website."""
    if client is None:
        async with httpx.AsyncClient() as client:
            scraper = WebsiteScraper(client)
            return await scraper.extract_description(url)
    else:
        scraper = WebsiteScraper(client)
        return await scraper.extract_description(url)