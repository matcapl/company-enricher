"""Parser for HTML meta tags and content extraction."""

import re
from typing import Optional, Dict, Any
from bs4 import BeautifulSoup
from ..logging_config import get_logger

logger = get_logger(__name__)


def extract_meta_data(html_content: str) -> Dict[str, Any]:
    """
    Extract various meta data from HTML content.
    
    Args:
        html_content: Raw HTML content as string
        
    Returns:
        Dictionary containing extracted meta data
    """
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        return {
            'title': _extract_title(soup),
            'description': _extract_description(soup),
            'keywords': _extract_keywords(soup),
            'company_info': _extract_company_info(soup),
            'contact_info': _extract_contact_info(soup),
        }
        
    except Exception as e:
        logger.debug(f"HTML parsing error: {e}")
        return {}


def _extract_title(soup: BeautifulSoup) -> str:
    """Extract page title."""
    title_tag = soup.find('title')
    if title_tag:
        return title_tag.get_text(strip=True)
    return ""


def _extract_description(soup: BeautifulSoup) -> str:
    """Extract meta description."""
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
            if len(content) > 20:
                return _clean_text(content)
    
    return ""


def _extract_keywords(soup: BeautifulSoup) -> list[str]:
    """Extract meta keywords."""
    meta = soup.find('meta', attrs={'name': re.compile('^keywords$', re.I)})
    if meta and meta.get('content'):
        keywords = meta['content'].split(',')
        return [kw.strip() for kw in keywords if kw.strip()]
    return []


def _extract_company_info(soup: BeautifulSoup) -> Dict[str, str]:
    """Extract company-specific information from page."""
    info = {}
    
    # Look for structured data (JSON-LD)
    scripts = soup.find_all('script', type='application/ld+json')
    for script in scripts:
        try:
            import json
            data = json.loads(script.string)
            if isinstance(data, dict):
                if data.get('@type') in ['Organization', 'Corporation', 'Company']:
                    info.update({
                        'name': data.get('name', ''),
                        'description': data.get('description', ''),
                        'url': data.get('url', ''),
                        'telephone': data.get('telephone', ''),
                        'address': _format_address(data.get('address', {})),
                    })
        except (json.JSONDecodeError, AttributeError):
            continue
    
    # Look for Open Graph data
    og_selectors = {
        'og_title': 'meta[property="og:title"]',
        'og_description': 'meta[property="og:description"]',
        'og_url': 'meta[property="og:url"]',
        'og_site_name': 'meta[property="og:site_name"]',
    }
    
    for key, selector in og_selectors.items():
        meta = soup.select_one(selector)
        if meta and meta.get('content'):
            info[key] = meta['content'].strip()
    
    return {k: v for k, v in info.items() if v}


def _extract_contact_info(soup: BeautifulSoup) -> Dict[str, str]:
    """Extract contact information from page."""
    contact_info = {}
    
    # Get page text for pattern matching
    text = soup.get_text()
    
    # Extract email addresses
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    emails = re.findall(email_pattern, text)
    if emails:
        # Filter out common non-business emails
        business_emails = [
            email for email in emails 
            if not any(spam in email.lower() for spam in ['noreply', 'no-reply', 'donotreply'])
        ]
        if business_emails:
            contact_info['email'] = business_emails[0]
    
    # Extract phone numbers (UK format)
    phone_patterns = [
        r'\+44\s?\d{3}\s?\d{3}\s?\d{4}',  # +44 format
        r'0\d{3}\s?\d{3}\s?\d{4}',       # 0xxx format
        r'\(\d{4}\)\s?\d{6}',            # (0xxx) format
    ]
    
    for pattern in phone_patterns:
        matches = re.findall(pattern, text)
        if matches:
            contact_info['phone'] = matches[0]
            break
    
    return contact_info


def _format_address(address_data: Any) -> str:
    """Format address data from structured data."""
    if isinstance(address_data, str):
        return address_data
    
    if isinstance(address_data, dict):
        parts = [
            address_data.get('streetAddress', ''),
            address_data.get('addressLocality', ''),
            address_data.get('addressRegion', ''),
            address_data.get('postalCode', ''),
            address_data.get('addressCountry', ''),
        ]
        return ', '.join(filter(None, parts))
    
    return ""


def _clean_text(text: str) -> str:
    """Clean and normalize text."""
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove common HTML artifacts
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')
    
    return text.strip()
