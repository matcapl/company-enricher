"""Parser for iXBRL filing documents to extract employee counts."""

import re
from typing import Optional
from xml.etree import ElementTree as ET
from bs4 import BeautifulSoup
from ..logging_config import get_logger

logger = get_logger(__name__)

# Common XBRL namespaces for UK GAAP
XBRL_NAMESPACES = {
    "uk-gaap": [
        "http://www.xbrl.org/uk/gaap/pt/2023-01-01",
        "http://www.xbrl.org/uk/gaap/pt/2022-01-01", 
        "http://www.xbrl.org/uk/gaap/pt/2021-01-01",
        "http://www.xbrl.org/uk/gaap/pt/2020-01-01",
    ],
    "gaap": [
        "http://www.xbrl.org/uk/gaap/core/2009-09-01",
        "http://www.xbrl.org/uk/gaap/core/2020-01-01",
    ]
}

# Employee-related XBRL tags to look for
EMPLOYEE_TAGS = [
    "AverageNumberOfEmployees",
    "NumberOfEmployees", 
    "EmployeesTotal",
    "DirectorsAndEmployees",
    "AverageNumberEmployeesDuringYear",
]


def extract_employees_from_ixbrl(content: str) -> Optional[int]:
    """
    Extract employee count from iXBRL document.
    
    Args:
        content: Raw iXBRL content as string
        
    Returns:
        Employee count as integer, or None if not found
    """
    try:
        # First try with BeautifulSoup for more tolerant parsing
        result = _extract_with_bs4(content)
        if result is not None:
            return result
        
        # Fallback to ElementTree for stricter XML parsing
        return _extract_with_etree(content)
        
    except Exception as e:
        logger.debug(f"iXBRL parsing error: {e}")
        return None


def _extract_with_bs4(content: str) -> Optional[int]:
    """Extract using BeautifulSoup (more tolerant)."""
    try:
        soup = BeautifulSoup(content, "xml")
        
        # Try different namespace combinations and tag variations
        for prefix, namespaces in XBRL_NAMESPACES.items():
            for namespace in namespaces:
                for tag_name in EMPLOYEE_TAGS:
                    # Try with namespace prefix
                    full_tag = f"{prefix}:{tag_name}"
                    elements = soup.find_all(full_tag)
                    
                    for element in elements:
                        value = _extract_numeric_value(element)
                        if value is not None:
                            logger.debug(f"Found employee count via BS4: {value} (tag: {full_tag})")
                            return value
                    
                    # Try without namespace prefix (sometimes stripped)
                    elements = soup.find_all(tag_name.lower())
                    for element in elements:
                        value = _extract_numeric_value(element)
                        if value is not None:
                            logger.debug(f"Found employee count via BS4: {value} (tag: {tag_name})")
                            return value
        
        # Try regex-based extraction as last resort
        return _extract_with_regex(content)
        
    except Exception as e:
        logger.debug(f"BS4 parsing failed: {e}")
        return None


def _extract_with_etree(content: str) -> Optional[int]:
    """Extract using ElementTree (stricter XML parsing)."""
    try:
        root = ET.fromstring(content)
        
        # Register namespaces and search
        for prefix, namespaces in XBRL_NAMESPACES.items():
            for namespace in namespaces:
                try:
                    ET.register_namespace(prefix, namespace)
                    
                    for tag_name in EMPLOYEE_TAGS:
                        xpath = f".//{{{namespace}}}{tag_name}"
                        elements = root.findall(xpath)
                        
                        for element in elements:
                            value = _extract_numeric_value(element)
                            if value is not None:
                                logger.debug(f"Found employee count via ET: {value} (xpath: {xpath})")
                                return value
                                
                except Exception:
                    continue
        
        return None
        
    except Exception as e:
        logger.debug(f"ElementTree parsing failed: {e}")
        return None


def _extract_with_regex(content: str) -> Optional[int]:
    """Extract using regex patterns (last resort)."""
    # Look for employee-related tags with numbers
    patterns = [
        r'<[^>]*AverageNumberOfEmployees[^>]*>(\d+)</',
        r'<[^>]*NumberOfEmployees[^>]*>(\d+)</',
        r'<[^>]*EmployeesTotal[^>]*>(\d+)</',
        r'>(\d+)</[^>]*AverageNumberOfEmployees[^>]*>',
        r'>(\d+)</[^>]*NumberOfEmployees[^>]*>',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, content, re.IGNORECASE)
        for match in matches:
            try:
                value = int(match)
                if 0 < value < 1000000:  # Sanity check
                    logger.debug(f"Found employee count via regex: {value}")
                    return value
            except ValueError:
                continue
    
    return None


def _extract_numeric_value(element) -> Optional[int]:
    """Extract numeric value from XML element."""
    if element is None:
        return None
    
    # Get text content
    if hasattr(element, 'get_text'):
        text = element.get_text(strip=True)
    elif hasattr(element, 'text') and element.text:
        text = element.text.strip()
    else:
        text = str(element).strip()
    
    if not text:
        return None
    
    # Try to extract number
    try:
        # Remove common formatting (commas, spaces)
        clean_text = re.sub(r'[,\s]', '', text)
        
        # Extract first number found
        match = re.search(r'\d+', clean_text)
        if match:
            value = int(match.group())
            
            # Sanity check: reasonable employee count
            if 0 < value < 1000000:
                return value
    
    except (ValueError, AttributeError):
        pass
    
    return None
