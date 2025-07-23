"""Parser for PDF filing documents to extract employee counts."""

import re
import io
from typing import Optional
from pdfminer.high_level import extract_text
from pdfminer.pdfparser import PDFSyntaxError
from ..logging_config import get_logger

logger = get_logger(__name__)


def extract_employees_from_pdf(pdf_content: bytes) -> Optional[int]:
    """
    Extract employee count from PDF filing document.
    
    Args:
        pdf_content: Raw PDF content as bytes
        
    Returns:
        Employee count as integer, or None if not found
    """
    if not pdf_content:
        return None
    
    try:
        # Extract text from PDF (limit to first 10 pages for performance)
        text = extract_text(
            io.BytesIO(pdf_content),
            maxpages=10,
            caching=True,
            codec_errors='ignore'
        )
        
        if not text:
            return None
        
        # Clean up text
        text = text.replace('\n', ' ').replace('\r', ' ')
        text = re.sub(r'\s+', ' ', text)
        
        return _extract_employee_count_from_text(text)
        
    except PDFSyntaxError:
        logger.debug("PDF syntax error - may not be a valid PDF")
        return None
    except Exception as e:
        logger.debug(f"PDF parsing error: {e}")
        return None


def _extract_employee_count_from_text(text: str) -> Optional[int]:
    """Extract employee count from PDF text using regex patterns."""
    
    # Common patterns for employee counts in UK company filings
    patterns = [
        # "Average number of employees: 25"
        r'average\s+number\s+of\s+employees[:\s]+(\d{1,6})',
        
        # "Number of employees 42"
        r'number\s+of\s+employees[:\s]+(\d{1,6})',
        
        # "Employees: 15"
        r'employees[:\s]+(\d{1,6})',
        
        # "Total employees 38"
        r'total\s+employees[:\s]+(\d{1,6})',
        
        # "Staff numbers: 22"
        r'staff\s+numbers?[:\s]+(\d{1,6})',
        
        # "Number of persons employed: 18"
        r'number\s+of\s+persons\s+employed[:\s]+(\d{1,6})',
        
        # "Average number employed: 33"
        r'average\s+number\s+employed[:\s]+(\d{1,6})',
        
        # In tables: "Employees 27"
        r'\bemployees\s+(\d{1,6})\b',
        
        # "Directors and employees: 12" 
        r'directors\s+and\s+employees[:\s]+(\d{1,6})',
        
        # "The average number of employees during the year was 45"
        r'average\s+number\s+of\s+employees\s+during\s+the\s+year\s+was\s+(\d{1,6})',
        
        # "Employed an average of 31 people"
        r'employed\s+an\s+average\s+of\s+(\d{1,6})\s+people',
    ]
    
    text_lower = text.lower()
    
    for pattern in patterns:
        matches = re.findall(pattern, text_lower, re.IGNORECASE)
        for match in matches:
            try:
                count = int(match)
                
                # Sanity checks
                if count < 1:
                    continue
                if count > 500000:  # Very large companies
                    continue
                
                # Additional context checks to avoid false positives
                if _is_likely_employee_count(text_lower, match, count):
                    logger.debug(f"Found employee count in PDF: {count}")
                    return count
                    
            except ValueError:
                continue
    
    return None


def _is_likely_employee_count(text: str, match: str, count: int) -> bool:
    """Additional validation to ensure the number is likely an employee count."""
    
    # Find the context around the match
    match_pos = text.find(match)
    if match_pos == -1:
        return True  # Default to accepting if we can't find context
    
    # Get surrounding context (100 chars before and after)
    start = max(0, match_pos - 100)
    end = min(len(text), match_pos + 100)
    context = text[start:end]
    
    # Red flags that suggest this isn't an employee count
    red_flags = [
        'turnover', 'revenue', 'sales', 'profit', 'loss', 
        'assets', 'liabilities', 'shares', 'capital',
        'dividend', 'tax', 'vat', 'percentage', '%',
        'thousand', 'million', 'billion', 'pounds', '£',
        'euro', '€', 'dollar', '$', 'currency'
    ]
    
    for flag in red_flags:
        if flag in context:
            logger.debug(f"Rejected employee count {count} due to context: {flag}")
            return False
    
    # Green flags that suggest this is an employee count
    green_flags = [
        'employee', 'staff', 'personnel', 'workforce', 
        'employed', 'people', 'persons', 'individuals',
        'full-time', 'part-time', 'fte', 'headcount'
    ]
    
    for flag in green_flags:
        if flag in context:
            return True
    
    # If no specific context clues, accept reasonable numbers
    return 1 <= count <= 10000  # Most companies have between 1-10000 employees
