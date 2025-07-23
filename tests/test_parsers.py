"""Tests for document parsers."""

import pytest
from company_enricher.parsers.filing_ixbrl import extract_employees_from_ixbrl
from company_enricher.parsers.filing_pdf import extract_employees_from_pdf


class TestiXBRLParser:
    """Test iXBRL employee extraction."""
    
    def test_extract_employees_basic(self):
        """Test basic employee extraction from iXBRL."""
        ixbrl_content = """
        <html xmlns="http://www.xbrl.org/2003/instance">
            <uk-gaap:AverageNumberOfEmployees>25</uk-gaap:AverageNumberOfEmployees>
        </html>
        """
        result = extract_employees_from_ixbrl(ixbrl_content)
        assert result == 25
    
    def test_extract_employees_with_namespace(self):
        """Test extraction with full namespace."""
        ixbrl_content = """
        <html xmlns="http://www.xbrl.org/2003/instance"
              xmlns:uk-gaap="http://www.xbrl.org/uk/gaap/pt/2023-01-01">
            <uk-gaap:AverageNumberOfEmployees>42</uk-gaap:AverageNumberOfEmployees>
        </html>
        """
        result = extract_employees_from_ixbrl(ixbrl_content)
        assert result == 42
    
    def test_extract_employees_not_found(self):
        """Test when no employee data is found."""
        ixbrl_content = """
        <html xmlns="http://www.xbrl.org/2003/instance">
            <uk-gaap:TotalAssets>1000000</uk-gaap:TotalAssets>
        </html>
        """
        result = extract_employees_from_ixbrl(ixbrl_content)
        assert result is None
    
    def test_extract_employees_invalid_xml(self):
        """Test with invalid XML."""
        ixbrl_content = "<invalid>xml<content"
        result = extract_employees_from_ixbrl(ixbrl_content)
        assert result is None


class TestPDFParser:
    """Test PDF employee extraction."""
    
    def test_extract_employees_from_text(self):
        """Test extraction from PDF text content."""
        # Mock PDF content - in real tests you'd use actual PDF bytes
        from company_enricher.parsers.filing_pdf import _extract_employee_count_from_text
        
        text = "The average number of employees during the year was 35."
        result = _extract_employee_count_from_text(text)
        assert result == 35
    
    def test_extract_employees_various_formats(self):
        """Test different text formats for employee counts."""
        from company_enricher.parsers.filing_pdf import _extract_employee_count_from_text
        
        test_cases = [
            ("Average number of employees: 25", 25),
            ("Number of employees 42", 42),
            ("Employees: 15", 15),
            ("Total employees 38", 38),
            ("Staff numbers: 22", 22),
        ]
        
        for text, expected in test_cases:
            result = _extract_employee_count_from_text(text)
            assert result == expected, f"Failed for text: {text}"
    
    def test_extract_employees_no_match(self):
        """Test when no employee data is found."""
        from company_enricher.parsers.filing_pdf import _extract_employee_count_from_text
        
        text = "This document contains no employee information."
        result = _extract_employee_count_from_text(text)
        assert result is None
    
    def test_extract_employees_false_positive_filter(self):
        """Test filtering of false positives."""
        from company_enricher.parsers.filing_pdf import _extract_employee_count_from_text
        
        # Should be filtered out due to financial context
        text = "Revenue of 25 million pounds and employees working hard."
        result = _extract_employee_count_from_text(text)
        assert result is None


@pytest.mark.asyncio
class TestIntegration:
    """Integration tests for parsers."""
    
    async def test_full_pipeline_mock(self):
        """Test full parsing pipeline with mocked data."""
        # This would test the full pipeline with mocked HTTP responses
        # Implementation depends on your mocking strategy
        pass
