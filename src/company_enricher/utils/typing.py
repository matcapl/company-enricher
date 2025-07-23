"""Type definitions for the application."""

from typing import TypedDict, Optional, Dict, Any
from datetime import date


class CompanyRecord(TypedDict):
    """Input company record structure."""
    CompanyName: str
    CompanyNumber: str
    incorporation_date: Optional[date]
    CompanyStatus: str
    SICCode_SicText_1: Optional[str]
    SICCode_SicText_2: Optional[str]
    SICCode_SicText_3: Optional[str]
    SICCode_SicText_4: Optional[str]


class EnrichmentResult(TypedDict):
    """Output enrichment result structure."""
    company_url: str
    description: str
    employees_2024: str
    employees_2023: str
    employees_2022: str
    manufacturing_location: str


class CompaniesHouseProfile(TypedDict, total=False):
    """Companies House company profile response."""
    company_name: str
    company_number: str
    company_status: str
    company_type: str
    date_of_creation: str
    registered_office_address: Dict[str, Any]
    sic_codes: list[str]


class FilingRecord(TypedDict, total=False):
    """Companies House filing record."""
    description: str
    date: str
    made_up_date: str
    links: Dict[str, str]
    category: str
    type: str
