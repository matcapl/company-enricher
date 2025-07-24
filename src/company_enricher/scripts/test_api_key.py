#!/usr/bin/env python3
"""
Test connectivity to Companies House API using COMPANIES_HOUSE_KEY.
"""

import httpx
from company_enricher.config import settings
from rich.console import Console

console = Console()

def main():
    console.print("[bold cyan]Running Companies House API key test...")

    key = settings.companies_house_key
    if not key:
        console.print("[red]❌ COMPANIES_HOUSE_KEY is not set in .env or environment.")
        return

    url = "https://api.company-information.service.gov.uk/company/00041424"  # Unilever PLC

    try:
        # Use proper Basic Auth
        response = httpx.get(url, auth=(key, ""))
        if response.status_code == 200:
            data = response.json()
            console.print(f"[green]✅ API key valid! Connected to Companies House.")
            console.print(f"[green]Company Name:[/green] {data.get('company_name')}")
            console.print(f"[green]Company Number:[/green] {data.get('company_number')}")
        elif response.status_code == 401:
            console.print("[red]❌ Invalid API key (401 Unauthorized)")
        else:
            console.print(f"[yellow]⚠️ Unexpected response: {response.status_code}")
            console.print(f"Body: {response.text}")
    except httpx.RequestError as e:
        console.print(f"[red]❌ Request failed: {e}")

if __name__ == "__main__":
    main()

