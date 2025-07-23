"""Geocoding functionality using OpenCage and Nominatim."""

import asyncio
from typing import Optional, Tuple
import httpx
from ..cache import cached
from ..logging_config import get_logger
from ..config import settings

logger = get_logger(__name__)


class GeocoderClient:
    """Geocoding client with OpenCage and Nominatim fallback."""
    
    def __init__(self, client: httpx.AsyncClient):
        self.client = client
    
    @cached(key_prefix="geocode", ttl_seconds=30*24*60*60)  # Cache for 30 days
    async def geocode_address(self, address: str) -> Optional[str]:
        """Geocode address to lat,lng string."""
        if not address or len(address.strip()) < 10:
            return None
        
        # Try OpenCage first if API key is available
        if settings.opencage_key:
            result = await self._geocode_opencage(address)
            if result:
                return result
        
        # Fallback to Nominatim (OpenStreetMap)
        return await self._geocode_nominatim(address)
    
    async def _geocode_opencage(self, address: str) -> Optional[str]:
        """Geocode using OpenCage API."""
        try:
            url = "https://api.opencagedata.com/geocode/v1/json"
            params = {
                "q": address,
                "key": settings.opencage_key,
                "limit": 1,
                "countrycode": "gb",  # Restrict to UK
                "language": "en",
            }
            
            response = await self.client.get(
                url,
                params=params,
                timeout=settings.http_timeout
            )
            response.raise_for_status()
            
            data = response.json()
            results = data.get("results", [])
            
            if results:
                location = results[0]["geometry"]
                lat, lng = location["lat"], location["lng"]
                formatted = results[0]["formatted"]
                
                logger.debug(f"OpenCage geocoded: {address} -> {lat},{lng}")
                return f"{lat},{lng} ({formatted})"
            
            return None
            
        except Exception as e:
            logger.warning(f"OpenCage geocoding failed for '{address}': {e}")
            return None
    
    async def _geocode_nominatim(self, address: str) -> Optional[str]:
        """Geocode using Nominatim (OpenStreetMap) API."""
        try:
            # Rate limit for Nominatim (1 request per second max)
            await asyncio.sleep(1.1)
            
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                "q": address,
                "format": "json",
                "limit": 1,
                "countrycodes": "gb",
                "addressdetails": 1,
            }
            
            headers = {
                "User-Agent": "company-enricher/0.1.0 (https://github.com/your-org/company-enricher)"
            }
            
            response = await self.client.get(
                url,
                params=params,
                headers=headers,
                timeout=settings.http_timeout
            )
            response.raise_for_status()
            
            results = response.json()
            
            if results:
                result = results[0]
                lat, lng = result["lat"], result["lon"]
                display_name = result.get("display_name", "")
                
                logger.debug(f"Nominatim geocoded: {address} -> {lat},{lng}")
                return f"{lat},{lng} ({display_name})"
            
            return None
            
        except Exception as e:
            logger.warning(f"Nominatim geocoding failed for '{address}': {e}")
            return None


# Module-level function for backward compatibility
async def to_latlon(address: str, client: Optional[httpx.AsyncClient] = None) -> Optional[str]:
    """Geocode address to lat,lng coordinates."""
    if client is None:
        async with httpx.AsyncClient() as client:
            geocoder = GeocoderClient(client)
            return await geocoder.geocode_address(address)
    else:
        geocoder = GeocoderClient(client)
        return await geocoder.geocode_address(address)
