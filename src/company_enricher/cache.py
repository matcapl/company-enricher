"""Disk caching utilities using diskcache."""

import time
from typing import Any, Optional, Callable, TypeVar, ParamSpec
from functools import wraps
import diskcache as dc
from .config import settings

# Type variables for decorator
P = ParamSpec('P')
T = TypeVar('T')

# Global cache instance
_cache: Optional[dc.Cache] = None


def get_cache() -> dc.Cache:
    """Get or create the global cache instance."""
    global _cache
    if _cache is None:
        _cache = dc.Cache(
            directory=settings.cache_dir,
            size_limit=1024 * 1024 * 1024,  # 1GB
            eviction_policy="least-recently-used",
        )
    return _cache


def cached(
    ttl_seconds: Optional[int] = None,
    key_prefix: str = "",
    ignore_kwargs: Optional[list[str]] = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    Decorator to cache function results.
    
    Args:
        ttl_seconds: Time to live in seconds. If None, uses default from settings.
        key_prefix: Prefix for cache keys.
        ignore_kwargs: List of kwargs to ignore when generating cache key.
    """
    if ttl_seconds is None:
        ttl_seconds = settings.cache_ttl_days * 24 * 60 * 60
    
    ignore_kwargs = ignore_kwargs or []
    
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            cache = get_cache()
            
            # Filter out ignored kwargs
            filtered_kwargs = {
                k: v for k, v in kwargs.items() 
                if k not in ignore_kwargs
            }
            
            # Generate cache key
            cache_key = f"{key_prefix}:{func.__name__}:{hash((args, tuple(sorted(filtered_kwargs.items()))))}"
            
            # Try to get from cache
            try:
                result = cache.get(cache_key)
                if result is not None:
                    return result
            except Exception:
                pass  # Cache miss or error, continue with function call
            
            # Call function and cache result
            result = func(*args, **kwargs)
            
            try:
                cache.set(cache_key, result, expire=ttl_seconds)
            except Exception:
                pass  # Cache write error, but return result anyway
            
            return result
        
        return wrapper
    return decorator


def clear_cache() -> None:
    """Clear all cached data."""
    cache = get_cache()
    cache.clear()


def cache_stats() -> dict[str, Any]:
    """Get cache statistics."""
    cache = get_cache()
    return {
        "size": len(cache),
        "volume": cache.volume(),
        "statistics": cache.stats(enable=True),
    }
