"""Rate limiting utilities for external API calls."""

import asyncio
import time
from typing import Optional
from ..logging_config import get_logger

logger = get_logger(__name__)


class RateLimiter:
    """Token bucket rate limiter for async operations."""
    
    def __init__(self, max_rate: float, burst_size: Optional[int] = None):
        """
        Initialize rate limiter.
        
        Args:
            max_rate: Maximum requests per second
            burst_size: Maximum burst size (defaults to max_rate * 2)
        """
        self.max_rate = max_rate
        self.burst_size = burst_size or max(1, int(max_rate * 2))
        self.tokens = float(self.burst_size)
        self.last_update = time.time()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> None:
        """
        Acquire tokens from the bucket, waiting if necessary.
        
        Args:
            tokens: Number of tokens to acquire
        """
        async with self._lock:
            now = time.time()
            
            # Add tokens based on elapsed time
            elapsed = now - self.last_update
            self.tokens = min(
                self.burst_size,
                self.tokens + elapsed * self.max_rate
            )
            self.last_update = now
            
            # If we don't have enough tokens, wait
            if self.tokens < tokens:
                wait_time = (tokens - self.tokens) / self.max_rate
                logger.debug(f"Rate limit reached, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
                
                # Update tokens after waiting
                self.tokens = tokens
            
            # Consume tokens
            self.tokens -= tokens
    
    def available_tokens(self) -> int:
        """Get number of currently available tokens."""
        now = time.time()
        elapsed = now - self.last_update
        current_tokens = min(
            self.burst_size,
            self.tokens + elapsed * self.max_rate
        )
        return int(current_tokens)
    
    def reset(self) -> None:
        """Reset the rate limiter."""
        with asyncio.Lock():
            self.tokens = float(self.burst_size)
            self.last_update = time.time()


class AdaptiveRateLimiter(RateLimiter):
    """Rate limiter that adapts to API responses."""
    
    def __init__(self, initial_rate: float, min_rate: float = 0.1, max_rate: float = 10.0):
        super().__init__(initial_rate)
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.current_rate = initial_rate
        self.consecutive_successes = 0
        self.recent_failures = 0
    
    async def record_success(self) -> None:
        """Record a successful API call."""
        self.consecutive_successes += 1
        self.recent_failures = max(0, self.recent_failures - 1)
        
        # Gradually increase rate after sustained success
        if self.consecutive_successes >= 10:
            new_rate = min(self.max_rate, self.current_rate * 1.1)
            if new_rate != self.current_rate:
                logger.debug(f"Increasing rate limit to {new_rate:.2f} QPS")
                self.current_rate = new_rate
                self.max_rate = new_rate
            self.consecutive_successes = 0
    
    async def record_failure(self, is_rate_limit: bool = False) -> None:
        """Record a failed API call."""
        self.recent_failures += 1
        self.consecutive_successes = 0
        
        if is_rate_limit or self.recent_failures >= 3:
            # Reduce rate significantly on rate limit errors
            new_rate = max(self.min_rate, self.current_rate * 0.5)
            if new_rate != self.current_rate:
                logger.warning(f"Reducing rate limit to {new_rate:.2f} QPS due to failures")
                self.current_rate = new_rate
                self.max_rate = new_rate
