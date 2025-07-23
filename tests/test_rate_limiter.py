"""Tests for rate limiting functionality."""

import asyncio
import time
import pytest
from company_enricher.pipeline.rate_limiter import RateLimiter, AdaptiveRateLimiter


class TestRateLimiter:
    """Test basic rate limiter functionality."""
    
    @pytest.mark.asyncio
    async def test_basic_rate_limiting(self):
        """Test basic rate limiting works."""
        limiter = RateLimiter(max_rate=2.0)  # 2 requests per second
        
        start_time = time.time()
        
        # Make 3 requests - should take at least 1 second due to rate limiting
        await limiter.acquire()
        await limiter.acquire()
        await limiter.acquire()
        
        elapsed = time.time() - start_time
        assert elapsed >= 0.5  # Should take at least 0.5 seconds
    
    @pytest.mark.asyncio
    async def test_burst_capacity(self):
        """Test burst capacity works."""
        limiter = RateLimiter(max_rate=1.0, burst_size=3)
        
        start_time = time.time()
        
        # First 3 requests should be immediate (burst)
        await limiter.acquire()
        await limiter.acquire()
        await limiter.acquire()
        
        # Should be nearly instantaneous
        elapsed = time.time() - start_time
        assert elapsed < 0.1
        
        # 4th request should be delayed
        start_time = time.time()
        await limiter.acquire()
        elapsed = time.time() - start_time
        assert elapsed >= 0.9  # Should wait ~1 second
    
    @pytest.mark.asyncio
    async def test_concurrent_requests(self):
        """Test rate limiter works with concurrent requests."""
        limiter = RateLimiter(max_rate=2.0)
        
        async def make_request():
            await limiter.acquire()
            return time.time()
        
        start_time = time.time()
        
        # Make 4 concurrent requests
        tasks = [make_request() for _ in range(4)]
        results = await asyncio.gather(*tasks)
        
        # All should complete, and timing should show rate limiting
        total_elapsed = max(results) - start_time
        assert total_elapsed >= 1.0  # Should take at least 1 second for 4 requests at 2/sec
    
    def test_available_tokens(self):
        """Test token availability reporting."""
        limiter = RateLimiter(max_rate=2.0, burst_size=4)
        
        # Should start with full burst capacity
        assert limiter.available_tokens() == 4
    
    def test_reset(self):
        """Test rate limiter reset."""
        limiter = RateLimiter(max_rate=1.0, burst_size=2)
        
        # Consume all tokens
        limiter.tokens = 0
        assert limiter.available_tokens() == 0
        
        # Reset should restore tokens
        limiter.reset()
        assert limiter.available_tokens() == 2


class TestAdaptiveRateLimiter:
    """Test adaptive rate limiter functionality."""
    
    @pytest.mark.asyncio
    async def test_rate_increase_on_success(self):
        """Test rate increases after sustained success."""
        limiter = AdaptiveRateLimiter(initial_rate=1.0, max_rate=5.0)
        
        initial_rate = limiter.current_rate
        
        # Record many successes
        for _ in range(15):
            await limiter.record_success()
        
        # Rate should have increased
        assert limiter.current_rate > initial_rate
    
    @pytest.mark.asyncio
    async def test_rate_decrease_on_failure(self):
        """Test rate decreases after failures."""
        limiter = AdaptiveRateLimiter(initial_rate=2.0, min_rate=0.5)
        
        initial_rate = limiter.current_rate
        
        # Record failures
        for _ in range(5):
            await limiter.record_failure()
        
        # Rate should have decreased
        assert limiter.current_rate < initial_rate
    
    @pytest.mark.asyncio
    async def test_rate_limits_respected(self):
        """Test rate doesn't go beyond limits."""
        limiter = AdaptiveRateLimiter(initial_rate=2.0, min_rate=1.0, max_rate=3.0)
        
        # Try to increase beyond max
        for _ in range(50):
            await limiter.record_success()
        
        assert limiter.current_rate <= 3.0
        
        # Try to decrease below min
        for _ in range(50):
            await limiter.record_failure()
        
        assert limiter.current_rate >= 1.0
