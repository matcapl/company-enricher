"""Batch processing utilities for handling large datasets."""

import asyncio
from typing import List, Callable, TypeVar, Any, Optional
import polars as pl
from ..logging_config import get_logger

logger = get_logger(__name__)

T = TypeVar('T')
R = TypeVar('R')


class BatchProcessor:
    """Process data in batches with checkpointing."""
    
    def __init__(
        self,
        batch_size: int = 100,
        max_concurrency: int = 10,
        checkpoint_callback: Optional[Callable] = None
    ):
        self.batch_size = batch_size
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.checkpoint_callback = checkpoint_callback
    
    async def process_items(
        self,
        items: List[T],
        processor: Callable[[T], R],
        checkpoint_every: int = 500
    ) -> List[R]:
        """
        Process items in batches with optional checkpointing.
        
        Args:
            items: List of items to process
            processor: Async function to process each item
            checkpoint_every: Save checkpoint every N items
            
        Returns:
            List of processed results
        """
        results = []
        processed_count = 0
        
        # Process in batches
        for i in range(0, len(items), self.batch_size):
            batch = items[i:i + self.batch_size]
            
            # Process batch concurrently
            batch_results = await self._process_batch(batch, processor)
            results.extend(batch_results)
            
            processed_count += len(batch)
            
            # Checkpoint if needed
            if (self.checkpoint_callback and 
                processed_count % checkpoint_every == 0):
                
                logger.info(f"Checkpointing at {processed_count} items")
                await self.checkpoint_callback(results[:processed_count])
        
        return results
    
    async def _process_batch(
        self,
        batch: List[T],
        processor: Callable[[T], R]
    ) -> List[R]:
        """Process a single batch of items."""
        
        async def process_with_semaphore(item: T) -> R:
            async with self.semaphore:
                return await processor(item)
        
        tasks = [process_with_semaphore(item) for item in batch]
        return await asyncio.gather(*tasks, return_exceptions=True)


def dataframe_chunker(df: pl.DataFrame, chunk_size: int = 1000):
    """
    Yield chunks of a polars DataFrame.
    
    Args:
        df: DataFrame to chunk
        chunk_size: Size of each chunk
        
    Yields:
        DataFrame chunks
    """
    total_rows = len(df)
    
    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        yield df.slice(start, end - start)


async def save_checkpoint(
    df: pl.DataFrame,
    filepath: str,
    mode: str = "overwrite"
) -> None:
    """
    Save DataFrame checkpoint asynchronously.
    
    Args:
        df: DataFrame to save
        filepath: Output file path
        mode: Write mode ('overwrite' or 'append')
    """
    try:
        if mode == "append" and filepath.endswith('.csv'):
            # Use lazy loading for better memory efficiency
            existing_df = None
            try:
                existing_df = pl.scan_csv(filepath)
                combined_df = pl.concat([existing_df, df.lazy()]).collect()
            except Exception:
                # File doesn't exist or is empty, just save new data
                combined_df = df
            
            combined_df.write_csv(filepath)
        else:
            # Direct write
            if filepath.endswith('.csv'):
                df.write_csv(filepath)
            elif filepath.endswith('.parquet'):
                df.write_parquet(filepath)
            else:
                raise ValueError(f"Unsupported file format: {filepath}")
        
        logger.debug(f"Checkpoint saved to {filepath}")
        
    except Exception as e:
        logger.error(f"Failed to save checkpoint to {filepath}: {e}")
        raise


class ProgressTracker:
    """Track and report progress of long-running operations."""
    
    def __init__(self, total_items: int, report_every: int = 100):
        self.total_items = total_items
        self.report_every = report_every
        self.processed = 0
        self.successful = 0
        self.failed = 0
        self.start_time = asyncio.get_event_loop().time()
    
    def update(self, success: bool = True) -> None:
        """Update progress counters."""
        self.processed += 1
        if success:
            self.successful += 1
        else:
            self.failed += 1
        
        # Report progress
        if self.processed % self.report_every == 0:
            self.report()
    
    def report(self) -> None:
        """Report current progress."""
        elapsed = asyncio.get_event_loop().time() - self.start_time
        rate = self.processed / elapsed if elapsed > 0 else 0
        
        progress_pct = (self.processed / self.total_items) * 100
        eta_seconds = (self.total_items - self.processed) / rate if rate > 0 else 0
        
        logger.info(
            f"Progress: {self.processed}/{self.total_items} "
            f"({progress_pct:.1f}%) - "
            f"Success: {self.successful}, Failed: {self.failed} - "
            f"Rate: {rate:.1f}/s - "
            f"ETA: {eta_seconds:.0f}s"
        )
    
    def final_report(self) -> None:
        """Report final statistics."""
        elapsed = asyncio.get_event_loop().time() - self.start_time
        avg_rate = self.processed / elapsed if elapsed > 0 else 0
        
        logger.info(
            f"Completed: {self.processed} items in {elapsed:.1f}s "
            f"(avg {avg_rate:.1f}/s) - "
            f"Success: {self.successful}, Failed: {self.failed}"
        )
