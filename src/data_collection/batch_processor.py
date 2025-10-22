import pandas as pd
from datetime import datetime, date, timedelta
import logging
from typing import List, Dict, Any
import json
from pathlib import Path

from .review_scraper import ReviewScraper
from .data_storage import DataStorage
from config import BATCH_STATUS_DIR, APP_ID

logger = logging.getLogger(__name__)

class DailyBatchProcessor:
    def __init__(self):
        self.scraper = ReviewScraper()
        self.storage = DataStorage()
        self.status_file = Path(BATCH_STATUS_DIR) / 'processing_status.json'
        self._load_processing_status()
    
    def _load_processing_status(self):
        """Load which dates have been processed"""
        if self.status_file.exists():
            with open(self.status_file, 'r') as f:
                self.processing_status = json.load(f)
        else:
            self.processing_status = {
                'processed_dates': [],
                'last_processed_date': None,
                'total_batches_processed': 0
            }
    
    def _save_processing_status(self):
        """Save processing status to file"""
        with open(self.status_file, 'w') as f:
            json.dump(self.processing_status, f, indent=2, default=str)
    
    def get_unprocessed_dates(self, all_available_dates: List[date]) -> List[date]:
        """Get list of dates that haven't been processed yet from available dates"""
        # Filter out processed dates
        processed_dates = [datetime.strptime(d, '%Y-%m-%d').date() 
                          for d in self.processing_status['processed_dates']]
        
        unprocessed_dates = [d for d in all_available_dates if d not in processed_dates]
        
        logger.info(f"Unprocessed dates: {len(unprocessed_dates)} from {len(all_available_dates)} available")
        return unprocessed_dates
    
    def process_single_day_batch(self, daily_reviews: pd.DataFrame, target_date: date) -> bool:
        """
        Process reviews for a single specific day
        """
        logger.info(f"Processing daily batch for {target_date} with {len(daily_reviews)} reviews")
        
        try:
            if daily_reviews.empty:
                logger.warning(f"No reviews found for {target_date}")
                self._mark_date_processed(target_date, 0)
                return True
            
            # Store the daily batch
            self.storage.store_daily_batch(daily_reviews, APP_ID, target_date)
            
            # Update processing status
            self._mark_date_processed(target_date, len(daily_reviews))
            
            logger.info(f"Successfully processed {len(daily_reviews)} reviews for {target_date}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to process batch for {target_date}: {e}")
            return False
    
    def _mark_date_processed(self, target_date: date, review_count: int):
        """Mark a date as processed"""
        date_str = target_date.strftime('%Y-%m-%d')
        
        if date_str not in self.processing_status['processed_dates']:
            self.processing_status['processed_dates'].append(date_str)
        
        self.processing_status['last_processed_date'] = date_str
        self.processing_status['total_batches_processed'] += 1
        
        if 'batch_stats' not in self.processing_status:
            self.processing_status['batch_stats'] = {}
        
        self.processing_status['batch_stats'][date_str] = {
            'review_count': review_count,
            'processed_at': datetime.now().isoformat()
        }
        
        self._save_processing_status()
    
    def process_historical_data(self, days_range: int = 60, reviews_per_day: int = 100) -> Dict[str, Any]:  # CHANGED: Added reviews_per_day
        """
        Main method: Scrape historical data and process as daily batches
        """
        logger.info(f"Starting historical data processing for last {days_range} days with {reviews_per_day} reviews per day")
        
        # Step 1: Scrape all historical data using the working approach
        all_reviews_df = self.scraper.scrape_historical_reviews(days_range=days_range, reviews_per_day=reviews_per_day)
        
        if all_reviews_df.empty:
            logger.error("No reviews collected. Exiting.")
            return {'status': 'failed', 'error': 'No reviews collected'}
        
        # Step 2: Split into daily batches with exactly 100 reviews per day
        daily_batches = self.scraper.split_into_daily_batches(all_reviews_df, reviews_per_day=reviews_per_day)
        
        if not daily_batches:
            logger.error("No daily batches created")
            return {'status': 'failed', 'error': 'No batches created'}
        
        # Step 3: Get unprocessed dates from available data
        all_available_dates = list(daily_batches.keys())
        unprocessed_dates = self.get_unprocessed_dates(all_available_dates)
        
        if not unprocessed_dates:
            logger.info("All available dates are already processed")
            return {'status': 'completed', 'processed': 0}
        
        logger.info(f"Processing {len(unprocessed_dates)} unprocessed batches from available data")
        
        success_count = 0
        failed_dates = []
        
        # Step 4: Process each unprocessed daily batch
        for i, process_date in enumerate(unprocessed_dates, 1):
            logger.info(f"Processing batch {i}/{len(unprocessed_dates)} for {process_date}")
            
            daily_reviews = daily_batches[process_date]
            success = self.process_single_day_batch(daily_reviews, process_date)
            
            if success:
                success_count += 1
            else:
                failed_dates.append(process_date)
        
        summary = {
            'status': 'completed',
            'total_available_dates': len(all_available_dates),
            'total_attempted': len(unprocessed_dates),
            'successful': success_count,
            'failed': len(failed_dates),
            'failed_dates': failed_dates,
            'total_batches_processed': self.processing_status['total_batches_processed'],
            'date_range': {
                'start': min(all_available_dates) if all_available_dates else None,
                'end': max(all_available_dates) if all_available_dates else None
            }
        }
        
        logger.info(f"Batch processing completed: {success_count} successful, {len(failed_dates)} failed")
        return summary

    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of batch processing status"""
        return {
            'total_processed': len(self.processing_status['processed_dates']),
            'last_processed_date': self.processing_status['last_processed_date'],
            'total_batches': self.processing_status['total_batches_processed']
        }