import logging
from datetime import datetime, timedelta
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

from data_collection.batch_processor import DailyBatchProcessor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('phase1_batch_processing.log')
    ]
)

logger = logging.getLogger(__name__)

def run_phase1_batch_processing():
    """
    Run Phase 1 with the working historical scraping approach for 2 months
    """
    logger.info("Starting Phase 1: HISTORICAL BATCH PROCESSING (2 MONTHS)")
    
    try:
        processor = DailyBatchProcessor()
        
        # Show initial status
        summary = processor.get_processing_summary()
        logger.info(f"Initial Status: {summary}")
        
        # Process historical data (60 days = 2 months) with 100 reviews per day
        logger.info("Processing historical data as daily batches for last 2 months (100 reviews per day)...")
        result = processor.process_historical_data(days_range=60, reviews_per_day=100)  # CHANGED: Added reviews_per_day
        
        logger.info(f"Processing Result: {result}")
        
        # Final status
        final_summary = processor.get_processing_summary()
        
        print(f"\n{'='*80}")
        print("PHASE 1 - HISTORICAL BATCH PROCESSING COMPLETED!")
        print(f"{'='*80}")
        print(f"Total Batches Processed: {final_summary['total_batches']}")
        print(f"Processed Dates: {final_summary['total_processed']}")
        print(f"Last Processed: {final_summary['last_processed_date']}")
        if 'date_range' in result:
            print(f"Data Date Range: {result['date_range']['start']} to {result['date_range']['end']}")
        print(f"Successful Batches: {result.get('successful', 0)}")
        print(f"Failed Batches: {result.get('failed', 0)}")
        print(f"Reviews per day: 100")
        print(f"{'='*80}")
        
        return result
        
    except Exception as e:
        logger.error(f"Phase 1 batch processing failed: {e}")
        raise

if __name__ == "__main__":
    run_phase1_batch_processing()