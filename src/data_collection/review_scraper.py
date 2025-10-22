import pandas as pd
from google_play_scraper import reviews, Sort
from datetime import datetime, timedelta, timezone
import time
import logging
from typing import List, Dict, Any
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config import APP_ID, LANG, COUNTRY

logger = logging.getLogger(__name__)

class ReviewScraper:
    def __init__(self, app_id: str = APP_ID, lang: str = LANG, country: str = COUNTRY):
        self.app_id = app_id
        self.lang = lang
        self.country = country
        
    def scrape_historical_reviews(self, days_range: int = 60, reviews_per_day: int = 100) -> pd.DataFrame:  # CHANGED: reviews_per_day instead of max_reviews
        """
        Scrape historical reviews for last 2 months (60 days) with 100 reviews per day
        """
        logger.info(f"Fetching reviews for '{self.app_id}' for last {days_range} days with {reviews_per_day} reviews per day...")

        all_reviews = []
        continuation_token = None
        daily_review_counts = {}
        
        today = datetime.now(timezone.utc).date()
        start_date = today - timedelta(days=days_range)

        # Keep scraping until we have enough days with 100 reviews each
        while len(daily_review_counts) < days_range:
            batch, continuation_token = reviews(
                self.app_id,
                lang=self.lang,
                country=self.country,
                sort=Sort.NEWEST,
                count=100,
                continuation_token=continuation_token
            )
            
            if not batch:
                logger.info("No more reviews available")
                break

            # Process each review in the batch
            for review in batch:
                review_date = review["at"].date()
                
                # Only process reviews within our date range
                if start_date <= review_date <= today:
                    # Check if we already have enough reviews for this day
                    if review_date not in daily_review_counts:
                        daily_review_counts[review_date] = 0
                    
                    # Only add if we haven't reached the daily limit
                    if daily_review_counts[review_date] < reviews_per_day:
                        all_reviews.append(review)
                        daily_review_counts[review_date] += 1
            
            logger.info(f"Progress: {len(all_reviews)} total reviews, {len(daily_review_counts)} days with data")
            
            # Stop if we have enough days with 100 reviews OR no more reviews
            days_with_enough_reviews = sum(1 for count in daily_review_counts.values() if count >= reviews_per_day)
            if days_with_enough_reviews >= days_range or continuation_token is None:
                logger.info(f"Stopping: {days_with_enough_reviews} days with enough reviews")
                break

            time.sleep(1)  # polite delay between requests

        # Convert to DataFrame
        df = pd.DataFrame(all_reviews)
        if df.empty:
            logger.warning("No reviews collected")
            return df
            
        df["date"] = df["at"].dt.date

        # Sort newest→oldest
        df = df.sort_values("date", ascending=False)
        
        # Log daily counts
        daily_counts = df.groupby('date').size()
        logger.info(f"Daily review counts:")
        for date_val, count in daily_counts.items():
            logger.info(f"  {date_val}: {count} reviews")
        
        logger.info(f"Final dataset: {len(df)} reviews from {df['date'].min()} to {df['date'].max()}")
        logger.info(f"Unique days with data: {len(daily_review_counts)}")
        
        return df

    def split_into_daily_batches(self, df: pd.DataFrame, reviews_per_day: int = 100) -> Dict[datetime.date, pd.DataFrame]:
        """
        Split the historical data into daily batches with exactly 100 reviews per day
        """
        if df.empty:
            return {}
            
        daily_batches = {}
        unique_dates = df['date'].unique()
        
        for date_val in unique_dates:
            daily_reviews = df[df['date'] == date_val]
            
            # Take only first 100 reviews for each day
            if len(daily_reviews) > reviews_per_day:
                daily_reviews = daily_reviews.head(reviews_per_day)
            
            daily_batches[date_val] = daily_reviews
            
        logger.info(f"Split into {len(daily_batches)} daily batches (max {reviews_per_day} reviews per day)")
        return daily_batches

    def get_sample_per_day(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Get one sample review per day for demonstration
        """
        if df.empty:
            return df
            
        df_one_per_day = (
            df.drop_duplicates(subset=["date"], keep="first")
            .sort_values("date")
            .reset_index(drop=True)
        )
        return df_one_per_day

if __name__ == "__main__":
    # Test the scraper with 2 months data
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    scraper = ReviewScraper()
    df = scraper.scrape_historical_reviews(days_range=60, reviews_per_day=100)
    
    if not df.empty:
        print(f"Successfully collected {len(df)} reviews")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"Unique days: {len(df['date'].unique())}")
        
        # Show daily counts
        daily_counts = df.groupby('date').size()
        print(f"\nDaily review counts:")
        for date_val, count in daily_counts.items():
            print(f"  {date_val}: {count} reviews")
        
        # Show sample as in original code
        sample_df = scraper.get_sample_per_day(df)
        print(f"\nOne review per day ({len(sample_df)} days):")
        print("=" * 70)
        for _, row in sample_df.iterrows():
            text = row["content"]
            short = (text[:110] + "...") if len(text) > 110 else text
            print(f"{row['date']} → ⭐{row['score']} → {short}")
        print("=" * 70)
    else:
        print("No reviews collected")