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
        
    def scrape_historical_reviews(self, days_range: int = 60, max_reviews: int = 5000) -> pd.DataFrame:
        """
        Scrape historical reviews for last 2 months (60 days)
        """
        logger.info(f"Fetching reviews for '{self.app_id}' for last {days_range} days...")

        all_reviews = []
        continuation_token = None
        unique_days = set()

        # keep scraping until we have enough unique review days (or run out)
        while True:
            batch, continuation_token = reviews(
                self.app_id,
                lang=self.lang,
                country=self.country,
                sort=Sort.NEWEST,
                count=200,
                continuation_token=continuation_token
            )
            if not batch:
                break

            all_reviews.extend(batch)
            # Track how many unique days seen so far
            new_days = {r["at"].date() for r in batch}
            unique_days |= new_days
            logger.info(f"Fetched {len(all_reviews)} reviews so far — {len(unique_days)} unique dates")

            # Stop once we probably covered the required day window or run out of reviews
            if len(unique_days) >= days_range or continuation_token is None or len(all_reviews) >= max_reviews:
                break

            time.sleep(1)  # polite delay between requests

        # Convert to DataFrame and process
        df = pd.DataFrame(all_reviews)
        if df.empty:
            logger.warning("No reviews collected")
            return df
            
        df["date"] = df["at"].dt.date

        today = datetime.now(timezone.utc).date()
        start_date = today - timedelta(days=days_range)
        logger.info(f"Filtering reviews from {start_date} to {today}")

        # Keep reviews only in that window
        df = df[(df["date"] >= start_date) & (df["date"] <= today)]

        # Sort newest→oldest
        df = df.sort_values("date", ascending=False)
        
        logger.info(f"Final dataset: {len(df)} reviews from {df['date'].min()} to {df['date'].max()}")
        return df

    def split_into_daily_batches(self, df: pd.DataFrame) -> Dict[datetime.date, pd.DataFrame]:
        """
        Split the historical data into daily batches for processing
        """
        if df.empty:
            return {}
            
        daily_batches = {}
        unique_dates = df['date'].unique()
        
        for date_val in unique_dates:
            daily_reviews = df[df['date'] == date_val]
            daily_batches[date_val] = daily_reviews
            
        logger.info(f"Split into {len(daily_batches)} daily batches")
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
    df = scraper.scrape_historical_reviews(days_range=60, max_reviews=3000)
    
    if not df.empty:
        print(f"Successfully collected {len(df)} reviews")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        
        # Show sample as in original code
        sample_df = scraper.get_sample_per_day(df)
        print(f"\nOne review per day ({len(sample_df)} days):")
        print("=" * 70)
        for _, row in sample_df.iterrows():
            text = row["content"]
            short = (text[:110] + "...") if len(text) > 110 else text
            print(f"{row['date']} → ⭐{row['score']} → {short}")
        print("=" * 70)
        print(f"Total unique days with reviews: {len(sample_df)}")
    else:
        print("No reviews collected")