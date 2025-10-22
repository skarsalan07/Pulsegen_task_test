import pandas as pd
import sqlite3
import logging
from datetime import datetime, date
from pathlib import Path
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config import DB_PATH

logger = logging.getLogger(__name__)

class DataStorage:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.setup_database()
    
    def setup_database(self):
        """Initialize database tables with batch support"""
        try:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Table for raw reviews
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS raw_reviews (
                    review_id TEXT PRIMARY KEY,
                    content TEXT NOT NULL,
                    score INTEGER,
                    date DATE NOT NULL,
                    at TIMESTAMP,
                    app_id TEXT,
                    thumbs_up_count INTEGER,
                    batch_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Table for batch processing status
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS batch_processing (
                    batch_date DATE PRIMARY KEY,
                    review_count INTEGER,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'completed'
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON raw_reviews(date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_batch_date ON raw_reviews(batch_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_app_id ON raw_reviews(app_id)')
            
            conn.commit()
            conn.close()
            logger.info("Database setup completed with batch support")
            
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise
    
    def store_daily_batch(self, df: pd.DataFrame, app_id: str, batch_date: date):
        """Store a daily batch of reviews"""
        if df.empty:
            logger.warning(f"No data to store for batch {batch_date}")
            return
            
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Prepare data for insertion - convert Timestamp to string
            records = []
            for _, row in df.iterrows():
                # Convert pandas Timestamp to datetime string for SQLite
                at_timestamp = row['at']
                if hasattr(at_timestamp, 'strftime'):
                    at_timestamp = at_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                
                record = (
                    row.get('reviewId', f"rev_{hash(row['content'])}_{row['at'].timestamp()}"),
                    row['content'],
                    row['score'],
                    row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else row['date'],
                    at_timestamp,
                    app_id,
                    row.get('thumbsUpCount', 0),
                    batch_date.strftime('%Y-%m-%d')
                )
                records.append(record)
            
            # Insert or ignore duplicates
            cursor.executemany('''
                INSERT OR IGNORE INTO raw_reviews 
                (review_id, content, score, date, at, app_id, thumbs_up_count, batch_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', records)
            
            inserted_count = cursor.rowcount
            
            # Update batch processing status
            cursor.execute('''
                INSERT OR REPLACE INTO batch_processing 
                (batch_date, review_count, processed_at)
                VALUES (?, ?, ?)
            ''', (batch_date.strftime('%Y-%m-%d'), inserted_count, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Stored {inserted_count} reviews for batch {batch_date}")
            
        except Exception as e:
            logger.error(f"Error storing batch {batch_date}: {e}")
            raise
    
    def get_reviews_by_batch_date(self, batch_date: date) -> pd.DataFrame:
        """Get all reviews processed in a specific batch"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = """
            SELECT * FROM raw_reviews 
            WHERE batch_date = ?
            ORDER BY at DESC
            """
            
            df = pd.read_sql_query(query, conn, params=[batch_date.strftime('%Y-%m-%d')])
            conn.close()
            
            logger.info(f"Retrieved {len(df)} reviews from batch {batch_date}")
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving batch {batch_date}: {e}")
            return pd.DataFrame()
    
    def get_reviews_by_date_range(self, start_date: date, end_date: date, app_id: str = None) -> pd.DataFrame:
        """Get reviews for a specific date range"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            if app_id:
                query = """
                SELECT * FROM raw_reviews 
                WHERE date BETWEEN ? AND ? AND app_id = ?
                ORDER BY date DESC, at DESC
                """
                params = [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), app_id]
            else:
                query = """
                SELECT * FROM raw_reviews 
                WHERE date BETWEEN ? AND ?
                ORDER BY date DESC, at DESC
                """
                params = [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            logger.info(f"Retrieved {len(df)} reviews from {start_date} to {end_date}")
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving reviews: {e}")
            return pd.DataFrame()
    
    def get_database_stats(self) -> dict:
        """Get database statistics"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Total reviews
            cursor.execute("SELECT COUNT(*) FROM raw_reviews")
            total_reviews = cursor.fetchone()[0]
            
            # Date range
            cursor.execute("SELECT MIN(date), MAX(date) FROM raw_reviews")
            min_date, max_date = cursor.fetchone()
            
            # Batch statistics
            cursor.execute("SELECT COUNT(DISTINCT batch_date) FROM raw_reviews")
            total_batches = cursor.fetchone()[0]
            
            # Reviews per app
            cursor.execute("SELECT app_id, COUNT(*) FROM raw_reviews GROUP BY app_id")
            app_stats = dict(cursor.fetchall())
            
            conn.close()
            
            stats = {
                'total_reviews': total_reviews,
                'total_batches': total_batches,
                'date_range': {'min': min_date, 'max': max_date},
                'reviews_per_app': app_stats
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}
    
    def export_to_csv(self, filepath: str):
        """Export all reviews to CSV"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query("SELECT * FROM raw_reviews ORDER BY date DESC", conn)
            conn.close()
            
            df.to_csv(filepath, index=False)
            logger.info(f"Exported {len(df)} reviews to {filepath}")
            
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    storage = DataStorage()
    stats = storage.get_database_stats()
    print(f"Database Stats: {stats}")