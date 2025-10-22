import logging
from datetime import datetime, timedelta
from typing import List
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

from data_collection.data_storage import DataStorage
from ai_agents.llm_client import LLMClient
from ai_agents.topic_extractor import TopicExtractionAgent
from ai_agents.vector_store import TopicVectorStore
from ai_agents.topic_consolidator import TopicConsolidationAgent

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('phase2_topic_processing.log')
    ]
)

logger = logging.getLogger(__name__)

class Phase2Processor:
    def __init__(self):
        self.storage = DataStorage()
        self.llm_client = LLMClient()
        self.vector_store = TopicVectorStore()
        self.topic_extractor = TopicExtractionAgent(self.llm_client)
        self.topic_consolidator = TopicConsolidationAgent(self.vector_store)
        
        # Setup topic tables in database
        self._setup_topic_tables()
    
    def _setup_topic_tables(self):
        """Setup tables for storing processed topics"""
        try:
            conn = self.storage._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_topics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    review_id TEXT,
                    topic_name TEXT NOT NULL,
                    topic_category TEXT,
                    date DATE NOT NULL,
                    batch_date DATE,
                    is_seed_topic BOOLEAN DEFAULT FALSE,
                    is_new_topic BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (review_id) REFERENCES raw_reviews (review_id)
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("✅ Topic tables setup completed")
            
        except Exception as e:
            logger.error(f"❌ Topic tables setup failed: {e}")
            raise
    
    def _store_processed_topics(self, topics_data: List[dict]):
        """Store processed topics in database"""
        if not topics_data:
            return
            
        try:
            conn = self.storage._get_connection()
            cursor = conn.cursor()
            
            records = []
            for topic in topics_data:
                record = (
                    topic.get('review_id'),
                    topic['topic_name'],
                    topic.get('topic_category', 'issue'),
                    topic['date'],
                    topic.get('batch_date'),
                    topic.get('is_seed_topic', False),
                    topic.get('is_new_topic', False)
                )
                records.append(record)
            
            cursor.executemany('''
                INSERT INTO processed_topics 
                (review_id, topic_name, topic_category, date, batch_date, is_seed_topic, is_new_topic)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', records)
            
            conn.commit()
            conn.close()
            logger.info(f"✅ Stored {len(topics_data)} processed topics")
            
        except Exception as e:
            logger.error(f"❌ Error storing processed topics: {e}")
            raise
    
    def process_all_batches(self, days_to_process: int = 60):
        """Process all daily batches through AI pipeline"""
        logger.info(f"🚀 Starting Phase 2: AI Topic Processing for {days_to_process} days")
        
        # Get all batch dates from Phase 1
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_to_process)
        
        batches_processed = 0
        total_topics = 0
        
        # Process each day as a batch
        current_date = start_date
        while current_date <= end_date:
            logger.info(f"📅 Processing batch for {current_date}")
            
            # Get reviews for this date
            daily_reviews = self.storage.get_reviews_by_date_range(current_date, current_date)
            
            if not daily_reviews.empty:
                # Limit to 100 reviews per day as per assignment
                daily_reviews = daily_reviews.head(100)
                
                # Extract topics
                raw_topics = self.topic_extractor.extract_topics_from_batch(daily_reviews, str(current_date))
                
                # Consolidate topics
                consolidated_topics = self.topic_consolidator.consolidate_topics(raw_topics)
                
                # Store processed topics
                self._store_processed_topics(consolidated_topics)
                
                batches_processed += 1
                total_topics += len(consolidated_topics)
                
                logger.info(f"✅ Processed {current_date}: {len(consolidated_topics)} topics")
            else:
                logger.info(f"⏭️  No reviews for {current_date}, skipping")
            
            current_date += timedelta(days=1)
        
        # Final summary
        logger.info(f"🎉 Phase 2 Completed: {batches_processed} batches, {total_topics} total topics")
        
        return {
            'batches_processed': batches_processed,
            'total_topics': total_topics,
            'date_range': {'start': start_date, 'end': end_date}
        }

def run_phase2():
    """Run Phase 2 pipeline"""
    print("🚀 Starting Senior AI Engineer Assignment - Phase 2")
    print("🤖 AI Agentic Topic Processing")
    print("=" * 60)
    
    try:
        processor = Phase2Processor()
        result = processor.process_all_batches(days_to_process=60)
        
        print(f"\n{'='*80}")
        print("🎉 PHASE 2 - AI TOPIC PROCESSING COMPLETED!")
        print(f"{'='*80}")
        print(f"📊 Batches Processed: {result['batches_processed']}")
        print(f"📈 Total Topics Extracted: {result['total_topics']}")
        print(f"📅 Date Range: {result['date_range']['start']} to {result['date_range']['end']}")
        print(f"{'='*80}")
        print("💡 Next: Phase 3 - Trend Analysis & Reporting")
        print(f"{'='*80}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Phase 2 failed: {e}")
        raise

if __name__ == "__main__":
    run_phase2()