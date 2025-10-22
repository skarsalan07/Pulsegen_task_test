import logging
import pandas as pd
from typing import List, Dict, Any
import re
import json
from datetime import datetime

from .llm_client import LLMClient

logger = logging.getLogger(__name__)

class TopicExtractionAgent:
    def __init__(self, llm_client: LLMClient):
        self.llm = llm_client
        
        self.seed_topics = [
            "Delivery issue",
            "Food stale", 
            "Delivery partner rude",
            "Maps not working properly",
            "Instamart should be open all night",
            "Bring back 10 minute bolt delivery"
        ]
        
        logger.info("✅ Topic Extraction Agent initialized")
    
    def extract_topics_from_batch(self, reviews_df: pd.DataFrame, batch_date: str) -> List[Dict[str, Any]]:
        """Extract topics from a batch of reviews using Agentic AI"""
        if reviews_df.empty:
            return []
        
        logger.info(f"Extracting topics from {len(reviews_df)} reviews for {batch_date}")
        
        all_topics = []
        
        # Process in chunks of 5 reviews
        chunk_size = 5
        for i in range(0, len(reviews_df), chunk_size):
            chunk = reviews_df.iloc[i:i + chunk_size]
            chunk_topics = self._process_reviews_chunk(chunk, batch_date)
            all_topics.extend(chunk_topics)
        
        logger.info(f"✅ Extracted {len(all_topics)} topics from batch {batch_date}")
        return all_topics
    
    def _process_reviews_chunk(self, reviews_chunk: pd.DataFrame, batch_date: str) -> List[Dict[str, Any]]:
        """Process a chunk of reviews using LLM"""
        try:
            reviews_text = self._prepare_reviews_for_llm(reviews_chunk)
            prompt = self._create_topic_extraction_prompt(reviews_text)
            llm_response = self.llm.generate(prompt)
            extracted_topics = self._parse_llm_response(llm_response, reviews_chunk, batch_date)
            return extracted_topics
            
        except Exception as e:
            logger.error(f"Error processing reviews chunk: {e}")
            return []
    
    def _prepare_reviews_for_llm(self, reviews_chunk: pd.DataFrame) -> str:
        """Prepare reviews text for LLM processing"""
        reviews_text = []
        for idx, row in reviews_chunk.iterrows():
            review_text = row['content']
            score = row['score']
            reviews_text.append(f"Review {idx+1} (⭐{score}): {review_text}")
        
        return "\n\n".join(reviews_text)
    
    def _create_topic_extraction_prompt(self, reviews_text: str) -> str:
        """Create prompt for topic extraction using Agentic AI approach"""
        prompt = f"""
        Analyze these app reviews and extract specific topics mentioned. 

        SEED TOPICS (use as reference):
        {', '.join(self.seed_topics)}

        REVIEWS:
        {reviews_text}

        Extract topics as:
        - Issues, requests, feedback
        - Consolidate similar phrases into single topics
        - Create clear, concise topic names
        - Identify new topics not in seed list

        Return JSON format:
        {{
            "topics": [
                {{
                    "topic_name": "topic name",
                    "category": "issue|request|feedback",
                    "review_ids": [1, 2, 3],
                    "is_new_topic": true/false
                }}
            ]
        }}

        Response:
        """
        
        return prompt
    
    def _parse_llm_response(self, llm_response: str, reviews_chunk: pd.DataFrame, batch_date: str) -> List[Dict[str, Any]]:
        """Parse LLM response and convert to structured topics"""
        try:
            json_match = re.search(r'\{.*\}', llm_response, re.DOTALL)
            if not json_match:
                return []
            
            json_str = json_match.group()
            parsed_data = json.loads(json_str)
            
            topics_data = []
            for topic in parsed_data.get('topics', []):
                topic_name = topic.get('topic_name', '').strip()
                category = topic.get('category', 'issue')
                
                if not topic_name:
                    continue
                
                # Apply to all reviews in chunk (simplified approach)
                for idx, row in reviews_chunk.iterrows():
                    topic_data = {
                        'review_id': row.get('review_id'),
                        'topic_name': topic_name,
                        'topic_category': category,
                        'date': row['date'],
                        'batch_date': batch_date,
                        'is_seed_topic': topic_name in self.seed_topics,
                        'is_new_topic': topic.get('is_new_topic', False)
                    }
                    topics_data.append(topic_data)
            
            return topics_data
            
        except Exception as e:
            logger.error(f"Error parsing LLM response: {e}")
            return []