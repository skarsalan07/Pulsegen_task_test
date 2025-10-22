import logging
from typing import List, Dict, Any
from .vector_store import TopicVectorStore

logger = logging.getLogger(__name__)

class TopicConsolidationAgent:
    def __init__(self, vector_store: TopicVectorStore, similarity_threshold: float = 0.8):
        self.vector_store = vector_store
        self.similarity_threshold = similarity_threshold
        logger.info("✅ Topic Consolidation Agent initialized")
    
    def consolidate_topics(self, raw_topics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Consolidate similar topics using semantic similarity"""
        if not raw_topics:
            return []
        
        logger.info(f"Consolidating {len(raw_topics)} raw topics")
        
        consolidated_topics = []
        
        for topic in raw_topics:
            topic_name = topic['topic_name']
            
            canonical_topic = self.vector_store.get_canonical_topic(
                topic_name, 
                threshold=self.similarity_threshold
            )
            
            if canonical_topic and canonical_topic != topic_name:
                consolidated_topic = topic.copy()
                consolidated_topic['topic_name'] = canonical_topic
                consolidated_topic['original_topic'] = topic_name
                consolidated_topics.append(consolidated_topic)
            else:
                self.vector_store.add_topics([topic_name])
                consolidated_topics.append(topic)
        
        logger.info(f"✅ Consolidated to {len(consolidated_topics)} topics")
        return consolidated_topics