import chromadb
import logging
from typing import List, Dict, Any, Optional
from sentence_transformers import SentenceTransformer
from pathlib import Path

logger = logging.getLogger(__name__)

class TopicVectorStore:
    def __init__(self, persist_directory: str = "./data/chroma_db"):
        self.persist_directory = Path(persist_directory)
        self.persist_directory.mkdir(parents=True, exist_ok=True)
        
        self.client = chromadb.PersistentClient(path=str(self.persist_directory))
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        
        self.collection = self.client.get_or_create_collection(
            name="topics",
            metadata={"description": "Topic embeddings for semantic similarity"}
        )
        
        logger.info("✅ Topic Vector Store initialized")
    
    def add_topics(self, topics: List[str]):
        """Add topics to vector store"""
        if not topics:
            return
        
        embeddings = self.embedding_model.encode(topics).tolist()
        ids = [f"topic_{hash(topic)}" for topic in topics]
        
        self.collection.add(
            embeddings=embeddings,
            documents=topics,
            ids=ids
        )
        
        logger.info(f"✅ Added {len(topics)} topics to vector store")
    
    def find_similar_topics(self, query_topic: str, threshold: float = 0.7, top_k: int = 5) -> List[Dict[str, Any]]:
        """Find similar topics using semantic similarity"""
        query_embedding = self.embedding_model.encode([query_topic]).tolist()
        
        results = self.collection.query(
            query_embeddings=query_embedding,
            n_results=top_k
        )
        
        similar_topics = []
        for i, (doc, distance) in enumerate(zip(results['documents'][0], results['distances'][0])):
            similarity = 1 - distance
            if similarity >= threshold:
                similar_topics.append({
                    'topic': doc,
                    'similarity': similarity
                })
        
        return similar_topics
    
    def get_canonical_topic(self, topic: str, threshold: float = 0.8) -> Optional[str]:
        """Get the canonical version of a topic if similar one exists"""
        similar = self.find_similar_topics(topic, threshold=threshold, top_k=1)
        if similar:
            return similar[0]['topic']
        return None