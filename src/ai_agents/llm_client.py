import logging
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import torch
from typing import List, Dict, Any
import os

logger = logging.getLogger(__name__)

class LLMClient:
    def __init__(self, model_name: str = "HuggingFaceH4/zephyr-7b-beta"):
        self.model_name = model_name
        self.model = None
        self.tokenizer = None
        self.pipeline = None
        self._load_model()
    
    def _load_model(self):
        """Load Mistral model locally via Hugging Face"""
        try:
            logger.info(f"🚀 Loading Mistral model: {self.model_name}")
            
            # Load tokenizer and model
            self.tokenizer = AutoTokenizer.from_pretrained(
                self.model_name,
                trust_remote_code=True
            )
            
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                torch_dtype=torch.float16,
                device_map="auto",
                low_cpu_mem_usage=True,
                trust_remote_code=True
            )
            
            # Create pipeline for easy generation
            self.pipeline = pipeline(
                "text-generation",
                model=self.model,
                tokenizer=self.tokenizer,
                torch_dtype=torch.float16,
                device_map="auto"
            )
            
            logger.info("✅ Mistral model loaded successfully locally!")
            
        except Exception as e:
            logger.error(f"❌ Failed to load Mistral model: {e}")
            raise
    
    def generate(self, prompt: str, max_tokens: int = 500) -> str:
        """Generate response using local Mistral model"""
        try:
            # Create proper instruction format for Mistral
            formatted_prompt = f"<s>[INST] {prompt} [/INST]"
            
            response = self.pipeline(
                formatted_prompt,
                max_new_tokens=max_tokens,
                temperature=0.1,
                do_sample=True,
                top_p=0.9,
                return_full_text=False,
                pad_token_id=self.tokenizer.eos_token_id
            )
            
            return response[0]['generated_text'].strip()
            
        except Exception as e:
            logger.error(f"Error generating response: {e}")
            return ""

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    client = LLMClient("mistralai/Mistral-7B-Instruct-v0.1")
    test_response = client.generate("Extract topics from: 'Delivery was late and food was cold'")
    print(f"Test: {test_response}")