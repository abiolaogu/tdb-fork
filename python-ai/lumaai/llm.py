"""
LLM Provider for LumaDB AI Service.

Handles text generation requests using various backends (Transformers, Llama.cpp, or Mock).
"""

import asyncio
from typing import Optional, List, Dict, Any
from abc import ABC, abstractmethod

class LLMProvider(ABC):
    """Abstract base class for LLM providers."""
    
    @abstractmethod
    async def generate(self, prompt: str, max_tokens: int = 100, temperature: float = 0.7) -> str:
        """Generate text from prompt."""
        pass

    @abstractmethod
    async def load(self):
        """Load model resources."""
        pass

class MockLLMProvider(LLMProvider):
    """Mock LLM for testing and low-resource environments."""
    
    async def load(self):
        print("Mock LLM loaded.")

    async def generate(self, prompt: str, max_tokens: int = 100, temperature: float = 0.7) -> str:
        """Generate deterministic mock response based on prompt."""
        # Simulate simple understanding
        prompt_lower = prompt.lower()
        if "select" in prompt_lower or "find" in prompt_lower:
            return f"Answer based on data: Found 5 records matching your criteria. Top result: {{ 'id': 'doc_1', 'value': 0.98 }}."
        elif "summarize" in prompt_lower:
            return "Summary: The provided documents discuss database architecture and high-performance storage engines."
        else:
            return "I am a mock LLM. I received your request and typically I would generate a coherent answer based on the RAG context provided."

class TransformersLLMProvider(LLMProvider):
    """LLM using HuggingFace Transformers (e.g. GPT-2 or TinyLlama)."""
    
    def __init__(self, model_name: str = "gpt2"):
        self.model_name = model_name
        self.pipeline = None
        self.lock = asyncio.Lock()

    async def load(self):
        try:
            from transformers import pipeline
            # Use a very small model for default to avoid huge downloads in this environment
            print(f"Loading Transformers model: {self.model_name}")
            self.pipeline = pipeline("text-generation", model=self.model_name)
            print("Transformers model loaded.")
        except ImportError:
            print("Transformers not installed. specific features disabled.")
        except Exception as e:
            print(f"Failed to load Transformers model: {e}")

    async def generate(self, prompt: str, max_tokens: int = 100, temperature: float = 0.7) -> str:
        if not self.pipeline:
            return "Error: Model not loaded."
            
        async with self.lock:
            # Run in executor to avoid blocking event loop
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, 
                lambda: self.pipeline(prompt, max_length=len(prompt.split()) + max_tokens, num_return_sequences=1)
            )
            return result[0]['generated_text'][len(prompt):].strip()
