"""
Azure OpenAI Embeddings Client
================================
High-quality embeddings using text-embedding-3-large

Benefits over sentence-transformers:
- 3072 dimensions vs 384 (8x more semantic information)
- Better multilingual support
- More accurate semantic matching
- Consistent with Azure OpenAI LLM

Falls back to sentence-transformers if no API key provided
"""

import os
from typing import List, Optional
from loguru import logger

try:
    from openai import AzureOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False


class AzureEmbeddingsClient:
    """
    Generate embeddings using Azure OpenAI or sentence-transformers

    Priority:
    1. Azure OpenAI text-embedding-3-large (if API key available)
    2. Fallback to sentence-transformers (free, local)
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        endpoint: Optional[str] = None,
        embedding_model: str = "text-embedding-3-large",
        fallback_to_local: bool = True,
    ):
        """
        Initialize embeddings client

        Args:
            api_key: Azure OpenAI API key (optional, reads from env)
            endpoint: Azure OpenAI endpoint (optional, reads from env)
            embedding_model: Model name (default: text-embedding-3-large)
            fallback_to_local: If True, fallback to sentence-transformers (default: True)
        """
        self.embedding_model = embedding_model
        self.fallback_to_local = fallback_to_local
        self.client = None
        self.local_model = None
        self.mode = None  # "azure" or "local"

        # Try Azure OpenAI first
        # Support separate embeddings resource group
        embeddings_endpoint = os.getenv("AZURE_EMBEDDINGS_ENDPOINT") or os.getenv("AZURE_OPENAI_ENDPOINT")
        embeddings_key = os.getenv("AZURE_EMBEDDINGS_API_KEY") or os.getenv("AZURE_OPENAI_API_KEY")
        embeddings_version = os.getenv("AZURE_EMBEDDINGS_API_VERSION") or os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")

        # Use separate embeddings deployment if specified, otherwise use the passed embedding_model
        embeddings_deployment = os.getenv("AZURE_EMBEDDINGS_DEPLOYMENT") or os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT") or embedding_model

        if OPENAI_AVAILABLE and (api_key or embeddings_key):
            try:
                self.client = AzureOpenAI(
                    azure_endpoint=endpoint or embeddings_endpoint,
                    api_key=api_key or embeddings_key,
                    api_version=embeddings_version,
                )

                # Update embedding model to use the deployment name
                self.embedding_model = embeddings_deployment

                # Test with a simple embedding
                test_response = self.client.embeddings.create(
                    model=self.embedding_model,
                    input="test"
                )

                self.mode = "azure"
                self.embedding_dim = len(test_response.data[0].embedding)

                # Determine if using separate resource group
                using_separate_rg = bool(os.getenv("AZURE_EMBEDDINGS_ENDPOINT"))

                logger.info(f"✓ Azure OpenAI Embeddings initialized")
                logger.info(f"  Model: {self.embedding_model}")
                logger.info(f"  Endpoint: {embeddings_endpoint}")
                logger.info(f"  Dimensions: {self.embedding_dim}")
                if using_separate_rg:
                    logger.info(f"  ℹ️  Using SEPARATE resource group for embeddings")
                else:
                    logger.info(f"  ℹ️  Using SAME resource group as LLM")

            except Exception as e:
                logger.warning(f"Could not initialize Azure OpenAI: {e}")

                if self.fallback_to_local:
                    logger.info("Falling back to local embeddings...")
                    self._initialize_local_model()
                else:
                    raise

        elif self.fallback_to_local:
            logger.info("Azure OpenAI not configured. Using local embeddings...")
            self._initialize_local_model()

        else:
            raise ValueError(
                "Azure OpenAI not available and fallback_to_local=False. "
                "Please provide API key or enable fallback."
            )

    def _initialize_local_model(self):
        """Initialize sentence-transformers model"""
        if not SENTENCE_TRANSFORMERS_AVAILABLE:
            raise ImportError(
                "sentence-transformers not available. "
                "Install with: pip install sentence-transformers"
            )

        logger.info("Loading local sentence-transformers model...")
        self.local_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.mode = "local"
        self.embedding_dim = 384

        logger.info(f"✓ Local Embeddings initialized")
        logger.info(f"  Model: all-MiniLM-L6-v2")
        logger.info(f"  Dimensions: {self.embedding_dim}")

    def embed_text(self, text: str) -> List[float]:
        """
        Generate embedding for single text

        Args:
            text: Text to embed

        Returns:
            List of floats (embedding vector)
        """
        if self.mode == "azure":
            return self._embed_with_azure([text])[0]
        else:
            return self._embed_with_local([text])[0]

    def embed_batch(self, texts: List[str], batch_size: int = 100) -> List[List[float]]:
        """
        Generate embeddings for batch of texts

        Args:
            texts: List of texts to embed
            batch_size: Batch size for Azure API (default: 100)

        Returns:
            List of embedding vectors
        """
        if not texts:
            return []

        if self.mode == "azure":
            # Process in batches to avoid API limits
            all_embeddings = []

            for i in range(0, len(texts), batch_size):
                batch = texts[i:i + batch_size]
                batch_embeddings = self._embed_with_azure(batch)
                all_embeddings.extend(batch_embeddings)

                if i + batch_size < len(texts):
                    logger.debug(f"Embedded {i + batch_size}/{len(texts)} texts...")

            return all_embeddings

        else:
            return self._embed_with_local(texts)

    def _embed_with_azure(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using Azure OpenAI"""
        try:
            response = self.client.embeddings.create(
                model=self.embedding_model,
                input=texts
            )

            embeddings = [item.embedding for item in response.data]
            return embeddings

        except Exception as e:
            logger.error(f"Azure embedding error: {e}")

            # Fallback to local if available
            if self.fallback_to_local and self.local_model is None:
                logger.warning("Falling back to local model...")
                self._initialize_local_model()

            if self.local_model:
                return self._embed_with_local(texts)
            else:
                raise

    def _embed_with_local(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using sentence-transformers"""
        embeddings = self.local_model.encode(
            texts,
            show_progress_bar=len(texts) > 100,
            convert_to_numpy=True
        )

        # Convert to list of lists
        return embeddings.tolist()

    def get_info(self) -> dict:
        """Get embedding client info"""
        return {
            "mode": self.mode,
            "model": self.embedding_model if self.mode == "azure" else "all-MiniLM-L6-v2",
            "dimensions": self.embedding_dim,
            "azure_available": self.mode == "azure",
            "local_fallback": self.fallback_to_local,
        }


# Convenience function for quick usage
def create_embedding_client(
    use_azure: bool = True,
    fallback_to_local: bool = True
) -> AzureEmbeddingsClient:
    """
    Create embedding client with sensible defaults

    Args:
        use_azure: Try to use Azure OpenAI (default: True)
        fallback_to_local: Fallback to local if Azure fails (default: True)

    Returns:
        AzureEmbeddingsClient instance
    """
    if use_azure:
        return AzureEmbeddingsClient(fallback_to_local=fallback_to_local)
    else:
        # Force local mode
        return AzureEmbeddingsClient(
            api_key=None,
            fallback_to_local=True
        )


# Example usage
if __name__ == "__main__":
    # Test embeddings
    client = create_embedding_client()

    print(f"\nEmbedding Client Info:")
    print(f"  Mode: {client.mode}")
    print(f"  Dimensions: {client.embedding_dim}")

    # Test single embedding
    text = "Ab Initio graph for customer data processing"
    embedding = client.embed_text(text)

    print(f"\nTest Embedding:")
    print(f"  Text: {text}")
    print(f"  Embedding length: {len(embedding)}")
    print(f"  First 5 values: {embedding[:5]}")

    # Test batch
    texts = [
        "Hadoop Spark script for data aggregation",
        "Databricks notebook for analytics",
        "Autosys job scheduling configuration"
    ]

    embeddings = client.embed_batch(texts)

    print(f"\nBatch Embeddings:")
    print(f"  Texts: {len(texts)}")
    print(f"  Embeddings generated: {len(embeddings)}")
