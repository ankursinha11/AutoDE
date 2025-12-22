# ========================================
# ENHANCED RAG SYSTEM REQUIREMENTS
# Azure OpenAI + ChromaDB + LangGraph + Advanced Retrieval
# ========================================

# ============================================
# Core Dependencies (from original requirements.txt)
# ============================================

# Data processing (for Excel outputs)
pandas>=2.0.0
numpy>=1.24.0
openpyxl>=3.1.0

# Azure OpenAI (for GPT-4 and embeddings)
openai>=1.0.0

# Local Vector Database
chromadb>=0.4.0
sentence-transformers>=2.2.0  # FREE local embeddings

# Code parsing (Ab Initio, Hadoop, Databricks)
lxml>=4.9.0
xmltodict>=0.13.0
pyyaml>=6.0

# Graph analysis (for data lineage)
networkx>=3.1

# Utilities
python-dotenv>=1.0.0  # For .env file
tqdm>=4.66.0          # Progress bars
loguru>=0.7.0         # Logging

# STAG UI (Streamlit frontend)
streamlit>=1.28.0

# Document parsing (for PDF/Excel/DOCX indexing)
pdfplumber>=0.10.0
python-docx>=1.0.0

# ============================================
# NEW ENHANCED RAG DEPENDENCIES
# ============================================

# LangGraph - Multi-agent workflow orchestration
langgraph>=0.2.0
langchain-core>=0.3.0
langchain-openai>=0.2.0
langchain-community>=0.3.0

# Hybrid Search - BM25 + Semantic
rank-bm25>=0.2.2

# Reranking - Cross-encoder for better relevance
# Note: sentence-transformers already included above (v2.2.0+)
# Cross-encoder models use sentence-transformers

# Query Understanding
rapidfuzz>=3.0.0  # Fast fuzzy string matching for query expansion

# Code Analysis
tree-sitter>=0.20.0  # For advanced code parsing
tree-sitter-python>=0.20.0
tree-sitter-java>=0.20.0

# Additional utilities
tiktoken>=0.5.0  # Token counting for context management
