"""
STAG - Smart Transform and Analysis Generator
==============================================
Comprehensive Streamlit frontend for CodebaseIntelligence RAG System

Features:
- Multi-system codebase chat with conversation memory
- Intelligent query routing and structured responses
- On-the-fly file indexing (upload Ab Initio, Autosys, PDFs)
- Cross-system comparison with Excel/PDF export
- Configurable AI parameters (temperature, top-k, top-p)
- Confidence scoring and source attribution
- Fuzzy matching and typo handling
- Adaptive learning within session
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import json
import io
import re

# Core services
from services.multi_collection_indexer import MultiCollectionIndexer
from services.query_router import QueryRouter
from services.response_formatter import ResponseFormatter
from services.logic_comparator import LogicComparator
from services.azure_embeddings import create_embedding_client

# Parsers
from parsers.abinitio.parser import AbInitioParser
from parsers.hadoop.parser import HadoopParser
from parsers.databricks.parser import DatabricksParser
from parsers.autosys.parser import AutosysParser
from parsers.documents.document_parser import DocumentParser

# Enhanced AbInitio Parser for structured component extraction
try:
    from parsers.abinitio.enhanced_parser import EnhancedAbInitioParser
    ENHANCED_PARSER_AVAILABLE = True
except ImportError:
    ENHANCED_PARSER_AVAILABLE = False
    EnhancedAbInitioParser = None

# RAG chatbot
from services.rag_chatbot_integrated import CodebaseRAGChatbot

# AI Analyzer
from services.ai_script_analyzer import AIScriptAnalyzer

# STTM Generator
from services.lineage.sttm_generator import STTMGenerator

# Workflow Intelligence (NEW)
from services.stag_workflow_integration import STAGWorkflowIntelligence

# UI Components
from ui.lineage_tab import render_lineage_tab
from ui.system_mapping_tab import render_system_mapping_tab
from ui.enhanced_lineage_tab import render_enhanced_lineage_tab

# ============================================
# AB INITIO FILTERED GRAPHS (36 graphs)
# ============================================
# Only these graphs will be indexed for Ab Initio
FILTERED_ABINITIO_GRAPHS = [
    # Commercial Generation (11 graphs)
    "100_commGenPrePrep",
    "105_commGenPrePrep",
    "400_commGenIpa",
    "405_commGenPatcho",
    "410_commGenPrePA",
    "415_commGenResultGmrn",
    "420_commGenFinal",
    "430_commGenFinalCluster",
    "435_commGenClusteringReport",
    "500_commGenLoadFinalFile",
    "505_GenLoadFinalFile",

    # Medicare Leads Generation (6 graphs)
    "120_mcarePrePrep",
    "440_mCareGenIpa",
    "445_mCareGenDsh",
    "450_mCareGenHets",
    "455_mCareGenHetsOnly",
    "460_mCareGenFinal",

    # CDD (13 graphs)
    "1000_CDD_PrePrep",
    "1100_CDD_Charlotte271Data",
    "1200_CDD_Charlotte271FamilyData",
    "1300_CDD_PatientAcctsXRefPermID",
    "1400_CDD_Charlotte271MRNData",
    "1500_CDD_TUSourcedFamilyMemberLink",
    "1600_CDD_HFC_FamilyFoundCoverage",
    "1700_CDD_HFC_RelatedMemberOPSourcedFC",
    "1800_CDD_HFC_Charlotte271Data",
    "2000_CDD_LoadStagingAndCallISP",
    "2200_CDD_LoadHelperFoundCoveragesAndCallISP",
    "2500_CDD_PropagateHFCForFamilyMembers",
    "2800_CDD_LoadHelperFoundCoveragesAndCallISP_Propagation",

    # GHIC (1 graph)
    "439_LoadSnavGlobalMRNXHospInsuranceCodes",

    # Data Ingestion (5 graphs - these are .plan files)
    "200_extractDataFromSqlToAbi",
    "210_compareDataInAbiToSql",
    "265_fileTransferToHadoopServer",
    "300_extractDataFromSqlToAbi_FasterETL",
    "600_consolidateArchiveCleanUp",
]

# Chat orchestration
from services.chat.chat_orchestrator import create_chat_orchestrator, UpdateType

# Utilities
from loguru import logger
import tempfile
import os

# Load environment variables
from dotenv import load_dotenv
load_dotenv()  # Load .env file to make Azure OpenAI credentials available


# ============================================
# Page Configuration
# ============================================

st.set_page_config(
    page_title="STAG - Smart Transform Analysis",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================
# Session State Initialization
# ============================================

def initialize_session_state():
    """Initialize all session state variables"""

    # Conversation history
    if 'messages' not in st.session_state:
        st.session_state.messages = []

    # RAG components
    if 'indexer' not in st.session_state:
        st.session_state.indexer = None

    if 'router' not in st.session_state:
        st.session_state.router = None

    if 'formatter' not in st.session_state:
        st.session_state.formatter = None

    if 'comparator' not in st.session_state:
        st.session_state.comparator = None

    if 'chatbot' not in st.session_state:
        st.session_state.chatbot = None

    if 'ai_analyzer' not in st.session_state:
        st.session_state.ai_analyzer = None

    if 'chat_orchestrator' not in st.session_state:
        st.session_state.chat_orchestrator = None

    # Settings
    if 'model_name' not in st.session_state:
        st.session_state.model_name = "gpt-4"

    if 'temperature' not in st.session_state:
        st.session_state.temperature = 0.3

    if 'top_k' not in st.session_state:
        st.session_state.top_k = 5

    if 'top_p' not in st.session_state:
        st.session_state.top_p = 0.9

    # Query history
    if 'query_history' not in st.session_state:
        st.session_state.query_history = []

    # Comparison mode
    if 'comparison_mode' not in st.session_state:
        st.session_state.comparison_mode = False

    if 'comparison_results' not in st.session_state:
        st.session_state.comparison_results = None

    # Indexed files tracking
    if 'indexed_files' not in st.session_state:
        st.session_state.indexed_files = {
            'abinitio': [],
            'hadoop': [],
            'databricks': [],
            'autosys': [],
            'documents': []
        }

    # Statistics
    if 'stats' not in st.session_state:
        st.session_state.stats = {}

    # Adaptive learning cache
    if 'learned_queries' not in st.session_state:
        st.session_state.learned_queries = {}


def initialize_rag_components():
    """Initialize RAG components if not already initialized"""

    if st.session_state.indexer is None:
        with st.spinner("Initializing RAG system..."):
            try:
                # Multi-collection indexer
                st.session_state.indexer = MultiCollectionIndexer(
                    vector_db_path="./outputs/vector_db"
                )

                # Query router
                st.session_state.router = QueryRouter()

                # Response formatter
                st.session_state.formatter = ResponseFormatter()

                # Logic comparator
                st.session_state.comparator = LogicComparator()

                # AI Analyzer (for intelligent analysis)
                st.session_state.ai_analyzer = AIScriptAnalyzer()

                # Chatbot (optional, for conversational mode)
                st.session_state.chatbot = CodebaseRAGChatbot(
                    use_local_search=True,
                    vector_db_path="./outputs/vector_db"
                )

                # Chat orchestrator (for agent-based streaming)
                st.session_state.chat_orchestrator = create_chat_orchestrator(
                    ai_analyzer=st.session_state.ai_analyzer,
                    indexer=st.session_state.indexer,
                    vector_store=None  # Not using vector_store directly
                )

                # Get initial stats
                st.session_state.stats = st.session_state.indexer.get_stats()

                logger.info("✓ STAG RAG components initialized")

            except Exception as e:
                st.error(f"Error initializing RAG components: {e}")
                logger.error(f"RAG initialization error: {e}")


# ============================================
# Sidebar - Configuration & Controls
# ============================================

def render_sidebar():
    """Render sidebar with configuration and controls"""

    st.sidebar.title("🚀 STAG Configuration")

    # Model selection
    st.sidebar.subheader("🤖 AI Model")
    model_options = ["gpt-4", "gpt-4o", "gpt-35-turbo"]
    st.session_state.model_name = st.sidebar.selectbox(
        "Model",
        options=model_options,
        index=model_options.index(st.session_state.model_name),
        help="Select Azure OpenAI model"
    )

    # AI Parameters
    st.sidebar.subheader("⚙️ AI Parameters")

    st.session_state.temperature = st.sidebar.slider(
        "Temperature",
        min_value=0.0,
        max_value=1.0,
        value=st.session_state.temperature,
        step=0.1,
        help="Higher = more creative, Lower = more deterministic"
    )

    st.session_state.top_k = st.sidebar.slider(
        "Top K Results",
        min_value=1,
        max_value=20,
        value=st.session_state.top_k,
        step=1,
        help="Number of search results to retrieve"
    )

    st.session_state.top_p = st.sidebar.slider(
        "Top P (Nucleus)",
        min_value=0.1,
        max_value=1.0,
        value=st.session_state.top_p,
        step=0.05,
        help="Nucleus sampling threshold"
    )

    st.sidebar.divider()

    # File upload section
    st.sidebar.subheader("📁 Index Files")

    uploaded_files = st.sidebar.file_uploader(
        "Upload Ab Initio, Autosys, or Documents",
        accept_multiple_files=True,
        type=['mp', 'jil', 'pdf', 'xlsx', 'docx', 'txt', 'md'],
        help="Upload files for on-the-fly indexing"
    )

    if uploaded_files:
        if st.sidebar.button("Index Uploaded Files"):
            index_uploaded_files(uploaded_files)

    st.sidebar.divider()

    # System filters
    st.sidebar.subheader("🔍 System Filters")

    all_systems = st.sidebar.checkbox("Search All Systems", value=True)

    if not all_systems:
        st.sidebar.multiselect(
            "Select Systems",
            options=["Ab Initio", "Hadoop", "Databricks", "Autosys", "Documents"],
            default=["Ab Initio", "Hadoop"]
        )

    st.sidebar.divider()

    # Statistics
    st.sidebar.subheader("📊 Database Stats")

    if st.session_state.stats:
        for collection, stats in st.session_state.stats.items():
            doc_count = stats.get('total_documents', 0)
            st.sidebar.metric(
                collection.replace('_collection', '').title(),
                f"{doc_count:,} docs"
            )

    st.sidebar.divider()

    # Actions
    st.sidebar.subheader("🛠️ Actions")

    if st.sidebar.button("Clear Conversation"):
        st.session_state.messages = []
        st.rerun()

    if st.sidebar.button("Refresh Stats"):
        if st.session_state.indexer:
            st.session_state.stats = st.session_state.indexer.get_stats()
            st.rerun()

    if st.sidebar.button("Export Chat History"):
        export_chat_history()


# ============================================
# File Indexing
# ============================================

def index_uploaded_files(uploaded_files):
    """Index uploaded files on-the-fly"""

    if not st.session_state.indexer:
        st.error("Indexer not initialized")
        return

    with st.spinner(f"Indexing {len(uploaded_files)} files..."):
        abinitio_files = []
        autosys_files = []
        document_files = []

        # Save files to temp directory
        temp_dir = Path(tempfile.mkdtemp())

        for uploaded_file in uploaded_files:
            file_path = temp_dir / uploaded_file.name

            with open(file_path, 'wb') as f:
                f.write(uploaded_file.getbuffer())

            # Categorize by extension
            if uploaded_file.name.endswith('.mp'):
                abinitio_files.append(file_path)
            elif uploaded_file.name.endswith('.jil'):
                autosys_files.append(file_path)
            else:
                document_files.append(file_path)

        # Index Ab Initio
        if abinitio_files:
            try:
                parser = AbInitioParser()
                for file_path in abinitio_files:
                    result = parser.parse_file(str(file_path))
                    if result.get('processes'):
                        st.session_state.indexer.index_abinitio(
                            processes=result['processes'],
                            components=result.get('components', [])
                        )
                        st.session_state.indexed_files['abinitio'].append(file_path.name)

                st.success(f"✓ Indexed {len(abinitio_files)} Ab Initio files")
            except Exception as e:
                st.error(f"Error indexing Ab Initio: {e}")

        # Index Autosys
        if autosys_files:
            try:
                parser = AutosysParser()
                for file_path in autosys_files:
                    result = parser.parse_file(str(file_path))
                    if result.get('components'):
                        jobs_dict = [job.__dict__ for job in result['components']]
                        st.session_state.indexer.index_autosys(jobs=jobs_dict)
                        st.session_state.indexed_files['autosys'].append(file_path.name)

                st.success(f"✓ Indexed {len(autosys_files)} Autosys files")
            except Exception as e:
                st.error(f"Error indexing Autosys: {e}")

        # Index documents
        if document_files:
            try:
                parser = DocumentParser()
                docs = []
                for file_path in document_files:
                    doc = parser.parse_file(str(file_path))
                    if doc:
                        docs.append(doc)

                if docs:
                    st.session_state.indexer.index_documents(docs)
                    st.session_state.indexed_files['documents'].extend(
                        [f.name for f in document_files]
                    )

                st.success(f"✓ Indexed {len(document_files)} documents")
            except Exception as e:
                st.error(f"Error indexing documents: {e}")

        # Refresh stats
        st.session_state.stats = st.session_state.indexer.get_stats()


# ============================================
# Chat Interface
# ============================================

def render_chat_interface():
    """Render main chat interface with agent-based streaming"""

    st.title("💬 STAG - Smart Transform Analysis")
    st.caption("🤖 Powered by AI agents with visible thinking process")

    # Display conversation history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

            # Display metadata if available
            if "metadata" in message and message["metadata"]:
                with st.expander("📊 Query Details"):
                    # Show thinking process if available
                    if message["metadata"].get("thinking"):
                        st.markdown("**🧠 Thinking Process:**")
                        for thought in message["metadata"]["thinking"]:
                            st.caption(f"• {thought}")

                    # Show task plan if available
                    if message["metadata"].get("task_plan"):
                        st.markdown("**📋 Task Plan:**")
                        for i, task in enumerate(message["metadata"]["task_plan"], 1):
                            st.caption(f"{i}. {task}")

                    # Show agent execution if available
                    if message["metadata"].get("agent_execution"):
                        st.markdown("**🔧 Agent Execution:**")
                        for agent_log in message["metadata"]["agent_execution"]:
                            st.caption(f"• {agent_log}")

                    # Show data
                    if message["metadata"].get("data"):
                        st.json(message["metadata"]["data"])

    # Removed document generation UI buttons - use natural language instead!
    # Example queries:
    # - "Generate document for bdf_download flow in databricks"
    # - "Create STTM mappings for ie_prebdf"
    # - "Compare merge workflow from hadoop vs databricks"
    # - "What databricks pipeline replaced ie_prebdf?"

    st.divider()

    # Chat input
    if prompt := st.chat_input("Ask about your codebase..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})

        with st.chat_message("user"):
            st.markdown(prompt)

        # Generate response with streaming
        with st.chat_message("assistant"):
            render_streaming_response(prompt)


def render_streaming_response(query: str):
    """Render response with streaming agent updates"""

    # All queries now go through the orchestrator which has intelligent routing
    # The orchestrator will automatically detect workflow mapping queries and use the appropriate handler

    # Check if orchestrator is available
    if not st.session_state.chat_orchestrator:
        st.error("Chat orchestrator not initialized. Please refresh the page.")
        return

    # Create containers for streaming updates
    thinking_container = st.container()
    task_plan_container = st.container()
    agent_execution_container = st.container()
    answer_container = st.container()

    # Track metadata for history
    thinking_logs = []
    task_plan = []
    agent_logs = []
    final_data = {}
    final_answer = ""

    try:
        # Stream updates from orchestrator
        for update in st.session_state.chat_orchestrator.process_query_stream(
            query=query,
            context=None,
            conversation_history=st.session_state.messages
        ):
            if update.type == UpdateType.THINKING:
                # Show thinking process
                with thinking_container:
                    st.caption(f"💭 {update.content}")
                thinking_logs.append(update.content)

            elif update.type == UpdateType.TASK_PLAN:
                # Show task plan
                with task_plan_container:
                    st.markdown("### 📋 Task Plan:")
                    if update.data and update.data.get("tasks"):
                        for i, task in enumerate(update.data["tasks"], 1):
                            st.caption(f"{i}. {task}")
                        task_plan = update.data["tasks"]

            elif update.type == UpdateType.TASK_START:
                # Show task start
                with agent_execution_container:
                    st.info(f"⚙️ {update.content}")
                agent_logs.append(f"[START] {update.content}")

            elif update.type == UpdateType.TASK_PROGRESS:
                # Show task progress
                with agent_execution_container:
                    st.caption(f"  ↳ {update.content}")
                agent_logs.append(f"[PROGRESS] {update.content}")

            elif update.type == UpdateType.TASK_COMPLETE:
                # Show task completion
                with agent_execution_container:
                    st.success(f"✅ {update.content}")
                agent_logs.append(f"[COMPLETE] {update.content}")

            elif update.type == UpdateType.AGENT_START:
                # Show agent start
                with agent_execution_container:
                    st.info(f"🤖 {update.content}")
                agent_logs.append(f"[AGENT START] {update.content}")

            elif update.type == UpdateType.AGENT_PROGRESS:
                # Show agent progress
                with agent_execution_container:
                    st.caption(f"  → {update.content}")
                agent_logs.append(f"[AGENT PROGRESS] {update.content}")

            elif update.type == UpdateType.AGENT_COMPLETE:
                # Show agent completion
                with agent_execution_container:
                    st.success(f"✅ {update.content}")
                agent_logs.append(f"[AGENT COMPLETE] {update.content}")

            elif update.type == UpdateType.FINAL_ANSWER:
                # Show final answer
                final_answer = update.content
                if update.data:
                    final_data = update.data

                with answer_container:
                    st.markdown("---")
                    st.markdown("### 💡 Answer:")
                    st.markdown(final_answer)

                    # Show additional data if available
                    if final_data.get("sources"):
                        with st.expander(f"📚 Sources ({len(final_data['sources'])} found)"):
                            for i, source in enumerate(final_data["sources"], 1):
                                if isinstance(source, dict):
                                    st.caption(f"{i}. {source.get('source', 'Unknown source')}")
                                else:
                                    st.caption(f"{i}. {str(source)[:100]}...")

            elif update.type == UpdateType.ERROR:
                # Show error
                with agent_execution_container:
                    st.error(f"❌ {update.content}")
                agent_logs.append(f"[ERROR] {update.content}")

        # Add assistant message to history
        st.session_state.messages.append({
            "role": "assistant",
            "content": final_answer if final_answer else "No response generated",
            "metadata": {
                "thinking": thinking_logs,
                "task_plan": task_plan,
                "agent_execution": agent_logs,
                "data": final_data,
                "timestamp": datetime.now().isoformat()
            }
        })

        # Add to query history
        st.session_state.query_history.append({
            "query": query,
            "timestamp": datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error in streaming response: {e}")
        st.error(f"Error generating response: {e}")


def generate_response(query: str) -> Dict[str, Any]:
    """Generate response using RAG system"""

    try:
        # Check adaptive learning cache
        if query in st.session_state.learned_queries:
            logger.info(f"Using cached response for: {query[:50]}...")
            return st.session_state.learned_queries[query]

        # Fuzzy matching for typos
        corrected_query = apply_fuzzy_matching(query)
        if corrected_query != query:
            st.info(f"Did you mean: *{corrected_query}*?")
            query = corrected_query

        # Route query
        routing = st.session_state.router.route_query(query)

        # Search collections
        results = st.session_state.indexer.search_multi_collection(
            query=query,
            collections=routing["collections"],
            top_k=st.session_state.top_k
        )

        # Format response
        formatted_response = st.session_state.formatter.format_response(
            results_by_collection=results,
            query=query,
            intent=routing.get("intent", "search")
        )

        # Generate answer using chatbot (if available)
        if st.session_state.chatbot:
            # Prepare context from results
            context_sources = []
            for coll, docs in results.items():
                context_sources.extend(docs)

            # Generate answer
            answer = st.session_state.chatbot.generate_answer(
                query=query,
                sources=context_sources,
                temperature=st.session_state.temperature
            )
        else:
            # Fallback: use formatted response
            answer = formatted_response["formatted_text"]

        response_data = {
            "answer": answer,
            "sources": context_sources if st.session_state.chatbot else [],
            "routing": routing,
            "formatted_response": formatted_response
        }

        # Cache in adaptive learning
        st.session_state.learned_queries[query] = response_data

        return response_data

    except Exception as e:
        logger.error(f"Error generating response: {e}")
        return {
            "answer": f"Error generating response: {e}",
            "sources": [],
            "routing": {},
            "formatted_response": {}
        }


def render_sources(sources: List[Dict[str, Any]]):
    """Render source documents with confidence scores"""

    for i, source in enumerate(sources, 1):
        metadata = source.get("metadata", {})

        # Calculate confidence score (based on relevance score)
        confidence = source.get("score", 0.5) * 100

        # Determine color
        if confidence >= 80:
            color = "green"
        elif confidence >= 60:
            color = "orange"
        else:
            color = "red"

        st.markdown(f"**{i}. {source.get('title', 'Untitled')}** "
                   f":{color}[{confidence:.0f}% confident]")

        st.markdown(f"*{source.get('content', '')[:200]}...*")

        # Metadata
        col1, col2, col3 = st.columns(3)
        with col1:
            st.caption(f"System: {metadata.get('system', 'N/A')}")
        with col2:
            st.caption(f"Type: {metadata.get('doc_type', 'N/A')}")
        with col3:
            st.caption(f"Collection: {source.get('collection', 'N/A')}")

        st.divider()


# ============================================
# Comparison Mode
# ============================================

def render_comparison_mode():
    """Render comparison mode interface"""

    st.subheader("🔄 Cross-System Comparison")

    col1, col2 = st.columns(2)

    with col1:
        st.text_input("System 1 (e.g., Ab Initio graph)", key="system1_input")

    with col2:
        st.text_input("System 2 (e.g., Hadoop script)", key="system2_input")

    if st.button("Compare Logic"):
        if st.session_state.system1_input and st.session_state.system2_input:
            run_comparison(
                st.session_state.system1_input,
                st.session_state.system2_input
            )

    # Display comparison results
    if st.session_state.comparison_results:
        render_comparison_results(st.session_state.comparison_results)


def run_comparison(system1_name: str, system2_name: str):
    """Run cross-system logic comparison"""

    with st.spinner("Comparing systems..."):
        try:
            # Search for system1
            results1 = st.session_state.indexer.search_multi_collection(
                query=system1_name,
                collections=["abinitio_collection", "hadoop_collection"],
                top_k=1
            )

            # Search for system2
            results2 = st.session_state.indexer.search_multi_collection(
                query=system2_name,
                collections=["abinitio_collection", "hadoop_collection"],
                top_k=1
            )

            # Extract top results
            system1_data = None
            system2_data = None

            for coll, docs in results1.items():
                if docs:
                    system1_data = {
                        "system_name": coll.replace('_collection', '').title(),
                        "name": docs[0].get('title'),
                        "code": docs[0].get('content'),
                        "description": docs[0].get('metadata', {})
                    }
                    break

            for coll, docs in results2.items():
                if docs:
                    system2_data = {
                        "system_name": coll.replace('_collection', '').title(),
                        "name": docs[0].get('title'),
                        "code": docs[0].get('content'),
                        "description": docs[0].get('metadata', {})
                    }
                    break

            if system1_data and system2_data:
                # Compare using LogicComparator
                comparison = st.session_state.comparator.compare_logic(
                    system1=system1_data,
                    system2=system2_data
                )

                st.session_state.comparison_results = comparison
            else:
                st.error("Could not find both systems for comparison")

        except Exception as e:
            st.error(f"Comparison error: {e}")


def render_comparison_results(comparison: Dict[str, Any]):
    """Render comparison results in structured format"""

    st.success("✓ Comparison Complete")

    # Summary
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Similarity Score", f"{comparison.get('similarity_score', 0):.2%}")
    with col2:
        equiv = "✓ Yes" if comparison.get('are_equivalent') else "✗ No"
        st.metric("Equivalent?", equiv)

    # Semantic summary
    st.markdown("### 📝 Summary")
    st.info(comparison.get('semantic_summary', 'N/A'))

    # Differences table
    if comparison.get('differences'):
        st.markdown("### 🔍 Key Differences")

        diff_df = pd.DataFrame(comparison['differences'])
        st.dataframe(diff_df, use_container_width=True)

    # Migration recommendation
    if comparison.get('migration_recommendation'):
        st.markdown("### 🚀 Migration Recommendation")

        migration = comparison['migration_recommendation']

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Difficulty", migration.get('difficulty', 'N/A').upper())
        with col2:
            st.metric("Effort Estimate", migration.get('effort_estimate', 'N/A'))

        st.markdown("**Approach:**")
        st.write(migration.get('approach', 'N/A'))

    # Export options
    st.markdown("### 📤 Export")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("Export to Excel"):
            export_comparison_to_excel(comparison)

    with col2:
        if st.button("Export to PDF"):
            export_comparison_to_pdf(comparison)


# ============================================
# Export Functions
# ============================================

def export_comparison_to_excel(comparison: Dict[str, Any]):
    """Export comparison to Excel"""

    try:
        output = io.BytesIO()

        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Summary sheet
            summary_df = pd.DataFrame([{
                'Similarity Score': comparison.get('similarity_score', 0),
                'Are Equivalent': comparison.get('are_equivalent', False),
                'Summary': comparison.get('semantic_summary', '')
            }])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)

            # Differences sheet
            if comparison.get('differences'):
                diff_df = pd.DataFrame(comparison['differences'])
                diff_df.to_excel(writer, sheet_name='Differences', index=False)

            # Migration sheet
            if comparison.get('migration_recommendation'):
                migration_df = pd.DataFrame([comparison['migration_recommendation']])
                migration_df.to_excel(writer, sheet_name='Migration', index=False)

        output.seek(0)

        st.download_button(
            label="Download Excel",
            data=output,
            file_name=f"comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error(f"Export error: {e}")


def export_comparison_to_pdf(comparison: Dict[str, Any]):
    """Export comparison to PDF (placeholder)"""
    st.info("PDF export functionality coming soon!")


def export_chat_history():
    """Export chat history to JSON"""

    try:
        history_json = json.dumps(st.session_state.messages, indent=2)

        st.sidebar.download_button(
            label="Download History",
            data=history_json,
            file_name=f"chat_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )

    except Exception as e:
        st.sidebar.error(f"Export error: {e}")


# ============================================
# Fuzzy Matching & Typo Handling
# ============================================

def apply_fuzzy_matching(query: str) -> str:
    """Apply fuzzy matching to correct common typos"""

    # Common corrections
    corrections = {
        'abinito': 'abinitio',
        'ab initio': 'abinitio',
        'hadop': 'hadoop',
        'sparc': 'spark',
        'autosys': 'autosys',
        'databrick': 'databricks',
    }

    query_lower = query.lower()

    for typo, correction in corrections.items():
        if typo in query_lower:
            query = query.replace(typo, correction)
            query = query.replace(typo.title(), correction.title())

    return query


# ============================================
# Database Management Interface
# ============================================

def render_database_management():
    """Render database management interface"""

    st.subheader("⚙️ Vector Database Management")

    st.markdown("""
    Manage your vector database collections, check status, clear data, and re-index your codebase.
    """)

    # Database status section
    st.markdown("---")
    st.markdown("### 📊 Database Status")

    col1, col2, col3 = st.columns(3)

    # Calculate total stats
    total_docs = 0
    total_collections = 0
    db_path = Path("./outputs/vector_db")
    db_size_mb = 0

    if db_path.exists():
        # Calculate size
        total_size = sum(f.stat().st_size for f in db_path.rglob('*') if f.is_file())
        db_size_mb = total_size / (1024 * 1024)

        # Count documents
        if st.session_state.stats:
            for stats in st.session_state.stats.values():
                total_docs += stats.get('total_documents', 0)
                total_collections += 1

    with col1:
        st.metric("Total Documents", f"{total_docs:,}")
    with col2:
        st.metric("Collections", total_collections)
    with col3:
        st.metric("Database Size", f"{db_size_mb:.2f} MB")

    # Refresh button
    if st.button("🔄 Refresh Status", use_container_width=True):
        if st.session_state.indexer:
            st.session_state.stats = st.session_state.indexer.get_stats()
            st.success("✓ Status refreshed!")
            st.rerun()

    # Collection details
    st.markdown("---")
    st.markdown("### 📈 Collection Details")

    if st.session_state.stats:
        for collection_name, collection_stats in st.session_state.stats.items():
            with st.expander(f"📁 {collection_name.replace('_collection', '').title()}", expanded=False):
                doc_count = collection_stats.get('total_documents', 0)

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Documents", f"{doc_count:,}")

                # Show any errors
                if 'error' in collection_stats:
                    st.error(f"⚠️ Error: {collection_stats['error']}")
    else:
        st.info("No database statistics available. Initialize the indexer first.")

    # Management operations
    st.markdown("---")
    st.markdown("### 🛠️ Management Operations")

    operation = st.selectbox(
        "Select Operation",
        [
            "View Status Only",
            "Clear Specific Collection",
            "Clear All Collections",
            "Re-index Ab Initio",
            "Re-index Hadoop",
            "Re-index Databricks",
            "Re-index Autosys",
            "Re-index Documents",
            "Re-index Everything",
            "Export Statistics"
        ]
    )

    # Operation-specific UI
    if operation == "Clear Specific Collection":
        render_clear_collection_ui()

    elif operation == "Clear All Collections":
        render_clear_all_ui()

    elif operation == "Re-index Ab Initio":
        render_reindex_abinitio_ui()

    elif operation == "Re-index Hadoop":
        render_reindex_hadoop_ui()

    elif operation == "Re-index Databricks":
        render_reindex_databricks_ui()

    elif operation == "Re-index Autosys":
        render_reindex_autosys_ui()

    elif operation == "Re-index Documents":
        render_reindex_documents_ui()

    elif operation == "Re-index Everything":
        render_reindex_all_ui()

    elif operation == "Export Statistics":
        render_export_stats_ui()


def render_clear_collection_ui():
    """UI for clearing specific collection"""
    st.markdown("#### 🗑️ Clear Specific Collection")

    collection_map = {
        'Ab Initio': 'abinitio_collection',
        'Hadoop': 'hadoop_collection',
        'Databricks': 'databricks_collection',
        'Autosys': 'autosys_collection',
        'Cross-System Links': 'cross_system_links',
        'Documents': 'documents_collection',
    }

    selected_display = st.selectbox("Select Collection to Clear", list(collection_map.keys()))
    selected_collection = collection_map[selected_display]

    # Show current count
    if st.session_state.stats and selected_collection in st.session_state.stats:
        current_count = st.session_state.stats[selected_collection].get('total_documents', 0)
        st.info(f"Current documents in {selected_display}: **{current_count:,}**")

    st.warning("⚠️ This will permanently delete all documents in this collection!")

    confirm = st.checkbox("I understand this action cannot be undone")

    if st.button("Clear Collection", type="primary", disabled=not confirm):
        with st.spinner(f"Clearing {selected_display}..."):
            try:
                # CRITICAL FIX: ChromaDB uses UUID directories, not collection names
                # Must get UUID from chroma.sqlite3 database
                import shutil
                import sqlite3

                db_path = Path("./outputs/vector_db")
                if not db_path.exists():
                    st.warning(f"Vector database directory doesn't exist")
                    return

                # Get collection UUID from SQLite
                sqlite_file = db_path / "chroma.sqlite3"
                if not sqlite_file.exists():
                    st.warning(f"ChromaDB metadata file not found")
                    return

                conn = sqlite3.connect(str(sqlite_file))
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM collections WHERE name = ?", (selected_collection,))
                row = cursor.fetchone()
                conn.close()

                if not row:
                    st.warning(f"Collection '{selected_display}' not found in database")
                    return

                collection_uuid = row[0]
                collection_path = db_path / collection_uuid

                if collection_path.exists():
                    shutil.rmtree(collection_path)
                    logger.info(f"Deleted collection directory: {collection_uuid}")

                # Also delete from SQLite metadata
                conn = sqlite3.connect(str(sqlite_file))
                cursor = conn.cursor()
                cursor.execute("DELETE FROM collections WHERE id = ?", (collection_uuid,))
                cursor.execute("DELETE FROM embeddings WHERE collection_id = ?", (collection_uuid,))
                cursor.execute("DELETE FROM segments WHERE collection = ?", (collection_uuid,))
                conn.commit()
                conn.close()

                st.success(f"✓ {selected_display} collection cleared successfully!")
                logger.info(f"Deleted collection metadata from SQLite: {selected_collection}")

                # Refresh stats and reinitialize indexer
                if st.session_state.indexer:
                    # Reinitialize the specific collection
                    from services.local_search.local_search_client import LocalSearchClient
                    client = LocalSearchClient(persist_directory=str(db_path))
                    client.create_index(selected_collection)
                    st.session_state.indexer.collections[selected_collection] = client
                    st.session_state.stats = st.session_state.indexer.get_stats()

                st.rerun()
            except Exception as e:
                st.error(f"Error clearing collection: {e}")
                logger.error(f"Collection deletion error: {e}", exc_info=True)


def render_clear_all_ui():
    """UI for clearing all collections"""
    st.markdown("#### 🗑️ Clear All Collections")

    st.error("⚠️ **DANGER ZONE** - This will delete the ENTIRE vector database!")

    total_docs = sum(stats.get('total_documents', 0)
                     for stats in st.session_state.stats.values()) if st.session_state.stats else 0

    st.warning(f"This will permanently delete **{total_docs:,} documents** across all collections!")

    confirm1 = st.checkbox("I understand this will delete all data")
    confirm2 = st.text_input("Type 'DELETE ALL' to confirm")

    if st.button("Clear All Collections", type="primary", disabled=(not confirm1 or confirm2 != "DELETE ALL")):
        with st.spinner("Clearing all collections..."):
            try:
                import shutil
                db_path = Path("./outputs/vector_db")
                if db_path.exists():
                    shutil.rmtree(db_path)
                    st.success("✓ All collections cleared successfully!")

                    # Reinitialize indexer
                    st.session_state.indexer = None
                    initialize_rag_components()

                    st.rerun()
                else:
                    st.warning("Database doesn't exist")
            except Exception as e:
                st.error(f"Error clearing database: {e}")


def render_reindex_abinitio_ui():
    """UI for re-indexing Ab Initio"""
    st.markdown("#### 🔄 Re-index Ab Initio")

    st.info("Index Ab Initio .mp files with FAWN-enhanced parser + Full STTM generation")

    # NEW: Index mode selection
    index_mode = st.radio(
        "Indexing Mode",
        ["📂 Multiple Graphs (Directory)", "🎯 Single Priority Graph", "📤 Upload Files"],
        help="Choose single graph for focused testing/demo, or multiple for full indexing"
    )

    if index_mode == "🎯 Single Priority Graph":
        st.markdown("**🎯 Index Single Priority Graph**")
        st.caption("Perfect for testing, demos, or focused analysis")

        # Single graph file input
        single_graph_path = st.text_input(
            "Graph File Path",
            placeholder="Input Files/blade/mp/265_fileTransferToHadoopServer.mp",
            help="Full path to a single .mp file"
        )

        # Options for large graphs
        st.markdown("**Pipeline Options**")
        col1, col2 = st.columns(2)
        with col1:
            generate_graphflow = st.checkbox("Generate GraphFlow Excel", value=True, help="Multi-sheet visualization")
        with col2:
            generate_sttm = st.checkbox("Generate STTM", value=True, help="Uncheck for large graphs (>200 vertices) to avoid rate limits")

        if not generate_graphflow and not generate_sttm:
            st.warning("⚠️ At least one option must be selected (GraphFlow or STTM)")

        st.info("💡 **Tip**: For large graphs (>200 vertices), uncheck STTM to avoid rate limits. GraphFlow + Vector DB indexing is still very useful!")

        if st.button("Index Single Graph", type="primary"):
            if single_graph_path and Path(single_graph_path).exists():
                if generate_graphflow or generate_sttm:
                    with st.spinner(f"🔄 Indexing single graph..."):
                        pipeline_str = []
                        if generate_graphflow:
                            pipeline_str.append("GraphFlow")
                        if generate_sttm:
                            pipeline_str.append("STTM")
                        st.info(f"📋 Pipeline: Parse → {' → '.join(pipeline_str)} → Vector DB")

                        reindex_single_abinitio_graph(
                            single_graph_path,
                            generate_graphflow=generate_graphflow,
                            generate_sttm=generate_sttm
                        )
            else:
                st.error(f"❌ File not found: {single_graph_path}")

    elif index_mode == "📂 Multiple Graphs (Directory)":
        # Option 1: Directory path
        st.markdown("**📂 Index from Directory**")
        st.caption("Indexes all .mp files in directory (uses filtered graph list)")
        directory_path = st.text_input("Ab Initio Directory Path", placeholder="Input Files/blade/mp")

        if st.button("Start Batch Indexing", type="primary"):
            if directory_path and Path(directory_path).exists():
                reindex_abinitio_from_directory(directory_path)
            else:
                st.error("Please provide a valid directory path")

    else:  # Upload Files
        # Option 2: File upload
        st.markdown("**📤 Upload Files**")
        uploaded_files = st.file_uploader(
            "Upload .mp files",
            accept_multiple_files=True,
            type=['mp'],
            key="abinitio_upload"
        )

        if st.button("Index Uploaded Files", type="primary"):
            if uploaded_files:
                reindex_abinitio_from_upload(uploaded_files)
            else:
                st.error("Please upload at least one .mp file")


def render_reindex_autosys_ui():
    """UI for re-indexing Autosys"""
    st.markdown("#### 🔄 Re-index Autosys")

    st.info("Index Autosys .jil files with AI-powered analysis")

    # Option 1: Directory path
    st.markdown("**Option 1: Index from Directory**")
    directory_path = st.text_input("Autosys Directory Path", placeholder="/path/to/autosys")

    # Option 2: File upload
    st.markdown("**Option 2: Upload Files**")
    uploaded_files = st.file_uploader(
        "Upload .jil files",
        accept_multiple_files=True,
        type=['jil'],
        key="autosys_upload"
    )

    if st.button("Start Indexing", type="primary"):
        if directory_path and Path(directory_path).exists():
            reindex_autosys_from_directory(directory_path)
        elif uploaded_files:
            reindex_autosys_from_upload(uploaded_files)
        else:
            st.error("Please provide a directory path or upload files")


def render_reindex_hadoop_ui():
    """UI for re-indexing Hadoop"""
    st.markdown("#### 🔄 Re-index Hadoop")

    st.info("Index Hadoop workflows (Pig, Hive, Oozie XML)")

    # Option 1: Directory path
    st.markdown("**Option 1: Index from Directory**")
    directory_path = st.text_input("Hadoop Directory Path", placeholder="/path/to/hadoop")

    # Option 2: File upload
    st.markdown("**Option 2: Upload Files**")
    uploaded_files = st.file_uploader(
        "Upload Hadoop files",
        accept_multiple_files=True,
        type=['pig', 'hql', 'xml', 'py'],
        key="hadoop_upload"
    )

    if st.button("Start Indexing", type="primary"):
        if directory_path and Path(directory_path).exists():
            reindex_hadoop_from_directory(directory_path)
        elif uploaded_files:
            reindex_hadoop_from_upload(uploaded_files)
        else:
            st.error("Please provide a directory path or upload files")


def render_reindex_databricks_ui():
    """UI for re-indexing Databricks"""
    st.markdown("#### 🔄 Re-index Databricks")

    st.info("Index Databricks notebooks (Python, Scala, SQL)")

    # Option 1: Directory path
    st.markdown("**Option 1: Index from Directory**")
    directory_path = st.text_input("Databricks Directory Path", placeholder="/path/to/databricks")

    # Option 2: File upload
    st.markdown("**Option 2: Upload Files**")
    uploaded_files = st.file_uploader(
        "Upload Databricks notebooks",
        accept_multiple_files=True,
        type=['py', 'scala', 'sql', 'ipynb'],
        key="databricks_upload"
    )

    if st.button("Start Indexing", type="primary"):
        if directory_path and Path(directory_path).exists():
            reindex_databricks_from_directory(directory_path)
        elif uploaded_files:
            reindex_databricks_from_upload(uploaded_files)
        else:
            st.error("Please provide a directory path or upload files")


def render_reindex_documents_ui():
    """UI for re-indexing documents"""
    st.markdown("#### 🔄 Re-index Documents")

    st.info("Index PDF, Excel, Word, and Markdown documents")

    # Option 1: Directory path
    st.markdown("**Option 1: Index from Directory**")
    directory_path = st.text_input("Documents Directory Path", placeholder="/path/to/documents")
    recursive = st.checkbox("Include subdirectories", value=True)

    # Option 2: File upload
    st.markdown("**Option 2: Upload Files**")
    uploaded_files = st.file_uploader(
        "Upload documents",
        accept_multiple_files=True,
        type=['pdf', 'xlsx', 'xls', 'docx', 'txt', 'md'],
        key="docs_upload"
    )

    if st.button("Start Indexing", type="primary"):
        if directory_path and Path(directory_path).exists():
            reindex_documents_from_directory(directory_path, recursive)
        elif uploaded_files:
            reindex_documents_from_upload(uploaded_files)
        else:
            st.error("Please provide a directory path or upload files")


def render_reindex_all_ui():
    """UI for re-indexing everything"""
    st.markdown("#### 🔄 Re-index Everything")

    st.warning("⚠️ This will clear all existing data and re-index from scratch")

    st.markdown("**Provide paths for each system (leave blank to skip):**")

    abinitio_path = st.text_input("Ab Initio Directory", placeholder="/path/to/abinitio or leave blank")
    autosys_path = st.text_input("Autosys Directory", placeholder="/path/to/autosys or leave blank")
    documents_path = st.text_input("Documents Directory", placeholder="/path/to/documents or leave blank")

    confirm = st.checkbox("Clear existing data and re-index")

    if st.button("Start Full Re-index", type="primary", disabled=not confirm):
        reindex_all(abinitio_path, autosys_path, documents_path)


def render_export_stats_ui():
    """UI for exporting statistics"""
    st.markdown("#### 📤 Export Statistics")

    st.info("Export database statistics to JSON file")

    if st.button("Export Stats", type="primary"):
        try:
            import json

            output = {
                "timestamp": datetime.now().isoformat(),
                "total_documents": sum(stats.get('total_documents', 0)
                                     for stats in st.session_state.stats.values()),
                "collections": st.session_state.stats
            }

            output_json = json.dumps(output, indent=2)

            st.download_button(
                label="Download JSON",
                data=output_json,
                file_name=f"vector_db_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )

            st.success("✓ Stats ready for download!")

        except Exception as e:
            st.error(f"Export error: {e}")


# ============================================
# AI-Powered Document Creation
# ============================================

def create_intelligent_document(
    process,
    components: List,
    system_type: str,
    ai_analyzer
) -> Dict[str, Any]:
    """
    Use AI to understand parsed data and create intelligent documents

    This is the key function that uses AI to:
    - Understand what the process does
    - Identify source-to-target mappings
    - Extract business logic
    - Understand data flow
    - Create comprehensive, searchable documents
    """

    # Extract all available attributes from the process object
    process_data = {}
    for attr in dir(process):
        if not attr.startswith('_'):
            try:
                value = getattr(process, attr)
                if not callable(value):
                    process_data[attr] = value
            except:
                pass

    # Build context from process and related components
    process_id = process_data.get('id', 'unknown')
    process_name = process_data.get('name', 'Unnamed Process')
    process_type = process_data.get('type', system_type)

    # Find related components
    related_components = []
    for comp in components:
        comp_data = {}
        for attr in dir(comp):
            if not attr.startswith('_'):
                try:
                    value = getattr(comp, attr)
                    if not callable(value):
                        comp_data[attr] = value
                except:
                    pass

        # Check if component belongs to this process
        comp_process_id = comp_data.get('process_id', comp_data.get('parent_id', ''))
        if process_id in str(comp_process_id):
            related_components.append(comp_data)

    # Create rich context for AI
    context = f"""
System: {system_type.upper()}
Process Name: {process_name}
Process ID: {process_id}
Process Type: {process_type}

Process Details:
{json.dumps(process_data, indent=2, default=str)}

Related Components ({len(related_components)}):
{json.dumps(related_components[:10], indent=2, default=str) if related_components else 'No components'}
"""

    # Use AI to understand and enrich the document
    ai_understanding = ""
    source_target_mapping = ""
    business_logic = ""
    data_flow = ""

    if ai_analyzer and ai_analyzer.enabled:
        try:
            # Ask AI to analyze the process
            analysis_result = ai_analyzer.analyze_with_context(
                query=f"""Analyze this {system_type} process and provide:
1. What does this process do? (1-2 sentences)
2. Source-to-Target mapping (identify inputs and outputs)
3. Key business logic and transformations
4. Data flow and dependencies
5. Important fields/columns being processed""",
                context=context[:8000]  # Limit context size
            )

            ai_understanding = analysis_result.get('analysis', analysis_result.get('response', ''))

            # Extract structured information if AI is available
            if "Source" in ai_understanding or "Target" in ai_understanding:
                source_target_mapping = ai_understanding

        except Exception as e:
            logger.warning(f"AI analysis failed for {process_name}: {e}")
            ai_understanding = "AI analysis not available"
    else:
        ai_understanding = "AI analysis not available (Azure OpenAI not configured)"

    # Build comprehensive document content
    content_parts = [
        f"# {process_name}",
        f"**System:** {system_type.upper()}",
        f"**Process ID:** {process_id}",
        f"**Type:** {process_type}",
        "",
        "## AI Understanding",
        ai_understanding,
        "",
        "## Process Structure"
    ]

    # Add description if available
    description = process_data.get('description') or process_data.get('doc_string')
    if description:
        content_parts.extend(["", "## Description", str(description)])

    # Add components summary
    if related_components:
        content_parts.extend([
            "",
            f"## Components ({len(related_components)})"
        ])
        for i, comp in enumerate(related_components[:5], 1):
            comp_name = comp.get('name', comp.get('component_name', f'Component {i}'))
            comp_type = comp.get('type', comp.get('component_type', 'unknown'))
            content_parts.append(f"- **{comp_name}** ({comp_type})")

    # Add raw data for searchability
    content_parts.extend([
        "",
        "## Technical Details",
        f"```json",
        json.dumps(process_data, indent=2, default=str)[:2000],  # Limit size
        "```"
    ])

    # Create the document
    document = {
        "id": process_id,
        "content": "\n".join(content_parts),
        "doc_type": f"{system_type}_workflow",
        "system": system_type,
        "metadata": {
            "process_name": process_name,
            "process_type": process_type,
            "process_id": process_id,
            "component_count": len(related_components),
            "has_ai_analysis": bool(ai_analyzer and ai_analyzer.enabled),
            "source_path": process_data.get('source_path', process_data.get('file_path', '')),
            # Add any other useful metadata
            **{k: v for k, v in process_data.items()
               if k in ['inputs', 'outputs', 'dependencies', 'tags'] and v}
        }
    }

    return document


def index_all_repository_files_with_ai(
    repository_path: str,
    system_type: str,
    ai_analyzer,
    sttm_generator: STTMGenerator,
    progress_bar=None,
    status_text=None,
    file_filter: List[str] = None,
    skip_json_export: bool = False
) -> Dict[str, Any]:
    """
    Deep repository indexing - Index ALL files (or filtered files) with AI understanding

    This addresses the user's requirement to index every script, not just workflows.
    When a workflow references a script (e.g., get_date.sh), this finds and indexes it too.

    Args:
        repository_path: Root directory of repository
        system_type: System type (hadoop, abinitio, databricks)
        ai_analyzer: AI analyzer for understanding scripts
        sttm_generator: STTM generator for creating mappings
        progress_bar: Streamlit progress bar
        status_text: Streamlit status text widget
        file_filter: Optional list of file name patterns to filter (for Ab Initio graphs)
        skip_json_export: If True, skip writing JSON files to disk (saves disk space)

    Returns:
        Dict with indexing statistics
    """
    # CRITICAL FIX: Use absolute paths to prevent Windows path issues and directory duplication
    repo_path = Path(repository_path).resolve()

    # Get absolute path to current working directory to create output directories
    import os
    current_dir = Path(os.getcwd()).resolve()

    # Define file extensions by system type
    file_extensions = {
        'hadoop': ['.pig', '.hql', '.xml', '.sh', '.py', '.properties', '.sql'],
        'abinitio': ['.mp', '.dml', '.ksh', '.sh', '.pl'],
        'databricks': ['.py', '.sql', '.scala', '.ipynb', '.sh', '.json']  # .json added for ADF pipelines
    }

    extensions = file_extensions.get(system_type, ['.sh', '.py', '.sql'])

    # Find all relevant files recursively
    all_files = []
    for ext in extensions:
        try:
            found_files = list(repo_path.rglob(f'*{ext}'))
            all_files.extend(found_files)
        except Exception as e:
            logger.warning(f"Error finding files with extension {ext}: {e}")
            continue

    # Apply file filter if provided (for Ab Initio graph filtering)
    if file_filter:
        filtered_files = []
        for file_path in all_files:
            # Check if file name (without extension) matches any filter pattern
            file_name_no_ext = file_path.stem
            if any(filter_pattern.lower() in file_name_no_ext.lower() for filter_pattern in file_filter):
                filtered_files.append(file_path)

        logger.info(f"Filtered from {len(all_files)} to {len(filtered_files)} files using {len(file_filter)} graph patterns")
        all_files = filtered_files

    total_files = len(all_files)

    if status_text:
        status_text.text(f"📁 Found {total_files} files to index with AI understanding...")

    documents = []
    sttm_mappings = []
    file_references = {}  # Track which files reference which other files

    # CRITICAL FIX: Create output directories using absolute paths
    output_base = (current_dir / "outputs" / "ai_enriched_docs" / system_type).resolve()
    sttm_output = (current_dir / "outputs" / "sttm_mappings" / system_type).resolve()

    # SAFETY CHECK: Verify paths are within expected directories
    if not str(output_base).startswith(str(current_dir)):
        raise ValueError(f"Security error: Output path {output_base} is outside working directory {current_dir}")
    if not str(sttm_output).startswith(str(current_dir)):
        raise ValueError(f"Security error: STTM path {sttm_output} is outside working directory {current_dir}")

    # SAFETY CHECK: Verify paths aren't too long for Windows (MAX_PATH = 260)
    if len(str(output_base)) > 240:
        logger.warning(f"Output path length {len(str(output_base))} may exceed Windows limits")
    if len(str(sttm_output)) > 240:
        logger.warning(f"STTM path length {len(str(sttm_output))} may exceed Windows limits")

    try:
        output_base.mkdir(parents=True, exist_ok=True)
        sttm_output.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Failed to create output directories: {e}", exc_info=True)
        raise

    logger.info(f"Output directory: {output_base}")
    logger.info(f"STTM directory: {sttm_output}")

    # Log what we're starting with
    logger.info(f"AI Analyzer enabled: {ai_analyzer.enabled if ai_analyzer else 'None'}")
    logger.info(f"STTM Generator available: {sttm_generator is not None}")

    # Create parsed_abinitio output folder for EnhancedAbInitioParser
    parsed_abinitio_folder = (current_dir / "outputs" / "parsed_abinitio").resolve()
    graphflow_folder = (current_dir / "outputs" / "graphflows").resolve()
    automation_sttm_folder = (current_dir / "outputs" / "sttm_automation").resolve()

    if system_type == "abinitio":
        try:
            parsed_abinitio_folder.mkdir(parents=True, exist_ok=True)
            graphflow_folder.mkdir(parents=True, exist_ok=True)
            automation_sttm_folder.mkdir(parents=True, exist_ok=True)
            logger.info(f"Parsed AbInitio output folder: {parsed_abinitio_folder}")
            logger.info(f"GraphFlow output folder: {graphflow_folder}")
            logger.info(f"STTM Automation output folder: {automation_sttm_folder}")
        except Exception as e:
            logger.warning(f"Could not create AbInitio output folders: {e}")

    # Initialize EnhancedAbInitioParser if available
    enhanced_parser = None
    graph_flow_generator = None
    automation_sttm_gen = None

    if system_type == "abinitio" and ENHANCED_PARSER_AVAILABLE:
        try:
            enhanced_parser = EnhancedAbInitioParser()
            logger.info("✅ EnhancedAbInitioParser initialized for structured component extraction")
        except Exception as e:
            logger.warning(f"Could not initialize EnhancedAbInitioParser: {e}")
            enhanced_parser = None

        # Initialize GraphFlow generator
        try:
            from parsers.abinitio.graph_flow import GraphFlowGenerator
            graph_flow_generator = GraphFlowGenerator()
            logger.info("✅ GraphFlowGenerator initialized for visualization")
        except Exception as e:
            logger.warning(f"Could not initialize GraphFlowGenerator: {e}")
            graph_flow_generator = None

        # Initialize VM_Automation STTM generator
        try:
            from parsers.abinitio.automation import AbInitioSTTMGenerator
            automation_sttm_gen = AbInitioSTTMGenerator(
                blade_path="Input Files/blade",
                output_folder=str(automation_sttm_folder),
                ai_analyzer=ai_analyzer
            )
            logger.info("✅ AbInitioSTTMGenerator initialized for STTM automation")
        except Exception as e:
            logger.warning(f"Could not initialize AbInitioSTTMGenerator: {e}")
            automation_sttm_gen = None

    # Index each file with AI understanding
    for idx, file_path in enumerate(all_files):
        try:
            if progress_bar:
                progress_bar.progress(int((idx / total_files) * 90))

            if status_text:
                relative_path = file_path.relative_to(repo_path)
                status_text.text(f"🤖 AI analyzing {idx+1}/{total_files}: {relative_path}")

            # Read file content
            # CRITICAL FIX: Convert Path to string and use resolve() to get absolute path
            try:
                abs_file_path = file_path.resolve()
                with open(str(abs_file_path), 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
            except Exception as e:
                logger.warning(f"Could not read {abs_file_path}: {e}")
                continue

            # Skip empty files
            if not content.strip():
                logger.debug(f"Skipping empty file: {file_path.name}")
                continue

            # CRITICAL FIX: Use EnhancedAbInitioParser for .mp files to extract structured components
            parsed_json_path = None
            parsed_components = None
            graphflow_excel_path = None
            automation_sttm_json = None
            automation_sttm_excel = None

            if system_type == "abinitio" and file_path.suffix.lower() == ".mp" and enhanced_parser:
                try:
                    output_filename = f"{file_path.stem}_components.json"
                    parsed_result = enhanced_parser.parse_mp_file(
                        file_path=str(abs_file_path),
                        output_folder=str(parsed_abinitio_folder),
                        output_filename=output_filename
                    )
                    parsed_json_path = str(parsed_abinitio_folder / output_filename)
                    parsed_components = parsed_result

                    # Log parsing success
                    vertices = len(parsed_result.get('vertices', {}))
                    flows = len(parsed_result.get('flows', {}))
                    ports = len(parsed_result.get('ports', {}))
                    logger.info(f"✅ Parsed {file_path.name}: {vertices} vertices, {flows} flows, {ports} ports")
                    logger.info(f"   Saved to: {parsed_json_path}")

                    # Generate GraphFlow visualization
                    graphflow_excel_path = None
                    if graph_flow_generator and parsed_json_path:
                        try:
                            graphflow_result = graph_flow_generator.generate_from_parsed_json(
                                parsed_json_path=parsed_json_path,
                                output_folder=str(graphflow_folder),
                                base_filename=file_path.stem
                            )
                            graphflow_excel_path = graphflow_result.get('excel_file')
                            if graphflow_excel_path:
                                logger.info(f"📊 Generated GraphFlow: {graphflow_excel_path}")
                        except Exception as e:
                            logger.warning(f"GraphFlow generation failed for {file_path.name}: {e}")

                    # Generate STTM using VM_Automation
                    automation_sttm_json = None
                    automation_sttm_excel = None
                    if automation_sttm_gen and parsed_json_path:
                        try:
                            sttm_result = automation_sttm_gen.generate_sttm_from_parsed_json(
                                parsed_json_path=parsed_json_path
                            )
                            if sttm_result.get('success'):
                                automation_sttm_json = sttm_result.get('mapping_json')
                                automation_sttm_excel = sttm_result.get('excel_file')
                                if automation_sttm_excel:
                                    logger.info(f"📋 Generated STTM: {automation_sttm_excel}")
                        except Exception as e:
                            logger.warning(f"STTM generation failed for {file_path.name}: {e}")

                except Exception as e:
                    logger.warning(f"EnhancedAbInitioParser failed for {file_path.name}: {e}")
                    parsed_json_path = None
                    parsed_components = None
                    graphflow_excel_path = None
                    automation_sttm_json = None
                    automation_sttm_excel = None

            # Extract file references (scripts calling other scripts)
            referenced_files = _extract_file_references(content, system_type)
            if referenced_files:
                file_references[str(file_path)] = referenced_files

            # Use AI to understand the file
            ai_understanding = ""
            if ai_analyzer and ai_analyzer.enabled:
                try:
                    analysis_result = ai_analyzer.analyze_with_context(
                        query=f"""Analyze this {system_type} script and provide:
1. What does this script do? (1-2 sentences)
2. Input sources (tables, files, databases)
3. Output targets (tables, files, databases)
4. Key transformations and business logic
5. Dependencies on other scripts/files
6. Data fields being processed""",
                        context=f"""
File: {file_path.name}
Path: {file_path.relative_to(repo_path)}
Type: {file_path.suffix}
System: {system_type}

Content (first 8000 chars):
{content[:8000]}
"""
                    )
                    ai_understanding = analysis_result.get('analysis', analysis_result.get('response', ''))
                except Exception as e:
                    logger.warning(f"AI analysis failed for {file_path.name}: {e}")
                    ai_understanding = "AI analysis not available"
            else:
                ai_understanding = "AI analysis not available (Azure OpenAI not configured)"

            # Create comprehensive document
            file_id = f"{system_type}_{file_path.stem}_{hash(str(file_path)) % 100000}"

            # CRITICAL FIX: Truncate very large content to prevent ChromaDB SQLite size limits
            # ChromaDB's underlying SQLite database has size limits (typically ~2GB)
            # Large Ab Initio files can be 500KB+, causing "database or disk is full" errors
            MAX_CONTENT_SIZE = 50000  # 50KB max content in vector DB (still searchable)

            full_content = content
            if len(content) > MAX_CONTENT_SIZE:
                logger.warning(f"⚠️ Truncating RAW mp file content for {file_path.name} from {len(content)} to {MAX_CONTENT_SIZE} chars")
                logger.info(f"   ℹ️  Note: PARSED components (vertices/flows/ports) are FULLY embedded, only raw mp content truncated")
                content_for_db = content[:MAX_CONTENT_SIZE] + "\n\n... (content truncated for database storage, full content available in file)"
            else:
                content_for_db = content

            # Build parsed components section for AbInitio mp files
            parsed_components_section = ""
            if parsed_components and system_type == "abinitio":
                vertices = parsed_components.get('vertices', {})
                flows = parsed_components.get('flows', {})
                ports = parsed_components.get('ports', {})
                graphs = parsed_components.get('graphs', {})

                # Create comprehensive summary with ALL components (not just samples)
                parsed_components_section = f"""
## Parsed Components (EnhancedAbInitioParser)
**Total Vertices:** {len(vertices)}
**Total Flows:** {len(flows)}
**Total Ports:** {len(ports)}
**Total Graphs:** {len(graphs)}
**Parsed JSON:** {parsed_json_path}

### All Vertices (Complete List):
"""
                # List ALL vertices with full details
                for i, (vid, vdata) in enumerate(vertices.items(), 1):
                    vname = vdata.get('name', 'Unknown')
                    vtype = vdata.get('type', 'Unknown')
                    vattrs = vdata.get('attributes', {})

                    # Extract key attributes
                    layout = vattrs.get('layout', '')
                    dml = vattrs.get('dml', '')
                    if dml and len(dml) > 100:
                        dml = dml[:100] + "..."

                    parsed_components_section += f"  {i}. **{vname}** (type: {vtype}, id: {vid})\n"
                    if layout:
                        parsed_components_section += f"     - Layout: {layout}\n"
                    if dml:
                        parsed_components_section += f"     - DML: {dml}\n"

                # List ALL flows with connection details
                if flows:
                    parsed_components_section += f"\n### All Flows (Complete List - {len(flows)} connections):\n"
                    for i, (fid, fdata) in enumerate(flows.items(), 1):
                        from_port = fdata.get('from_port', 'Unknown')
                        to_port = fdata.get('to_port', 'Unknown')
                        from_vertex = fdata.get('from_vertex', '')
                        to_vertex = fdata.get('to_vertex', '')
                        parsed_components_section += f"  {i}. {from_vertex}:{from_port} → {to_vertex}:{to_port}\n"

                # List ALL ports with details
                if ports:
                    parsed_components_section += f"\n### All Ports (Complete List - {len(ports)} ports):\n"
                    for i, (pid, pdata) in enumerate(ports.items(), 1):
                        pname = pdata.get('name', 'Unknown')
                        ptype = pdata.get('type', 'Unknown')
                        vertex = pdata.get('vertex', '')
                        parsed_components_section += f"  {i}. {pname} (type: {ptype}, vertex: {vertex})\n"

            # Build external artifacts section
            external_artifacts_section = ""
            if graphflow_excel_path or automation_sttm_excel:
                external_artifacts_section = "\n## Generated Artifacts\n"
                if graphflow_excel_path:
                    external_artifacts_section += f"- **GraphFlow Excel:** {graphflow_excel_path}\n"
                if automation_sttm_json:
                    external_artifacts_section += f"- **STTM JSON:** {automation_sttm_json}\n"
                if automation_sttm_excel:
                    external_artifacts_section += f"- **STTM Excel:** {automation_sttm_excel}\n"

            doc_content = f"""# {file_path.name}

**System:** {system_type.upper()}
**Path:** {file_path.relative_to(repo_path)}
**Type:** {file_path.suffix}
**Size:** {len(full_content)} characters (DB stores {len(content_for_db)} chars)

## AI Understanding
{ai_understanding}
{parsed_components_section}{external_artifacts_section}
## File References
{', '.join(referenced_files) if referenced_files else 'No external references found'}

## Code Content (Preview)
```
{full_content[:2000]}
...
```

## Searchable Content
{content_for_db}
"""

            # Build metadata with parsed_json_path for AbInitio
            metadata = {
                "file_name": file_path.name,
                "file_path": str(file_path.relative_to(repo_path)),
                "absolute_file_path": str(file_path),  # ADDED: Full path for agents to read actual files
                "file_type": file_path.suffix,
                "has_ai_analysis": bool(ai_analyzer and ai_analyzer.enabled),
                "references_count": len(referenced_files),
                "file_size": len(full_content),  # Original file size
                "content_truncated": len(full_content) > MAX_CONTENT_SIZE
            }

            # Add parsed_json_path for AbInitio mp files
            if parsed_json_path:
                metadata["parsed_json_path"] = parsed_json_path
                metadata["vertices_count"] = len(parsed_components.get('vertices', {})) if parsed_components else 0
                metadata["flows_count"] = len(parsed_components.get('flows', {})) if parsed_components else 0
                metadata["ports_count"] = len(parsed_components.get('ports', {})) if parsed_components else 0

                # Log what's being embedded
                total_embedded_size = len(doc_content)
                parsed_section_size = len(parsed_components_section)
                logger.info(f"   📦 Embedded in vector DB: {metadata['vertices_count']} vertices, {metadata['flows_count']} flows, {metadata['ports_count']} ports")
                logger.info(f"   📏 Document size: {total_embedded_size:,} chars (parsed components: {parsed_section_size:,} chars)")

            # Add external artifact paths to metadata
            if graphflow_excel_path:
                metadata["graphflow_excel"] = graphflow_excel_path
            if automation_sttm_json:
                metadata["automation_sttm_json"] = automation_sttm_json
            if automation_sttm_excel:
                metadata["automation_sttm_excel"] = automation_sttm_excel

            document = {
                "id": file_id,
                "content": doc_content,
                "doc_type": f"{system_type}_script",
                "system": system_type,
                "title": f"Script: {file_path.name}",
                "metadata": metadata
            }

            documents.append(document)
            logger.debug(f"✓ Created document for {file_path.name} (total: {len(documents)})")

            # Save document to disk (OPTIONAL - if disk space available)
            # Can be disabled with skip_json_export=True to save disk space
            if not skip_json_export:
                # CRITICAL FIX: Resolve path to absolute and convert to string
                # SAFETY: Truncate long filenames to prevent Windows path length issues
                safe_filename = file_path.stem[:100] if len(file_path.stem) > 100 else file_path.stem
                doc_file = (output_base / f"{safe_filename}_{hash(str(file_path)) % 10000}.json").resolve()

                # SAFETY CHECK: Verify the file path length
                if len(str(doc_file)) > 250:
                    logger.warning(f"File path too long ({len(str(doc_file))} chars), using shorter name")
                    doc_file = (output_base / f"{hash(str(file_path)) % 100000}.json").resolve()

                try:
                    with open(str(doc_file), 'w', encoding='utf-8') as f:
                        json.dump(document, f, indent=2, default=str)
                except OSError as e:
                    # Handle disk full errors gracefully
                    if e.errno == 28:  # No space left on device
                        logger.warning(f"⚠️ Disk full - skipping JSON file write for {file_path.name}")
                        logger.warning(f"💡 Document will still be indexed to vector DB (which is what matters)")
                        # Don't use 'continue' - we still want to process STTM and continue indexing
                    else:
                        logger.error(f"Failed to write document file {doc_file}: {e}")
                except Exception as e:
                    logger.error(f"Failed to write document file {doc_file}: {e}")

            # Generate STTM mappings if possible
            if sttm_generator:
                try:
                    file_sttm = _generate_sttm_from_script(
                        file_path=file_path,
                        content=content,
                        ai_understanding=ai_understanding,
                        system_type=system_type,
                        sttm_generator=sttm_generator
                    )
                    sttm_mappings.extend(file_sttm)
                except Exception as e:
                    logger.warning(f"STTM generation failed for {file_path.name}: {e}")

        except Exception as e:
            logger.error(f"❌ Error processing {file_path}: {e}", exc_info=True)
            continue

    # Log summary
    logger.info(f"Finished processing {total_files} files, created {len(documents)} documents")

    # Save STTM mappings (OPTIONAL - if disk space available)
    if sttm_mappings:
        # CRITICAL FIX: Resolve paths to absolute and convert to string
        sttm_file = (sttm_output / f"sttm_mappings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json").resolve()
        try:
            sttm_generator.export_to_json(sttm_mappings, str(sttm_file))
            logger.info(f"Successfully saved STTM JSON to {sttm_file}")
        except OSError as e:
            if e.errno == 28:  # No space left on device
                logger.warning(f"⚠️ Disk full - skipping STTM JSON export")
                logger.info(f"💡 STTM mappings generated but not saved to disk (disk full)")
            else:
                logger.error(f"Failed to save STTM JSON to {sttm_file}: {e}")
        except Exception as e:
            logger.error(f"Failed to save STTM JSON to {sttm_file}: {e}")

        sttm_excel = (sttm_output / f"sttm_mappings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx").resolve()
        try:
            sttm_generator.export_to_excel(sttm_mappings, str(sttm_excel))
            logger.info(f"Successfully saved STTM Excel to {sttm_excel}")
        except OSError as e:
            if e.errno == 28:  # No space left on device
                logger.warning(f"⚠️ Disk full - skipping STTM Excel export")
            else:
                logger.error(f"Failed to save STTM Excel to {sttm_excel}: {e}")
        except Exception as e:
            logger.error(f"Failed to save STTM Excel to {sttm_excel}: {e}")

        if status_text:
            status_text.text(f"✓ Generated {len(sttm_mappings)} STTM mappings")

    # Save file references map
    # CRITICAL FIX: Resolve path to absolute and convert to string
    references_file = (output_base / "file_references_map.json").resolve()
    try:
        with open(str(references_file), 'w', encoding='utf-8') as f:
            json.dump(file_references, f, indent=2)
        logger.info(f"Successfully saved file references map to {references_file}")
    except Exception as e:
        logger.error(f"Failed to write file references map to {references_file}: {e}", exc_info=True)

    return {
        "total_files": total_files,
        "documents_created": len(documents),
        "sttm_mappings": len(sttm_mappings),
        "documents": documents,
        "file_references": file_references,
        "output_dir": str(output_base),
        "sttm_dir": str(sttm_output) if sttm_mappings else None
    }


def _extract_file_references(content: str, system_type: str) -> List[str]:
    """Extract references to other files from script content"""
    references = []

    # Common patterns for file references
    patterns = [
        r'source\s+([^\s;]+\.sh)',  # source script.sh
        r'\./([^\s;]+\.sh)',  # ./script.sh
        r'bash\s+([^\s;]+\.sh)',  # bash script.sh
        r'sh\s+([^\s;]+\.sh)',  # sh script.sh
        r'python\s+([^\s;]+\.py)',  # python script.py
        r'pig\s+-f\s+([^\s;]+\.pig)',  # pig -f script.pig
        r'hive\s+-f\s+([^\s;]+\.hql)',  # hive -f script.hql
        r'spark-submit\s+([^\s;]+\.py)',  # spark-submit script.py
        r'"([^"]+\.(?:sh|py|pig|hql|sql))"',  # "script.sh"
        r"'([^']+\.(?:sh|py|pig|hql|sql))'",  # 'script.sh'
    ]

    for pattern in patterns:
        matches = re.findall(pattern, content)
        references.extend(matches)

    return list(set(references))


def _generate_sttm_from_script(
    file_path: Path,
    content: str,
    ai_understanding: str,
    system_type: str,
    sttm_generator: STTMGenerator
) -> List:
    """Generate STTM mappings from a script using AI understanding"""
    import re

    mappings = []

    # Extract table/field patterns based on system type
    if system_type == 'hadoop':
        # Look for INSERT INTO, CREATE TABLE, etc.
        insert_pattern = r'INSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?(\w+)'
        create_pattern = r'CREATE\s+(?:TABLE|EXTERNAL\s+TABLE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)'

        target_tables = set()
        target_tables.update(re.findall(insert_pattern, content, re.IGNORECASE))
        target_tables.update(re.findall(create_pattern, content, re.IGNORECASE))

        # Look for column definitions
        col_pattern = r'(\w+)\s+(?:STRING|INT|BIGINT|DOUBLE|FLOAT|BOOLEAN|DATE|TIMESTAMP)'
        columns = re.findall(col_pattern, content, re.IGNORECASE)

        # Create simple STTM mappings
        for table in target_tables:
            for idx, col in enumerate(columns[:20]):  # Limit to first 20 columns
                from services.lineage.sttm_generator import STTMMapping

                mapping = STTMMapping(
                    id=f"{file_path.stem}_{table}_{col}_{idx}",
                    partner="default",
                    schema="default",
                    target_table_name=table,
                    target_field_name=col,
                    target_field_data_type="string",
                    is_primary_key=False,
                    contains_pii=sttm_generator._detect_pii(col, ""),
                    field_type=sttm_generator._classify_field_type(col, content),
                    field_depends_on=[],
                    processing_order=idx,
                    pre_processing_rules=[],
                    source_field_names=[col],
                    source_dataset_name="unknown",
                    field_definition=f"Field from {file_path.name}",
                    transformation_logic=f"AI Understanding: {ai_understanding[:200]}",
                    system_type=system_type,
                    graph_name=file_path.stem,
                    component_name=file_path.name,
                    created_at=datetime.now().isoformat(),
                    confidence_score=0.70,
                    ai_reasoning=ai_understanding[:500]
                )
                mappings.append(mapping)

    return mappings


# ============================================
# Indexing Helper Functions
# ============================================

def reindex_abinitio_from_directory(directory_path: str):
    """
    Re-index Ab Initio with DEEP AI-powered understanding

    IMPORTANT: Only indexes the 36 FILTERED graphs specified in FILTERED_ABINITIO_GRAPHS.
    This filters from 1750+ files to only relevant graph files.
    Generates STTM mappings and tracks dependencies.
    """
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # Initialize AI analyzer if not already done
        if 'ai_analyzer' not in st.session_state or st.session_state.ai_analyzer is None:
            st.session_state.ai_analyzer = AIScriptAnalyzer()
            logger.info("✓ AI Script Analyzer initialized")

        # Initialize STTM generator if not already done
        if 'sttm_generator' not in st.session_state:
            st.session_state.sttm_generator = STTMGenerator(ai_analyzer=st.session_state.ai_analyzer)
            logger.info("✓ STTM Generator initialized")

        # Check if filter list is populated
        if not FILTERED_ABINITIO_GRAPHS:
            st.warning("⚠️ FILTERED_ABINITIO_GRAPHS is empty! Please add your 36 graph names to stag_app.py")
            st.info("Without filtering, ALL files in the directory will be indexed.")

        status_text.text(f"🔍 Scanning Ab Initio repository (filtering to {len(FILTERED_ABINITIO_GRAPHS)} graphs)...")
        progress_bar.progress(10)

        # Use deep indexing with graph filtering
        # DISK SPACE OPTIMIZATION: Skip JSON export for Ab Initio (large files)
        # Vector DB is what matters for search/chat/lineage - JSON files are just backups
        result = index_all_repository_files_with_ai(
            repository_path=directory_path,
            system_type="abinitio",
            ai_analyzer=st.session_state.ai_analyzer,
            sttm_generator=st.session_state.sttm_generator,
            progress_bar=progress_bar,
            status_text=status_text,
            file_filter=FILTERED_ABINITIO_GRAPHS if FILTERED_ABINITIO_GRAPHS else None,
            skip_json_export=True  # Skip JSON files to prevent disk full errors
        )

        # Index all documents to vector database
        if result["documents"]:
            status_text.text("💾 Indexing to vector database...")
            progress_bar.progress(95)
            st.session_state.indexer.collections["abinitio_collection"].index_documents(result["documents"])

        progress_bar.progress(100)
        status_text.empty()

        # Display results
        st.success(f"✅ **Deep Indexing Complete!**")
        st.info(f"""
**Statistics:**
- 📁 Total files scanned: {result['total_files']}
- 📝 Documents created: {result['documents_created']}
- 🎯 STTM mappings generated: {result['sttm_mappings']}
- 🔗 File dependencies tracked: {len(result['file_references'])}

**Output Locations:**
- AI-enriched documents: `{result['output_dir']}`
- STTM mappings: `{result['sttm_dir'] or 'No mappings generated'}`
- File references map: `{result['output_dir']}/file_references_map.json`
        """)

        # Refresh stats
        st.session_state.stats = st.session_state.indexer.get_stats()
        st.session_state.indexed_files['abinitio'].append(f"Directory: {directory_path} (DEEP)")

        # Initialize workflow intelligence after successful indexing (NEW)
        if 'workflow_intelligence' not in st.session_state:
            try:
                with st.spinner("🧠 Loading workflow intelligence..."):
                    intelligence = STAGWorkflowIntelligence()

                    # Load workflow intelligence
                    intelligence.load_workflow_intelligence(
                        hadoop_repo_path="./hadoop_repos/hadoop_repos",
                        databricks_analysis_file="databricks_pipeline_analysis.json",
                        abinitio_mappings_file="abinitio_graph_mappings.json"
                    )

                    st.session_state['workflow_intelligence'] = intelligence

                    # Update orchestrator with workflow intelligence
                    if st.session_state.chat_orchestrator:
                        st.session_state.chat_orchestrator.workflow_intelligence = intelligence
                        logger.info("✓ Workflow intelligence passed to chat orchestrator")

                    st.success("✅ Workflow intelligence loaded! Chat can now answer workflow mapping questions.")
            except Exception as e:
                logger.warning(f"Could not load workflow intelligence: {e}")
                st.warning("⚠️ Workflow intelligence not loaded - some chat features may be limited")

    except Exception as e:
        st.error(f"Error during deep indexing: {e}")
        logger.error(f"Ab Initio deep indexing error: {e}", exc_info=True)
    finally:
        progress_bar.empty()


def _find_vm_fawn_excel_for_mp(mp_file_path: Path) -> Optional[Path]:
    """
    Find VM_FAWN generated Excel file for a given .mp file

    Search pattern: Input Files/VM_FAWN/bbi_preprocessing_output/{filename}_*.xlsx

    Args:
        mp_file_path: Path to .mp file

    Returns:
        Path to VM_FAWN Excel if found, None otherwise
    """
    try:
        mp_basename = mp_file_path.stem  # e.g., "265_fileTransferToHadoopServer"

        # Search in VM_FAWN output folder
        vm_fawn_folder = Path("Input Files/VM_FAWN/bbi_preprocessing_output")

        if not vm_fawn_folder.exists():
            return None

        # Find Excel files matching pattern: {mp_basename}_*.xlsx
        matching_files = list(vm_fawn_folder.glob(f"{mp_basename}_*.xlsx"))

        if matching_files:
            # Return most recent file (by modification time)
            latest_file = max(matching_files, key=lambda p: p.stat().st_mtime)
            return latest_file

        return None

    except Exception as e:
        logger.warning(f"Error finding VM_FAWN Excel for {mp_file_path.name}: {e}")
        return None


def reindex_single_abinitio_graph(graph_file_path: str, generate_graphflow: bool = True, generate_sttm: bool = True):
    """
    Index a single Ab Initio graph with configurable pipeline:
    Parse → [GraphFlow] → [STTM] → Vector DB

    Args:
        graph_file_path: Path to .mp file
        generate_graphflow: If True, generate GraphFlow Excel (default: True)
        generate_sttm: If False, skip STTM generation for large graphs (default: True)

    Perfect for focused testing, demos, or priority graphs with rate limit handling.
    """
    import time

    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        graph_path = Path(graph_file_path)
        if not graph_path.exists():
            st.error(f"❌ File not found: {graph_file_path}")
            return

        base_filename = graph_path.stem

        # Initialize components
        status_text.text("🔧 Initializing AI components...")
        progress_bar.progress(5)

        if 'ai_analyzer' not in st.session_state or st.session_state.ai_analyzer is None:
            st.session_state.ai_analyzer = AIScriptAnalyzer()

        if not st.session_state.ai_analyzer.enabled:
            st.error("❌ AI Script Analyzer is disabled. Please configure Azure OpenAI credentials in .env")
            return

        # Get absolute path
        import os
        current_dir = Path(os.getcwd()).resolve()

        # Create output folders
        parsed_folder = (current_dir / "outputs" / "parsed_abinitio").resolve()
        graphflow_folder = (current_dir / "outputs" / "graphflows").resolve()
        sttm_folder = (current_dir / "outputs" / "sttm_automation").resolve()

        parsed_folder.mkdir(parents=True, exist_ok=True)
        graphflow_folder.mkdir(parents=True, exist_ok=True)
        sttm_folder.mkdir(parents=True, exist_ok=True)

        # STEP 1: Automated VM_FAWN → Enhanced JSON Pipeline
        status_text.text(f"📋 Step 1a: Running VM_FAWN parser (automated)...")
        progress_bar.progress(10)

        # Try to run VM_FAWN automatically
        vm_fawn_excel = None
        try:
            from parsers.abinitio.vm_fawn_auto_runner import VMFAWNAutoRunner

            st.info(f"🤖 Running VM_FAWN parser automatically...")
            runner = VMFAWNAutoRunner()
            vm_fawn_excel = runner.run_vm_fawn(str(graph_path))

            if vm_fawn_excel:
                st.success(f"✅ VM_FAWN generated: {vm_fawn_excel.name}")
            else:
                st.warning(f"⚠️ VM_FAWN execution failed")

        except Exception as e:
            logger.warning(f"VM_FAWN auto-run failed: {e}")
            st.warning(f"⚠️ VM_FAWN auto-run error: {str(e)[:100]}")

        # If auto-run failed, check for existing VM_FAWN Excel
        if not vm_fawn_excel:
            status_text.text(f"📋 Step 1b: Checking for existing VM_FAWN Excel...")
            vm_fawn_excel = _find_vm_fawn_excel_for_mp(graph_path)

            if vm_fawn_excel:
                st.info(f"📊 Found existing VM_FAWN Excel: {vm_fawn_excel.name}")

        # Convert VM_FAWN Excel to Enhanced JSON (if available)
        if vm_fawn_excel:
            status_text.text(f"📋 Step 1c: Converting VM_FAWN Excel + Enhanced Parser (hybrid)...")
            progress_bar.progress(15)

            # First: Get component details from VM_FAWN
            from parsers.abinitio.vm_fawn_excel_converter import VMFAWNExcelConverter
            converter = VMFAWNExcelConverter()

            parsed_json_path = parsed_folder / f"{base_filename}_components.json"

            vm_fawn_result = converter.convert_excel_to_json(
                vm_fawn_excel_path=str(vm_fawn_excel),
                source_mp_file_path=str(graph_path),
                output_json_path=None  # Don't save yet
            )

            # Second: Get flows and ports from Enhanced Parser
            from parsers.abinitio.enhanced_parser import EnhancedAbInitioParser
            enhanced_parser = EnhancedAbInitioParser()

            # Generate Enhanced Parser output to temp file
            enhanced_json_path = enhanced_parser.parse_mp_file(
                file_path=str(graph_path),
                output_folder=str(parsed_folder),
                output_filename=f"{base_filename}_enhanced_temp.json"
            )

            # Load the Enhanced Parser result
            import json
            if enhanced_json_path and Path(enhanced_json_path).exists():
                with open(enhanced_json_path, 'r', encoding='utf-8') as f:
                    enhanced_result = json.load(f)
            else:
                logger.warning("Enhanced Parser did not generate output, using empty flows/ports")
                enhanced_result = {'flows': {}, 'ports': {}}

            # Merge: VM_FAWN vertices + Enhanced Parser flows/ports
            parsed_result = {
                'metadata': vm_fawn_result.get('metadata', {}),
                'raw_content': vm_fawn_result.get('raw_content', ''),
                'vertices': vm_fawn_result.get('vertices', {}),  # From VM_FAWN (detailed)
                'flows': enhanced_result.get('flows', {}),       # From Enhanced Parser (connected)
                'ports': enhanced_result.get('ports', {}),       # From Enhanced Parser (complete)
                'graphs': vm_fawn_result.get('graphs', {}),
                'summary': {
                    'total_vertices': len(vm_fawn_result.get('vertices', {})),
                    'total_flows': len(enhanced_result.get('flows', {})),
                    'total_ports': len(enhanced_result.get('ports', {})),
                    'total_graphs': len(vm_fawn_result.get('graphs', {})),
                    'components_with_transforms': vm_fawn_result.get('summary', {}).get('components_with_transforms', 0),
                    'datasets_count': vm_fawn_result.get('summary', {}).get('datasets_count', 0)
                }
            }

            # Save merged result
            import json
            with open(parsed_json_path, 'w', encoding='utf-8') as f:
                json.dump(parsed_result, f, indent=2)

            st.success(f"✅ Hybrid: VM_FAWN components ({len(parsed_result['vertices'])}) + Enhanced Parser flows ({len(parsed_result['flows'])}) & ports ({len(parsed_result['ports'])})")

        else:
            # Fallback to Enhanced Parser
            st.info(f"📋 Falling back to Enhanced Parser (basic extraction)")
            status_text.text(f"📋 Step 1c: Parsing with Enhanced Parser...")
            progress_bar.progress(15)

            from parsers.abinitio.enhanced_parser import EnhancedAbInitioParser
            parser = EnhancedAbInitioParser()

            parsed_result = parser.parse_mp_file(
                file_path=str(graph_path),
                output_folder=str(parsed_folder),
                output_filename=f"{base_filename}_components.json"
            )

            parsed_json_path = parsed_folder / f"{base_filename}_components.json"

        vertex_count = len(parsed_result.get('vertices', {}))
        st.success(f"✅ Step 1 Complete: Parsed {vertex_count} vertices, {len(parsed_result.get('flows', {}))} flows")

        # Warn if large graph
        if vertex_count > 200:
            st.warning(f"⚠️ Large graph with {vertex_count} vertices - may consume many tokens and hit rate limits")
            if generate_sttm:
                st.info("💡 Consider unchecking 'Generate STTM' option to avoid rate limits for this large graph")

        # Initialize result variables
        graphflow_result = {'success': False}
        sttm_success = False
        sttm_result = {}

        # STEP 2: Generate GraphFlow Excel (if enabled)
        if generate_graphflow:
            status_text.text(f"📊 Step 2: Generating GraphFlow Excel visualization...")
            progress_bar.progress(35)

            from parsers.abinitio.graph_flow.excel_generator import GraphFlowExcelGenerator
            graphflow_gen = GraphFlowExcelGenerator()

            graphflow_result = graphflow_gen.generate_from_parsed_json(
                parsed_json_path=str(parsed_json_path),
                output_folder=str(graphflow_folder),
                base_filename=base_filename
            )

            if graphflow_result['success']:
                st.success(f"✅ GraphFlow Excel → {graphflow_result['excel_file']}")
            else:
                st.warning(f"⚠️ GraphFlow generation failed: {graphflow_result.get('error')}")
        else:
            st.info("⏭️ Skipping GraphFlow Excel generation")

        # STEP 3: Generate STTM with retry handling (if enabled)
        if generate_sttm:
            status_text.text(f"📋 Step 3: Generating STTM with enhanced pipeline (with rate limit retry)...")
            progress_bar.progress(50)

            from parsers.abinitio.automation.abinitio_sttm_generator import AbInitioSTTMGenerator

            sttm_gen = AbInitioSTTMGenerator(
                blade_path="Input Files/blade/dml",
                output_folder=str(sttm_folder),
                ai_analyzer=st.session_state.ai_analyzer
            )

            # Retry logic for rate limits
            max_retries = 3

            for attempt in range(max_retries):
                try:
                    st.info(f"   Attempt {attempt + 1}/{max_retries}...")

                    sttm_result = sttm_gen.generate_sttm_from_parsed_json(
                        parsed_json_path=str(parsed_json_path)
                    )

                    if sttm_result['success']:
                        sttm_success = True
                        st.success(f"✅ STTM generated → {sttm_result.get('excel_file')}")

                        # Check outputs
                        if sttm_result.get('json_file') and Path(sttm_result['json_file']).exists():
                            import json
                            with open(sttm_result['json_file'], 'r') as f:
                                sttm_data = json.load(f)
                                outputs = sttm_data.get('outputs', [])
                                if len(outputs) > 0:
                                    st.success(f"   🎯 Identified {len(outputs)} output(s)!")
                                else:
                                    st.warning("   ⚠️ No outputs identified")
                        break
                    else:
                        st.error(f"❌ STTM generation failed: {sttm_result.get('error')}")
                        break

                except Exception as e:
                    error_str = str(e)

                    # Check for rate limit
                    if "429" in error_str or "RateLimitReached" in error_str:
                        if attempt < max_retries - 1:
                            wait_time = 60 * (2 ** attempt)  # 60s, 120s, 240s
                            st.warning(f"   ⚠️ Rate limit hit! Waiting {wait_time}s before retry...")
                            time.sleep(wait_time)
                        else:
                            st.error("   ❌ Rate limit exceeded after 3 retries. Try again later.")
                            break
                    else:
                        st.error(f"   ❌ Error: {e}")
                        logger.error(f"STTM generation error: {e}", exc_info=True)
                        break
        else:
            st.info("⏭️ Skipping STTM generation (unchecked for large graph)")

        # STEP 4: Index in Vector DB
        status_text.text(f"💾 Step 4/4: Indexing in vector database...")
        progress_bar.progress(75)

        # Build document content with full details
        raw_content = parsed_result.get('raw_content', '')
        vertices = parsed_result.get('vertices', {})
        flows = parsed_result.get('flows', {})
        ports = parsed_result.get('ports', {})

        # CRITICAL FIX: Embed ALL parsed components (not just first 50!)
        # Strategy: Include ALL vertices/flows/ports names + types, skip raw mp content
        # This ensures STAG can analyze the full graph structure even for 1295 vertices

        content_parts = [
            f"# Ab Initio Graph: {base_filename}\n",
            f"**File**: {graph_path.name}\n",
            f"**Vertices**: {len(vertices)}, **Flows**: {len(flows)}, **Ports**: {len(ports)}\n\n"
        ]

        # Add ALL vertex details (names + types only, no raw content)
        content_parts.append(f"## All Vertices ({len(vertices)} components):\n")
        for vid, vdata in vertices.items():
            vname = vdata.get('name', 'Unknown')
            vtype = vdata.get('component_type', vdata.get('type', 'Unknown'))
            vattrs = vdata.get('attributes', {})

            # Extract key attributes (DML, layout)
            dml = vattrs.get('dml', '')
            layout = vattrs.get('layout', '')

            content_parts.append(f"- **{vname}** (type: {vtype}, id: {vid})")
            if dml:
                content_parts.append(f" - DML: {dml[:100]}")
            if layout:
                content_parts.append(f" - Layout: {layout[:100]}")
            content_parts.append("\n")

        # Add ALL flow details (data lineage)
        content_parts.append(f"\n## All Flows ({len(flows)} connections):\n")
        for fid, fdata in flows.items():
            from_vertex = fdata.get('from_vertex', 'Unknown')
            to_vertex = fdata.get('to_vertex', 'Unknown')
            from_port = fdata.get('from_port', '')
            to_port = fdata.get('to_port', '')
            content_parts.append(f"- {from_vertex}:{from_port} → {to_vertex}:{to_port}\n")

        # Add port details summary
        if ports:
            input_ports = [p for p in ports.values() if p.get('type') == 'input']
            output_ports = [p for p in ports.values() if p.get('type') == 'output']
            content_parts.append(f"\n## Ports Summary:\n")
            content_parts.append(f"- Input ports: {len(input_ports)}\n")
            content_parts.append(f"- Output ports: {len(output_ports)}\n")

        # Add metadata references
        content_parts.append(f"\n## Generated Artifacts:\n")
        content_parts.append(f"- **Parsed JSON**: {parsed_json_path}\n")
        if graphflow_result.get('success'):
            content_parts.append(f"- **GraphFlow Excel**: {graphflow_result['excel_file']}\n")
        if sttm_success and sttm_result.get('excel_file'):
            content_parts.append(f"- **STTM Excel**: {sttm_result['excel_file']}\n")

        # Add MINIMAL raw content (for context only, not analysis)
        content_parts.append(f"\n## Graph Header (first 2000 chars):\n")
        content_parts.append(f"```\n{raw_content[:2000]}\n```\n")

        content_parts.append(f"\n**Note**: Full parsed components available at: {parsed_json_path}\n")
        content_parts.append(f"**Graph structure**: {len(vertices)} vertices with {len(flows)} data flows\n")

        final_content = ''.join(content_parts)

        # Create document as dictionary (same format as index_all_repository_files_with_ai)
        doc = {
            "id": f"abinitio_{hash(str(graph_path))}",
            "content": final_content,
            "doc_type": "abinitio_script",
            "system": "abinitio",
            "title": f"Ab Initio Graph: {base_filename}",
            "metadata": {
                'file_name': graph_path.name,
                'file_path': str(graph_path),
                'absolute_file_path': str(graph_path.resolve()),  # For agents to read actual file
                'system_type': 'abinitio',
                'graph_name': base_filename,
                'vertex_count': len(vertices),
                'flow_count': len(flows),
                'port_count': len(ports),
                'has_graphflow': graphflow_result.get('success', False),
                'has_sttm': sttm_success,
                'graphflow_file': graphflow_result.get('excel_file', ''),
                'sttm_file': sttm_result.get('excel_file', '') if sttm_success else '',
                'parsed_json_path': str(parsed_json_path),  # CRITICAL FIX: Must be 'parsed_json_path' not 'parsed_json'
                'graphflow_excel': graphflow_result.get('excel_file', ''),  # Alternative lookup
                'automation_sttm_json': sttm_result.get('json_file', '') if sttm_success else '',
                'automation_sttm_excel': sttm_result.get('excel_file', '') if sttm_success else '',
                'raw_content_size': len(raw_content)
            }
        }

        # Index in vector DB
        st.session_state.indexer.collections["abinitio_collection"].index_documents([doc])

        progress_bar.progress(100)
        status_text.empty()

        # Final success message
        st.success("🎉 **Single Graph Indexing Complete!**")
        st.info(f"""
**Graph**: {base_filename}
**Components**: {len(vertices)} vertices, {len(flows)} flows, {len(ports)} ports

**Generated Files**:
- 📄 Parsed JSON: `{parsed_json_path}`
- 📊 GraphFlow Excel: `{graphflow_result.get('excel_file', 'N/A')}`
- 📋 STTM Excel: `{sttm_result.get('excel_file', 'N/A') if sttm_success else 'N/A'}`

**Vector DB**: ✅ Indexed in abinitio_collection

**You can now**:
- 💬 Ask questions about this graph in the chat
- 📊 Generate STAG comparison documents
- 🔍 Search for components and flows
        """)

        # Refresh stats
        st.session_state.stats = st.session_state.indexer.get_stats()

    except Exception as e:
        st.error(f"❌ Error during single graph indexing: {e}")
        logger.error(f"Single graph indexing error: {e}", exc_info=True)
    finally:
        progress_bar.empty()


def reindex_abinitio_from_upload(uploaded_files):
    """Re-index Ab Initio from uploaded files"""
    # Reuse existing file upload logic
    index_uploaded_files(uploaded_files)


def reindex_autosys_from_directory(directory_path: str):
    """Re-index Autosys from directory"""
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        status_text.text("Parsing Autosys files...")
        progress_bar.progress(20)

        parser = AutosysParser()
        result = parser.parse_directory(directory_path)

        progress_bar.progress(50)
        status_text.text("Indexing jobs...")

        if st.session_state.indexer:
            jobs_dict = [job.__dict__ for job in result.get("components", [])]
            stats = st.session_state.indexer.index_autosys(jobs=jobs_dict)

            progress_bar.progress(100)
            status_text.empty()

            st.success(f"✓ Indexed {stats.get('jobs', 0)} Autosys jobs")

            # Refresh stats
            st.session_state.stats = st.session_state.indexer.get_stats()
            st.session_state.indexed_files['autosys'].append(f"Directory: {directory_path}")

    except Exception as e:
        st.error(f"Error indexing Autosys: {e}")
    finally:
        progress_bar.empty()


def reindex_autosys_from_upload(uploaded_files):
    """Re-index Autosys from uploaded files"""
    index_uploaded_files(uploaded_files)


def reindex_hadoop_from_directory(directory_path: str):
    """
    Re-index Hadoop with DEEP AI-powered understanding

    This now indexes ALL files in the repository, not just workflows.
    Uses AI to understand each script, extracts STTM mappings, and tracks file dependencies.
    """
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # Initialize AI analyzer if not already done
        if 'ai_analyzer' not in st.session_state or st.session_state.ai_analyzer is None:
            st.session_state.ai_analyzer = AIScriptAnalyzer()
            logger.info("✓ AI Script Analyzer initialized")

        # Initialize STTM generator if not already done
        if 'sttm_generator' not in st.session_state:
            st.session_state.sttm_generator = STTMGenerator(ai_analyzer=st.session_state.ai_analyzer)
            logger.info("✓ STTM Generator initialized")

        # Log what we're working with
        logger.info(f"Starting Hadoop re-indexing from: {directory_path}")
        logger.info(f"AI Analyzer enabled: {st.session_state.ai_analyzer.enabled if st.session_state.ai_analyzer else False}")

        status_text.text("🔍 Scanning repository for ALL files...")
        progress_bar.progress(10)

        # Use deep indexing to index ALL files with AI
        result = index_all_repository_files_with_ai(
            repository_path=directory_path,
            system_type="hadoop",
            ai_analyzer=st.session_state.ai_analyzer,
            sttm_generator=st.session_state.sttm_generator,
            progress_bar=progress_bar,
            status_text=status_text
        )

        # Index all documents to vector database
        if result["documents"]:
            status_text.text("💾 Indexing to vector database...")
            progress_bar.progress(95)
            st.session_state.indexer.collections["hadoop_collection"].index_documents(result["documents"])

        progress_bar.progress(100)
        status_text.empty()

        # Display results
        st.success(f"✅ **Deep Indexing Complete!**")
        st.info(f"""
**Statistics:**
- 📁 Total files scanned: {result['total_files']}
- 📝 Documents created: {result['documents_created']}
- 🎯 STTM mappings generated: {result['sttm_mappings']}
- 🔗 File dependencies tracked: {len(result['file_references'])}

**Output Locations:**
- AI-enriched documents: `{result['output_dir']}`
- STTM mappings: `{result['sttm_dir'] or 'No mappings generated'}`
- File references map: `{result['output_dir']}/file_references_map.json`
        """)

        # Refresh stats
        st.session_state.stats = st.session_state.indexer.get_stats()
        st.session_state.indexed_files['hadoop'] = st.session_state.indexed_files.get('hadoop', [])
        st.session_state.indexed_files['hadoop'].append(f"Directory: {directory_path} (DEEP)")

    except Exception as e:
        st.error(f"Error during deep indexing: {e}")
        logger.error(f"Hadoop deep indexing error: {e}", exc_info=True)
    finally:
        progress_bar.empty()


def reindex_hadoop_from_upload(uploaded_files):
    """Re-index Hadoop from uploaded files"""
    index_uploaded_files(uploaded_files)


def reindex_databricks_from_directory(directory_path: str):
    """
    Re-index Databricks with DEEP AI-powered understanding

    This now indexes ALL files in the repository, not just workflows.
    Uses AI to understand each script, extracts STTM mappings, and tracks file dependencies.
    """
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # Initialize AI analyzer if not already done
        if 'ai_analyzer' not in st.session_state or st.session_state.ai_analyzer is None:
            st.session_state.ai_analyzer = AIScriptAnalyzer()
            logger.info("✓ AI Script Analyzer initialized")

        # Initialize STTM generator if not already done
        if 'sttm_generator' not in st.session_state:
            st.session_state.sttm_generator = STTMGenerator(ai_analyzer=st.session_state.ai_analyzer)
            logger.info("✓ STTM Generator initialized")

        # Log what we're working with
        logger.info(f"Starting Databricks re-indexing from: {directory_path}")
        logger.info(f"AI Analyzer enabled: {st.session_state.ai_analyzer.enabled if st.session_state.ai_analyzer else False}")

        status_text.text("🔍 Scanning repository for ALL files...")
        progress_bar.progress(10)

        # Use deep indexing to index ALL files with AI
        result = index_all_repository_files_with_ai(
            repository_path=directory_path,
            system_type="databricks",
            ai_analyzer=st.session_state.ai_analyzer,
            sttm_generator=st.session_state.sttm_generator,
            progress_bar=progress_bar,
            status_text=status_text
        )

        # Index all documents to vector database
        if result["documents"]:
            status_text.text("💾 Indexing to vector database...")
            progress_bar.progress(95)
            st.session_state.indexer.collections["databricks_collection"].index_documents(result["documents"])

        progress_bar.progress(100)
        status_text.empty()

        # Display results
        st.success(f"✅ **Deep Indexing Complete!**")
        st.info(f"""
**Statistics:**
- 📁 Total files scanned: {result['total_files']}
- 📝 Documents created: {result['documents_created']}
- 🎯 STTM mappings generated: {result['sttm_mappings']}
- 🔗 File dependencies tracked: {len(result['file_references'])}

**Output Locations:**
- AI-enriched documents: `{result['output_dir']}`
- STTM mappings: `{result['sttm_dir'] or 'No mappings generated'}`
- File references map: `{result['output_dir']}/file_references_map.json`
        """)

        # Refresh stats
        st.session_state.stats = st.session_state.indexer.get_stats()
        st.session_state.indexed_files['databricks'] = st.session_state.indexed_files.get('databricks', [])
        st.session_state.indexed_files['databricks'].append(f"Directory: {directory_path} (DEEP)")

    except Exception as e:
        st.error(f"Error during deep indexing: {e}")
        logger.error(f"Databricks deep indexing error: {e}", exc_info=True)
    finally:
        progress_bar.empty()


def reindex_databricks_from_upload(uploaded_files):
    """Re-index Databricks from uploaded files"""
    index_uploaded_files(uploaded_files)


def reindex_documents_from_directory(directory_path: str, recursive: bool = True):
    """Re-index documents from directory"""
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        status_text.text("Parsing documents...")
        progress_bar.progress(20)

        parser = DocumentParser()
        docs = parser.parse_directory(directory_path, recursive=recursive)

        progress_bar.progress(50)
        status_text.text("Indexing document chunks...")

        if st.session_state.indexer:
            stats = parser.index_documents(docs, st.session_state.indexer)

            progress_bar.progress(100)
            status_text.empty()

            st.success(f"✓ Indexed {stats.get('files', 0)} documents ({stats.get('total', 0)} chunks)")

            # Refresh stats
            st.session_state.stats = st.session_state.indexer.get_stats()
            st.session_state.indexed_files['documents'].append(f"Directory: {directory_path}")

    except Exception as e:
        st.error(f"Error indexing documents: {e}")
    finally:
        progress_bar.empty()


def reindex_documents_from_upload(uploaded_files):
    """Re-index documents from uploaded files"""
    index_uploaded_files(uploaded_files)


def reindex_all(abinitio_path: str = "", autosys_path: str = "", documents_path: str = ""):
    """Re-index all systems"""
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # Step 1: Clear database
        status_text.text("Step 1/4: Clearing existing database...")
        progress_bar.progress(10)

        import shutil
        db_path = Path("./outputs/vector_db")
        if db_path.exists():
            shutil.rmtree(db_path)

        # Step 2: Reinitialize
        status_text.text("Step 2/4: Initializing fresh indexer...")
        progress_bar.progress(20)

        st.session_state.indexer = MultiCollectionIndexer()

        # Step 3: Index each system
        total_steps = sum([bool(abinitio_path), bool(autosys_path), bool(documents_path)])
        current_step = 0

        if abinitio_path and Path(abinitio_path).exists():
            current_step += 1
            status_text.text(f"Step 3/4: Indexing Ab Initio ({current_step}/{total_steps})...")
            progress_bar.progress(30 + (current_step * 20))
            reindex_abinitio_from_directory(abinitio_path)

        if autosys_path and Path(autosys_path).exists():
            current_step += 1
            status_text.text(f"Step 3/4: Indexing Autosys ({current_step}/{total_steps})...")
            progress_bar.progress(30 + (current_step * 20))
            reindex_autosys_from_directory(autosys_path)

        if documents_path and Path(documents_path).exists():
            current_step += 1
            status_text.text(f"Step 3/4: Indexing Documents ({current_step}/{total_steps})...")
            progress_bar.progress(30 + (current_step * 20))
            reindex_documents_from_directory(documents_path)

        # Step 4: Finalize
        status_text.text("Step 4/4: Finalizing...")
        progress_bar.progress(90)

        st.session_state.stats = st.session_state.indexer.get_stats()

        progress_bar.progress(100)
        status_text.empty()

        st.success("✓ Full re-index complete!")
        st.balloons()

    except Exception as e:
        st.error(f"Error during re-indexing: {e}")
    finally:
        progress_bar.empty()


# ============================================
# Main Application
# ============================================

def main():
    """Main application entry point"""

    # Initialize session state
    initialize_session_state()

    # Initialize RAG components
    initialize_rag_components()

    # Render sidebar
    render_sidebar()

    # Main tabs
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "💬 Chat",
        "🔄 Compare",
        "📊 Analytics",
        "⚙️ Database",
        "🔗 Lineage (Legacy)",
        "🧩 System Mapping",
        "🔍 Lineage Tracking"
    ])

    with tab1:
        render_chat_interface()

    with tab2:
        render_comparison_mode()

    with tab3:
        render_analytics_dashboard()

    with tab4:
        render_database_management()

    with tab5:
        render_lineage_tab()

    with tab6:
        render_system_mapping_tab()

    with tab7:
        render_enhanced_lineage_tab()


def render_analytics_dashboard():
    """Render analytics and query history"""

    st.subheader("📊 Analytics Dashboard")

    # Query history
    if st.session_state.query_history:
        st.markdown("### 📜 Query History")

        history_df = pd.DataFrame(st.session_state.query_history)
        st.dataframe(history_df, use_container_width=True)

    # Collection statistics
    st.markdown("### 📈 Collection Statistics")

    if st.session_state.stats:
        stats_data = []
        for coll, stats in st.session_state.stats.items():
            stats_data.append({
                "Collection": coll.replace('_collection', '').title(),
                "Documents": stats.get('total_documents', 0)
            })

        stats_df = pd.DataFrame(stats_data)

        col1, col2 = st.columns([2, 1])
        with col1:
            st.bar_chart(stats_df.set_index("Collection"))
        with col2:
            st.dataframe(stats_df, use_container_width=True)

    # Indexed files
    st.markdown("### 📁 Indexed Files")

    for system, files in st.session_state.indexed_files.items():
        if files:
            with st.expander(f"{system.title()} ({len(files)} files)"):
                for file in files:
                    st.write(f"- {file}")


# ============================================
# Run Application
# ============================================

if __name__ == "__main__":
    main()
