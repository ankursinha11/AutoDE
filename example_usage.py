#!/usr/bin/env python3
"""
Example Usage Script for AI-Powered Source-to-Target Mapping Tool
================================================================

This script demonstrates how to use the AI-powered mapping tool with your
Azure OpenAI credentials and repository paths.

Usage:
    python example_usage.py

Make sure to update the configuration section with your actual credentials and paths.
"""

import os
from ai_powered_source_target_mapper import AIPoweredSourceTargetMapper

def main():
    """Example usage of the AI-powered source-to-target mapping tool"""
    
    print("🚀 AI-Powered Source-to-Target Mapping Tool - Example Usage")
    print("=" * 70)
    
    # ============================================================================
    # CONFIGURATION SECTION - UPDATE THESE VALUES
    # ============================================================================
    
    # Azure OpenAI Configuration
    AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY", "your-azure-openai-api-key-here")
    AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "https://your-resource.openai.azure.com/")
    
    # Repository Paths - Update these to your actual repository paths
    HADOOP_REPO_PATH = "./OneDrive_1_7-25-2025/Hadoop/app-data-ingestion"  # Your Hadoop repo path
    DATABRICKS_REPO_PATH = "./mock_databricks_cdd"  # Your Databricks repo path
    
    # Repository Names
    HADOOP_REPO_NAME = "app-cdd"
    DATABRICKS_REPO_NAME = "CDD"
    
    # Output Configuration
    OUTPUT_PREFIX = "CDD_MIGRATION_ANALYSIS"
    
    # ============================================================================
    # VALIDATION SECTION
    # ============================================================================
    
    print("🔍 Validating Configuration...")
    
    # Check Azure OpenAI credentials
    if AZURE_OPENAI_KEY == "your-azure-openai-api-key-here":
        print("❌ Please set your Azure OpenAI API key!")
        print("   Option 1: Set environment variable: export AZURE_OPENAI_KEY='your-key'")
        print("   Option 2: Update the script with your actual API key")
        return
    
    if "your-resource" in AZURE_OPENAI_ENDPOINT:
        print("❌ Please set your Azure OpenAI endpoint!")
        print("   Option 1: Set environment variable: export AZURE_OPENAI_ENDPOINT='https://your-resource.openai.azure.com/'")
        print("   Option 2: Update the script with your actual endpoint")
        return
    
    # Check repository paths
    if not os.path.exists(HADOOP_REPO_PATH):
        print(f"❌ Hadoop repository path not found: {HADOOP_REPO_PATH}")
        print("   Please update HADOOP_REPO_PATH with the correct path to your Hadoop repository")
        return
    
    if not os.path.exists(DATABRICKS_REPO_PATH):
        print(f"❌ Databricks repository path not found: {DATABRICKS_REPO_PATH}")
        print("   Please update DATABRICKS_REPO_PATH with the correct path to your Databricks repository")
        return
    
    print("✅ Configuration validated successfully!")
    
    # ============================================================================
    # ANALYSIS EXECUTION
    # ============================================================================
    
    try:
        print(f"\n🚀 Starting Analysis...")
        print(f"   Hadoop Repository: {HADOOP_REPO_PATH}")
        print(f"   Databricks Repository: {DATABRICKS_REPO_PATH}")
        print(f"   Azure OpenAI Endpoint: {AZURE_OPENAI_ENDPOINT}")
        
        # Initialize the mapper
        print("\n🔧 Initializing AI-Powered Mapper...")
        mapper = AIPoweredSourceTargetMapper(AZURE_OPENAI_KEY, AZURE_OPENAI_ENDPOINT)
        
        # Analyze both repositories
        print("\n📊 Analyzing Repositories...")
        analysis_results = mapper.analyze_repositories(
            hadoop_repo_path=HADOOP_REPO_PATH,
            databricks_repo_path=DATABRICKS_REPO_PATH,
            hadoop_repo_name=HADOOP_REPO_NAME,
            databricks_repo_name=DATABRICKS_REPO_NAME
        )
        
        # Generate all reports
        print("\n📋 Generating Excel Reports...")
        report_files = mapper.generate_all_reports(analysis_results, OUTPUT_PREFIX)
        
        # Print comprehensive summary
        print("\n" + "="*70)
        mapper.print_analysis_summary(analysis_results)
        
        # Print report locations
        print(f"\n📊 Generated Reports:")
        print(f"   📄 Hadoop Mapping: {report_files['hadoop_report']}")
        print(f"   📄 Databricks Mapping: {report_files['databricks_report']}")
        print(f"   📄 Comparison Report: {report_files['comparison_report']}")
        
        # Print next steps
        print(f"\n🎯 Next Steps:")
        print(f"   1. Review the Excel reports for detailed field mappings")
        print(f"   2. Focus on the Comparison Report to identify differences")
        print(f"   3. Use the field-level mappings to plan your migration")
        print(f"   4. Address any missing tables or fields identified in the comparison")
        
        print(f"\n✅ Analysis Complete! All reports generated successfully.")
        
    except Exception as e:
        print(f"\n❌ Error during analysis: {e}")
        print(f"\n🔧 Troubleshooting Tips:")
        print(f"   1. Verify your Azure OpenAI credentials are correct")
        print(f"   2. Check that your repository paths are accessible")
        print(f"   3. Ensure you have the required Python packages installed")
        print(f"   4. Check your Azure OpenAI quota and model availability")
        
        # Print detailed error information for debugging
        import traceback
        print(f"\n🔍 Detailed Error Information:")
        traceback.print_exc()

def print_configuration_template():
    """Print a configuration template for easy setup"""
    print("\n" + "="*70)
    print("📋 CONFIGURATION TEMPLATE")
    print("="*70)
    print("""
# Environment Variables (Recommended)
export AZURE_OPENAI_KEY="your-actual-api-key-here"
export AZURE_OPENAI_ENDPOINT="https://your-actual-resource.openai.azure.com/"

# Or update these variables in the script:
AZURE_OPENAI_KEY = "your-actual-api-key-here"
AZURE_OPENAI_ENDPOINT = "https://your-actual-resource.openai.azure.com/"
HADOOP_REPO_PATH = "/path/to/your/hadoop/app-cdd"
DATABRICKS_REPO_PATH = "/path/to/your/databricks/CDD"
""")

if __name__ == "__main__":
    # Print configuration template if credentials are not set
    if (os.getenv("AZURE_OPENAI_KEY", "your-azure-openai-api-key-here") == "your-azure-openai-api-key-here" or
        "your-resource" in os.getenv("AZURE_OPENAI_ENDPOINT", "https://your-resource.openai.azure.com/")):
        print_configuration_template()
    
    main()
