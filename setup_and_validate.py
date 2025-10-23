#!/usr/bin/env python3
"""
Quick Setup Script for AI-Powered Source-to-Target Mapping Tool
================================================================

This script helps you set up the environment and validate your configuration
before running the main analysis.

Usage:
    python setup_and_validate.py
"""

import os
import sys
import subprocess
from pathlib import Path

def check_python_version():
    """Check if Python version is compatible"""
    print("🐍 Checking Python version...")
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        print(f"   Current version: {sys.version}")
        return False
    print(f"✅ Python version: {sys.version}")
    return True

def install_requirements():
    """Install required packages"""
    print("\n📦 Installing required packages...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ All packages installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error installing packages: {e}")
        return False

def check_azure_openai_config():
    """Check Azure OpenAI configuration"""
    print("\n🔑 Checking Azure OpenAI configuration...")
    
    api_key = os.getenv("AZURE_OPENAI_KEY")
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    
    if not api_key or api_key == "your-azure-openai-api-key-here":
        print("❌ Azure OpenAI API key not configured")
        print("   Please set: export AZURE_OPENAI_KEY='your-actual-api-key'")
        return False
    
    if not endpoint or "your-resource" in endpoint:
        print("❌ Azure OpenAI endpoint not configured")
        print("   Please set: export AZURE_OPENAI_ENDPOINT='https://your-resource.openai.azure.com/'")
        return False
    
    print("✅ Azure OpenAI configuration found")
    print(f"   Endpoint: {endpoint}")
    print(f"   API Key: {'*' * (len(api_key) - 4) + api_key[-4:] if len(api_key) > 4 else '***'}")
    return True

def check_repository_paths():
    """Check if repository paths exist"""
    print("\n📁 Checking repository paths...")
    
    # Default paths from the example
    hadoop_path = "./OneDrive_1_7-25-2025/Hadoop/app-data-ingestion"
    databricks_path = "./mock_databricks_cdd"
    
    hadoop_exists = Path(hadoop_path).exists()
    databricks_exists = Path(databricks_path).exists()
    
    print(f"   Hadoop path ({hadoop_path}): {'✅ Found' if hadoop_exists else '❌ Not found'}")
    print(f"   Databricks path ({databricks_path}): {'✅ Found' if databricks_exists else '❌ Not found'}")
    
    if not hadoop_exists:
        print("   💡 Update HADOOP_REPO_PATH in your script with the correct path")
    
    if not databricks_exists:
        print("   💡 Update DATABRICKS_REPO_PATH in your script with the correct path")
    
    return hadoop_exists and databricks_exists

def test_azure_openai_connection():
    """Test Azure OpenAI connection"""
    print("\n🔌 Testing Azure OpenAI connection...")
    
    try:
        from openai import AzureOpenAI
        
        api_key = os.getenv("AZURE_OPENAI_KEY")
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        
        client = AzureOpenAI(
            api_key=api_key,
            api_version="2024-02-15-preview",
            azure_endpoint=endpoint
        )
        
        # Test with a simple request
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": "Hello, this is a test."}],
            max_tokens=10
        )
        
        print("✅ Azure OpenAI connection successful")
        return True
        
    except Exception as e:
        print(f"❌ Azure OpenAI connection failed: {e}")
        print("   💡 Check your API key, endpoint, and model availability")
        return False

def print_next_steps():
    """Print next steps for the user"""
    print("\n" + "="*60)
    print("🎯 NEXT STEPS")
    print("="*60)
    print("""
1. 📝 Update Configuration:
   - Set your Azure OpenAI credentials
   - Update repository paths in example_usage.py

2. 🚀 Run Analysis:
   python example_usage.py

3. 📊 Review Results:
   - Check generated Excel reports
   - Focus on comparison differences
   - Plan your migration strategy

4. 🔧 Customize (Optional):
   - Modify field mapping attributes
   - Adjust AI analysis parameters
   - Customize report formats
""")

def main():
    """Main setup and validation function"""
    print("🚀 AI-Powered Source-to-Target Mapping Tool - Setup & Validation")
    print("="*70)
    
    # Check Python version
    if not check_python_version():
        return
    
    # Install requirements
    if not install_requirements():
        return
    
    # Check Azure OpenAI configuration
    azure_config_ok = check_azure_openai_config()
    
    # Check repository paths
    repo_paths_ok = check_repository_paths()
    
    # Test Azure OpenAI connection if configured
    connection_ok = False
    if azure_config_ok:
        connection_ok = test_azure_openai_connection()
    
    # Summary
    print("\n" + "="*60)
    print("📋 SETUP SUMMARY")
    print("="*60)
    print(f"   Python Version: ✅")
    print(f"   Required Packages: ✅")
    print(f"   Azure OpenAI Config: {'✅' if azure_config_ok else '❌'}")
    print(f"   Repository Paths: {'✅' if repo_paths_ok else '❌'}")
    print(f"   Azure OpenAI Connection: {'✅' if connection_ok else '❌'}")
    
    if azure_config_ok and repo_paths_ok and connection_ok:
        print("\n🎉 Setup Complete! You're ready to run the analysis.")
        print("   Run: python example_usage.py")
    else:
        print("\n⚠️ Setup incomplete. Please address the issues above.")
        print_next_steps()

if __name__ == "__main__":
    main()
