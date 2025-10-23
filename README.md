#!/usr/bin/env python3
"""
Simple Azure OpenAI Test - Step by Step: AIzaSyCDFhjA94fAV5UYYxX43WVm19T24smy4vA
=======================================
"""

import os

def test_step_by_step():
    """Test Azure OpenAI step by step"""
    print("🔍 Step-by-Step Azure OpenAI Test")
    print("=" * 40)
    
    # Step 1: Check if openai package is installed
    print("\n1️⃣ Checking if openai package is installed...")
    try:
        import openai
        print("✅ openai package is installed")
    except ImportError:
        print("❌ openai package not installed. Run: pip install openai")
        return False
    
    # Step 2: Check credentials
    print("\n2️⃣ Checking credentials...")
    api_key = os.getenv("AZURE_OPENAI_KEY", "REPLACE_WITH_YOUR_ACTUAL_API_KEY")
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "https://az-opn-ai.openai.azure.com/")
    
    print(f"   API Key: {'Set' if api_key != 'REPLACE_WITH_YOUR_ACTUAL_API_KEY' else 'NOT SET'}")
    print(f"   Endpoint: {endpoint}")
    
    if api_key == "REPLACE_WITH_YOUR_ACTUAL_API_KEY":
        print("❌ Please set your API key!")
        print("   Either:")
        print("   - Set environment variable: export AZURE_OPENAI_KEY='your-key'")
        print("   - Or update the script with your actual API key")
        return False
    
    # Step 3: Test connection
    print("\n3️⃣ Testing connection...")
    try:
        from openai import AzureOpenAI
        
        client = AzureOpenAI(
            api_key=api_key,
            api_version="2024-02-15-preview",
            azure_endpoint=endpoint
        )
        print("✅ Client created successfully")
        
        # Step 4: Test with gpt-4
        print("\n4️⃣ Testing with gpt-4...")
        try:
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": "Hello"}],
                max_tokens=5
            )
            print("✅ gpt-4 works!")
            print(f"   Response: {response.choices[0].message.content}")
            return True
        except Exception as e:
            print(f"❌ gpt-4 failed: {e}")
            
            # Step 5: Try gpt-35-turbo
            print("\n5️⃣ Testing with gpt-35-turbo...")
            try:
                response = client.chat.completions.create(
                    model="gpt-35-turbo",
                    messages=[{"role": "user", "content": "Hello"}],
                    max_tokens=5
                )
                print("✅ gpt-35-turbo works!")
                print(f"   Response: {response.choices[0].message.content}")
                return True
            except Exception as e2:
                print(f"❌ gpt-35-turbo failed: {e2}")
                print("\n🔍 Both models failed. Check your Azure portal for deployment names.")
                return False
                
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    success = test_step_by_step()
    
    if success:
        print("\n🎉 SUCCESS! Your Azure OpenAI is working.")
    else:
        print("\n⚠️ FAILED. Please check the errors above.")
