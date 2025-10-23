# AutoDE
This repo contains projects done which applies principles in data engineering. This repo contains multiple projects and modules that will help automate day to day manual tasks that data engineers, data scientist, business teams etc perform.
python -c "
import os
from openai import AzureOpenAI
api_key = 'YOUR_API_KEY_HERE'
endpoint = 'https://az-opn-ai.openai.azure.com/'
client = AzureOpenAI(api_key=api_key, api_version='2024-02-15-preview', azure_endpoint=endpoint)
response = client.chat.completions.create(model='gpt-4', messages=[{'role': 'user', 'content': 'Hello'}], max_tokens=5)
print('✅ Success:', response.choices[0].message.content)
"
