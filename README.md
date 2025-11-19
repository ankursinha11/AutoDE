Things to Check on Windows AVD
Check the vector DB collection name:
import chromadb
client = chromadb.PersistentClient(path="path/to/vector_db")
collections = client.list_collections()
for col in collections:
    print(f"{col.name}: {col.count()} documents")
Check if parsed JSON files exist:
# Look for files like:
# {output_folder}/{graph_name}_components.json
Check the metadata in indexed documents:
collection = client.get_collection("abinitio_collection")
results = collection.get(include=['metadatas'])
for metadata in results['metadatas'][:5]:
    print(f"parsed_json_path: {metadata.get('parsed_json_path')}")
