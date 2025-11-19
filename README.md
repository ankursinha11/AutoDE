"""
Fix Corrupted ChromaDB Collections

This script fixes the "hnsw segment reader: Nothing found on disk" error
by recreating corrupted or empty collections.

Usage:
    python fix_corrupted_collections.py
"""

import shutil
from pathlib import Path
from loguru import logger

def fix_corrupted_collections():
    """Fix corrupted ChromaDB collections"""

    vector_db_path = Path("./outputs/vector_db")

    if not vector_db_path.exists():
        print("✅ No vector_db directory found - nothing to fix")
        return

    print("=" * 80)
    print("🔧 FIXING CORRUPTED CHROMADB COLLECTIONS")
    print("=" * 80)
    print()

    # List all collections
    collections = [d for d in vector_db_path.iterdir() if d.is_dir()]

    if not collections:
        print("✅ No collections found")
        return

    print(f"Found {len(collections)} collections:")
    for coll in collections:
        print(f"  - {coll.name}")
    print()

    # Check each collection
    corrupted = []

    for coll_dir in collections:
        coll_name = coll_dir.name

        # Check if collection has data
        has_data = False

        # Look for .bin files (HNSW index data)
        bin_files = list(coll_dir.rglob("*.bin"))

        # Look for .parquet files (metadata)
        parquet_files = list(coll_dir.rglob("*.parquet"))

        if bin_files or parquet_files:
            has_data = True
            print(f"✅ {coll_name}: OK ({len(bin_files)} .bin files, {len(parquet_files)} .parquet files)")
        else:
            print(f"❌ {coll_name}: CORRUPTED (no data files)")
            corrupted.append(coll_dir)

    print()

    if not corrupted:
        print("✅ All collections are healthy!")
        return

    # Ask user for confirmation
    print(f"Found {len(corrupted)} corrupted collection(s):")
    for coll in corrupted:
        print(f"  - {coll.name}")
    print()

    response = input("Delete corrupted collections? (yes/no): ").strip().lower()

    if response == "yes":
        print()
        for coll_dir in corrupted:
            try:
                print(f"🗑️  Deleting {coll_dir.name}...")
                shutil.rmtree(coll_dir)
                print(f"   ✅ Deleted")
            except Exception as e:
                print(f"   ❌ Error: {e}")

        print()
        print("=" * 80)
        print("✅ CLEANUP COMPLETE")
        print("=" * 80)
        print()
        print("Next steps:")
        print("1. Start STAG: streamlit run stag_app.py")
        print("2. Go to: Database → Re-index Databricks (or whatever was corrupted)")
        print("3. Index your data fresh")
    else:
        print("❌ Cancelled - no changes made")


if __name__ == "__main__":
    try:
        fix_corrupted_collections()
    except KeyboardInterrupt:
        print("\n\n⏹️  Cancelled by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
