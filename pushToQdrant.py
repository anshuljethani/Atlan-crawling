import json
from qdrant_client import QdrantClient
from platform_vectorize import vectorize  # platform.vectorize.io SDK

# --- Step 1: Load JSON ---
with open("data.json", "r") as f:
    docs = json.load(f)

# --- Step 2: Initialize Qdrant client ---
qdrant = QdrantClient(url="http://localhost:6333")  # or your Qdrant cloud endpoint

# --- Step 3: Vectorize using platform.vectorize.io ---
# This assumes your platform.vectorize API is set up and authenticated
# `text_field` = field to embed, `id_field` = unique id
vectors = vectorize(
    docs,
    text_field="text",
    id_field="url_id"
)

# --- Step 4: Upsert into Qdrant ---
# Make sure you already created the collection with correct vector size (e.g., 1536 for OpenAI)
qdrant.upsert(
    collection_name="atlan_docs",
    points=[
        {
            "id": v["id"],  # taken from url_id
            "vector": v["vector"],  # dense embedding
            "payload": {
                "url": v["url"],
                "text": v["text"]
            }
        }
        for v in vectors
    ]
)

print("✅ Data successfully pushed to Qdrant!")
