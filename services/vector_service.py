import redis
import json
import faiss
import numpy as np
import os

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
INDEX_FILE = "vector.index"
DIMENSION = 128  # Must match the dimension in inference_service.py


def start_vector_service():
    """
    Subscribes to 'vector.generated' and manages the FAISS Vector Index.
    """
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe("vector.generated")

    # 1. Initialize or Load FAISS Index
    if os.path.exists(INDEX_FILE):
        index = faiss.read_index(INDEX_FILE)
        print(f" [*] Loaded existing FAISS index from {INDEX_FILE}")
    else:
        # IndexFlatL2 is a simple brute-force L2 distance search
        index = faiss.IndexFlatL2(DIMENSION)
        # We use IndexIDMap so we can associate our numeric_id with the vector
        index = faiss.IndexIDMap(index)
        print(" [*] Created new FAISS IndexIDMap")

    print(f" [*] Vector Service started. Listening for 'vector.generated' events...")

    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                # 2. Parse Event
                event_data = json.loads(message['data'])
                payload = event_data.get('payload', {})

                img_id = payload.get('image_id')
                num_id = payload.get('numeric_id')
                vector = payload.get('vector')

                if not vector or num_id is None:
                    print(" [!] Missing vector data or ID. Skipping...")
                    continue

                # 3. Prepare data for FAISS
                # FAISS requires float32 and (1, dimension) shape
                np_vector = np.array([vector]).astype('float32')
                np_id = np.array([num_id]).astype('int64')

                # 4. Add to Index
                index.add_with_ids(np_vector, np_id)

                # 5. Save the index to disk (Persistence)
                faiss.write_index(index, INDEX_FILE)

                print(f" [v] Vector added to FAISS: Image {img_id} (ID: {num_id})")
                print(f" [Total Vectors in Index]: {index.ntotal}")

            except Exception as e:
                print(f" [ERROR] Vector Service failure: {e}")


if __name__ == "__main__":
    start_vector_service()