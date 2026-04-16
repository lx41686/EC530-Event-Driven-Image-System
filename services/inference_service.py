import redis
import numpy as np # Remember to add numpy to requirements.txt

def mock_vector_generation(image_id):
    """
    Simulates the Embedding Service by generating a random 128-d vector.
    Assigns a unique integer ID to the image_id for FAISS indexing.
    """
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # 1. Generate an incremental integer ID (e.g., 1, 2, 3...)
    # Redis 'incr' command is perfect for this
    numeric_id = r.incr("global:image:id:counter")
    
    # 2. Store the mapping: numeric_id -> image_id
    r.set(f"map:id:{numeric_id}", image_id)
    
    # 3. Mock a 128-dimension vector
    vector = np.random.random(128).tolist()
    
    return numeric_id, vector
