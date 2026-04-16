import redis
import json
import time
import numpy as np

def mock_vector_generation(image_id):
    """
    Simulates the Embedding Service by generating a random 128-d vector.
    Assigns a unique integer ID to the image_id for FAISS indexing later.
    """
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # 1. Generate an incremental integer ID (e.g., 1, 2, 3...)
    # This is required because FAISS only accepts integer IDs.
    numeric_id = r.incr("global:image:id:counter")
    
    # 2. Store the mapping: numeric_id -> image_id for future retrieval
    r.set(f"map:id:{numeric_id}", image_id)
    
    # 3. Mock a 128-dimension vector (standard for small embedding models)
    vector = np.random.random(128).tolist()
    
    return numeric_id, vector

def start_inference_worker():
    """
    Main loop that listens to 'image.submitted' and simulates AI object detection.
    """
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe("image.submitted")
    
    print(" [*] Inference Service started. Waiting for images...")
    
    for message in pubsub.listen():
        if message['type'] == 'message':
            # 1. Extract data from the 'Payload' of the received event
            event_data = json.loads(message['data'])
            img_id = event_data['payload'].get('image_id')
            
            print(f" [v] Processing image: {img_id}")
            
            # 2. Simulate AI processing time
            time.sleep(1) 
            
            # 3. Generate Mock Data (Annotations and Vector)
            # We call our helper function to get a numeric ID and a vector
            numeric_id, vector = mock_vector_generation(img_id)
            
            inference_results = {
                "image_id": img_id,
                "numeric_id": numeric_id, # Added for FAISS compatibility
                "vector": vector,         # Mocked embedding
                "objects": [
                    {"label": "cat", "confidence": 0.95, "bbox": [10, 20, 50, 50]},
                    {"label": "tree", "confidence": 0.88, "bbox": [100, 200, 30, 40]}
                ]
            }
            
            # 4. Construct and Publish the 'inference.completed' event
            completion_event = {
                "topic": "inference.completed",
                "event_id": f"inf_{int(time.time())}",
                "payload": inference_results,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }
            
            r.publish("inference.completed", json.dumps(completion_event))
            print(f" [x] Inference results published for {img_id}")
            
            # Break for the unit test to complete successfully
            break

if __name__ == "__main__":
    start_inference_worker()
