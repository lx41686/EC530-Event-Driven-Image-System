import redis
import json
import time
import random
import numpy as np

# Configuration for Redis
REDIS_HOST = 'localhost'
REDIS_PORT = 6379


def mock_vector_generation(image_id: str):
    """
    Simulates an Embedding model generating a feature vector.
    Assigns a unique integer ID for future FAISS indexing.

    Args:
        image_id (str): The string-based ID of the image.

    Returns:
        tuple: (numeric_id, vector_list)
    """
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Generate an incremental integer ID (Required for FAISS)
    numeric_id = r.incr("global:image:id:counter")

    # Map numeric ID to the original Image ID in Redis for lookups
    r.set(f"map:id:{numeric_id}", image_id)

    # Generate a random 128-dimension vector to simulate an embedding
    vector = np.random.random(128).tolist()

    return numeric_id, vector

def start_inference_worker():
    """
    Subscribes
    to
    'image.submitted'
    events, simulates
    AI
    object
    detection,
    and publishes
    an
    'inference.completed'
    event
    with localized results.
    """
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe("image.submitted")

    print(" [*] Inference Service started. Listening for submitted images...")

    # Pre-defined pool of objects to make the mock data more diverse
    object_pool = [
        {"label": "cat", "confidence": 0.95},
        {"label": "dog", "confidence": 0.92},
        {"label": "car", "confidence": 0.88},
        {"label": "bicycle", "confidence": 0.91},
        {"label": "person", "confidence": 0.97}
    ]

    for message in pubsub.listen():
        if message['type'] == 'message':
            # Parse the incoming event
            event_data = json.loads(message['data'])
            payload = event_data['payload']
            img_id = payload.get('image_id')
            img_path = payload.get('image_path')

            print(f" [v] Analyzing image: {img_id} at {img_path}")

            # Simulate processing latency (AI inference takes time)
            time.sleep(1.5)

            # Select 1-2 random objects from the pool to simulate detection
            detected_objects = random.sample(object_pool, random.randint(1, 2))

            # Add randomized bounding boxes [x, y, w, h] for each object
            for obj in detected_objects:
                obj["bbox"] = [random.randint(0, 100) for _ in range(4)]

            # Generate a mock vector and a numeric ID for this image
            num_id, vector = mock_vector_generation(img_id)

            # Build the enriched inference payload
            inference_results = {
                "image_id": img_id,
                "numeric_id": num_id,
                "vector": vector,
                "objects": detected_objects,
                "processed_at_path": img_path
            }

            # Create the standardized completion event
            completion_event = {
                "topic": "inference.completed",
                "event_id": f"inf_{int(time.time())}",
                "payload": inference_results,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }

            # Publish back to Redis for the DB service to consume
            r.publish("inference.completed", json.dumps(completion_event))
            print(f" [x] Inference complete for {img_id}. Data sent to DB Service.")

if __name__ == "__main__":
    start_inference_worker()
