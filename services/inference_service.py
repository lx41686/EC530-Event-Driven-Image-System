import redis
import json
import time
import random
import numpy as np
import uuid

# Configuration for Redis
REDIS_HOST = 'localhost'
REDIS_PORT = 6379


def mock_vector_generation(r, image_id: str):
    """
    Simulates an Embedding model generating a feature vector.
    """
    if image_id is None:
        raise ValueError("image_id cannot be None")

    # Generate an incremental integer ID (Required for FAISS)
    numeric_id = r.incr("global:image:id:counter")

    # Map numeric ID to the original Image ID in Redis for lookups
    r.set(f"map:id:{numeric_id}", image_id)

    # Generate a random 128-dimension vector
    vector = np.random.random(128).tolist()

    return numeric_id, vector


def start_inference_worker(run_once=False):
    """
    Subscribes to 'image.submitted', splits detections into discrete events:
    1. 'vector.generated' -> For FAISS (Vector DB)
    2. 'object.detected'  -> For MongoDB (Document DB)
    """
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe("image.submitted")

    print(f" [*] Inference Service started (run_once={run_once}). Listening...")

    object_pool = [
        {"label": "cat", "confidence": 0.95},
        {"label": "dog", "confidence": 0.92},
        {"label": "car", "confidence": 0.88},
        {"label": "bicycle", "confidence": 0.91},
        {"label": "person", "confidence": 0.97}
    ]

    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                # 1. Parse Event
                event_data = json.loads(message['data'])
                payload = event_data.get('payload', {})
                img_id = payload.get('image_id')

                if not img_id:
                    print(f" [!] Skipping corrupted event: Missing image_id")
                    if run_once: break
                    continue

                print(f" [v] Analyzing image: {img_id}")
                time.sleep(0.5)

                # --- NEW LOGIC: DETECT OBJECTS AND ASSIGN IDS ---
                detected_objects = random.sample(object_pool, random.randint(1, 2))

                # Mock latitude and longitude for the image
                lat = round(random.uniform(-90, 90), 6)
                lng = round(random.uniform(-180, 180), 6)

                # 2. Vector Generation (Target: FAISS)
                num_id, vector = mock_vector_generation(r, img_id)

                vector_event = {
                    "topic": "vector.generated",
                    "payload": {
                        "image_id": img_id,
                        "numeric_id": num_id,
                        "vector": vector
                    },
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                }
                r.publish("vector.generated", json.dumps(vector_event))
                print(f" [x] Vector event published for {img_id}.")

                # 3. Individual Object Events (Target: MongoDB)
                # We treat each detection as a discrete event as per the instructor's request
                for obj in detected_objects:
                    obj_id = f"obj_{uuid.uuid4().hex[:8]}"  # Unique Object ID

                    object_event = {
                        "topic": "object.detected",
                        "payload": {
                            "image_id": img_id,
                            "object_id": obj_id,
                            "label": obj["label"],
                            "confidence": obj["confidence"],
                            "bbox": [random.randint(0, 100) for _ in range(4)],
                            "lat_long": {"lat": lat, "lng": lng}
                        },
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    }
                    r.publish("object.detected", json.dumps(object_event))
                    print(f" [x] Object event published: {obj['label']} ({obj_id})")

                if run_once:
                    break

            except Exception as e:
                print(f" [ERROR] Unexpected failure: {e}")
                if run_once: break
                continue


if __name__ == "__main__":
    start_inference_worker(run_once=False)
