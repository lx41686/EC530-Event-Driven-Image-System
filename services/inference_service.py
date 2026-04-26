import redis
import json
import time
import random
import numpy as np

# Configuration for Redis
REDIS_HOST = 'localhost'
REDIS_PORT = 6379


def mock_vector_generation(r, image_id: str):
    """
    Simulates an Embedding model generating a feature vector.

    Args:
        r: Redis client instance.
        image_id (str): The string-based ID of the image.
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
    Subscribes to 'image.submitted', simulates AI inference,
    and handles errors gracefully.

    Args:
        run_once (bool): If True, stops after processing one message (for tests).
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
                img_path = payload.get('image_path')

                # 2. Validation Check: If data is corrupted, skip it
                if not img_id:
                    print(f" [!] Skipping corrupted event: Missing image_id")
                    if run_once: break  # Even if corrupted, exit if in test mode
                    continue

                print(f" [v] Analyzing image: {img_id}")

                # 3. Simulate processing
                time.sleep(0.5)
                detected_objects = random.sample(object_pool, random.randint(1, 2))
                for obj in detected_objects:
                    obj["bbox"] = [random.randint(0, 100) for _ in range(4)]

                # 4. Vector Generation
                num_id, vector = mock_vector_generation(r, img_id)

                # 5. Build Result
                inference_results = {
                    "image_id": img_id,
                    "numeric_id": num_id,
                    "vector": vector,
                    "objects": detected_objects,
                    "processed_at_path": img_path
                }

                completion_event = {
                    "topic": "inference.completed",
                    "event_id": f"inf_{int(time.time())}",
                    "payload": inference_results,
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                }

                r.publish("inference.completed", json.dumps(completion_event))
                print(f" [x] Inference complete for {img_id}.")

                # --- Run once and exit if this is test ---
                if run_once:
                    print(" [TEST] run_once enabled. Exiting inference worker.")
                    break

            except Exception as e:
                print(f" [ERROR] Unexpected failure: {e}")
                if run_once: break
                continue


if __name__ == "__main__":
    # Default: run_once = False
    start_inference_worker(run_once=False)
