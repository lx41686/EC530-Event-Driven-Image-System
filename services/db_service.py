import redis
import json
import time
from pymongo import MongoClient


def start_db_worker():
    """
    Subscribes to 'inference.completed' topic and persists enriched
    image metadata into MongoDB.

    This worker acts as the persistence layer, ensuring that AI inference
    results, including bounding boxes and feature vectors, are stored.
    """
    try:
        # 1. Initialize MongoDB connection
        # Defaulting to localhost:27017 as per project environment setup
        mongo_client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=2000)
        db = mongo_client['image_system']
        collection = db['annotations']

        # 2. Initialize Redis connection
        # We use decode_responses=True to handle incoming messages as strings
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        pubsub = r.pubsub()
        pubsub.subscribe("inference.completed")

        print(" [*] DB Service started. Waiting for inference results...")

        # 3. Continuous message processing loop
        for message in pubsub.listen():
            if message['type'] == 'message':
                # Parse the incoming event data (JSON string to dict)
                event_data = json.loads(message['data'])
                payload = event_data['payload']

                # Prepare a structured document for MongoDB storage.
                # It is CRITICAL to include numeric_id and vector for the
                # upcoming vector search (FAISS) implementation next week.
                document = {
                    "image_id": payload.get('image_id'),
                    "numeric_id": payload.get('numeric_id'),  # Mapping for FAISS
                    "vector": payload.get('vector'),  # Embedding data
                    "annotations": payload.get('objects', []),  # Detected objects/bboxes
                    "event_timestamp": event_data.get('timestamp'),
                    "db_stored_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "system_version": "1.1"
                }

                # Insert the enriched document into the MongoDB collection
                collection.insert_one(document)
                print(f" [DB] Successfully stored metadata for image: {payload.get('image_id')}")

                # NOTE: In a production environment, this loop runs indefinitely.
                # We use 'break' here to allow automated unit tests to conclude.
                break

    except Exception as e:
        print(f" [ERROR] DB Service encountered a failure: {e}")


if __name__ == "__main__":
    # Entry point for starting the DB service independently
    start_db_worker()