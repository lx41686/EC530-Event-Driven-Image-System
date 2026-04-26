import redis
import json
import time
from pymongo import MongoClient


def start_db_worker(run_once=False):
    """
    Subscribes to 'inference.completed' topic and persists enriched
    image metadata into MongoDB.

    Args:
        run_once (bool): If True, the service will stop after processing
                         one message. Useful for unit tests.
    """
    try:
        # 1. Initialize MongoDB connection
        mongo_client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=2000)
        db = mongo_client['image_system']
        collection = db['annotations']

        # 2. Initialize Redis connection
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        pubsub = r.pubsub()
        pubsub.subscribe("inference.completed")

        print(" [*] DB Service started. Waiting for inference results...")

        # 3. Continuous message processing loop
        for message in pubsub.listen():
            if message['type'] == 'message':
                # Parse the incoming event data
                event_data = json.loads(message['data'])
                payload = event_data.get('payload', {})

                # Prepare a structured document for MongoDB storage
                document = {
                    "image_id": payload.get('image_id'),
                    "numeric_id": payload.get('numeric_id'),
                    "vector": payload.get('vector'),
                    "annotations": payload.get('objects', []),
                    "event_timestamp": event_data.get('timestamp'),
                    "db_stored_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "system_version": "1.1"
                }

                # Insert the enriched document into the MongoDB collection
                collection.insert_one(document)
                print(f" [DB] Successfully stored metadata for image: {payload.get('image_id')}")

                # CRITICAL: If we are in 'run_once' mode (for Pytest), we break.
                # Otherwise, we keep listening for the next image.
                if run_once:
                    break

    except Exception as e:
        print(f" [ERROR] DB Service encountered a failure: {e}")


if __name__ == "__main__":
    # When running as a service, we want it to run FOREVER (run_once=False)
    start_db_worker(run_once=False)