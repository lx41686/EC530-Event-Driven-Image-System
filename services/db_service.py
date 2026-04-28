import redis
import json
import time
from pymongo import MongoClient

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'image_system'
COLLECTION_NAME = 'object_annotations'  # 老师要求的 Object-level 存储


def start_db_service(run_once=False):
    """
    Subscribes to 'object.detected' and stores each object as a
    discrete document in MongoDB.

    Args:
        run_once (bool): If True, exits after processing one message (for CI/CD tests).
    """
    # 1. Initialize Redis and MongoDB
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe("object.detected")

    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]

    print(f" [*] DB Service started (run_once={run_once}). Listening for 'object.detected'...")

    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                # 2. Parse Event
                event_data = json.loads(message['data'])
                payload = event_data.get('payload', {})

                # 3. Build Document (Document DB - Object-centric)
                document = {
                    "image_id": payload.get('image_id'),
                    "object_id": payload.get('object_id'),
                    "label": payload.get('label'),
                    "confidence": payload.get('confidence'),
                    "bbox": payload.get('bbox'),
                    "location": payload.get('lat_long'),  # 包含 lat 和 lng 的地理信息
                    "system_version": "1.2",
                    "db_stored_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                }

                # 4. Store in MongoDB
                collection.insert_one(document)

                print(f" [DB] Stored object {payload.get('object_id')} ({payload.get('label')}) "
                      f"from image {payload.get('image_id')}")

                # 5. Support for automated testing
                if run_once:
                    print(" [TEST] DB Service run_once enabled. Exiting.")
                    break

            except Exception as e:
                print(f" [ERROR] DB Service failure: {e}")
                if run_once:
                    break


if __name__ == "__main__":
    # Default: Keep running
    start_db_service(run_once=False)