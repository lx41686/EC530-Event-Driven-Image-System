import redis
import json
import time
from pymongo import MongoClient

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'image_system'
COLLECTION_NAME = 'object_annotations'  # 建议换个名字，强调存储的是 Object


def start_db_service():
    """
    Subscribes to 'object.detected' and stores each object as a
    discrete document in MongoDB.
    """
    # 1. Initialize Redis and MongoDB
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe("object.detected")

    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]

    print(f" [*] DB Service started. Listening for 'object.detected' events...")

    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                # 2. Parse Event
                event_data = json.loads(message['data'])
                payload = event_data.get('payload', {})

                # 3. Build Document (The "Document DB" way)
                # Each object detection is a unique entry
                document = {
                    "image_id": payload.get('image_id'),
                    "object_id": payload.get('object_id'),
                    "label": payload.get('label'),
                    "confidence": payload.get('confidence'),
                    "bbox": payload.get('bbox'),
                    "location": payload.get('lat_long'),  # 包含 lat 和 lng
                    "system_version": "1.2",
                    "db_stored_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                }

                # 4. Store in MongoDB
                result = collection.insert_one(document)

                print(f" [DB] Stored object {payload.get('object_id')} ({payload.get('label')}) "
                      f"from image {payload.get('image_id')}")

            except Exception as e:
                print(f" [ERROR] DB Service failure: {e}")


if __name__ == "__main__":
    start_db_service()