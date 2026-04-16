import redis
import json
import time
from pymongo import MongoClient

def start_db_worker():
    """
    Subscribes to 'inference.completed' and persists enriched metadata into MongoDB.
    """
    try:
        # Initialize MongoDB connection
        # Default connection points to localhost:27017
        mongo_client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=2000)
        db = mongo_client['image_system']
        collection = db['annotations']
        
        # Initialize Redis connection
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        pubsub = r.pubsub()
        pubsub.subscribe("inference.completed")
        
        print(" [*] DB Service started. Waiting for inference results...")
        
        # Continuous loop to listen for messages from the inference service
        for message in pubsub.listen():
            if message['type'] == 'message':
                # Parse the event data from JSON string
                event_data = json.loads(message['data'])
                payload = event_data['payload']
                
                # Prepare a structured document for MongoDB storage
                # This makes searching and auditing much easier later
                document = {
                    "image_id": payload.get('image_id'),
                    "annotations": payload.get('objects', []),
                    "event_timestamp": event_data.get('timestamp'),
                    "db_stored_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "system_version": "1.0"
                }
                
                # Persist the document into MongoDB
                collection.insert_one(document)
                print(f" [DB] Successfully stored metadata for image: {payload.get('image_id')}")
                
                # For testing and demonstration purposes, we break after the first message.
                # In a real production environment, this 'break' would be removed.
                break

    except Exception as e:
        print(f" [ERROR] DB Service encountered a failure: {e}")

if __name__ == "__main__":
    start_db_worker()