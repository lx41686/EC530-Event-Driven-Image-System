import redis
import json
from pymongo import MongoClient

def start_db_worker():
    """
    Subscribes to 'inference.completed' and saves results to MongoDB.
    """
    # Initialize MongoDB connection
    # MongoDB service is expected to be running on localhost:27017
    mongo_client = MongoClient('localhost', 27017)
    db = mongo_client['image_system']
    collection = db['annotations']
    
    # Initialize Redis connection
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe("inference.completed")
    
    print(" [*] DB Service started. Waiting for inference results...")
    
    # Continuous loop to listen for messages
    for message in pubsub.listen():
        if message['type'] == 'message':
            # Parse the event data from JSON string
            event_data = json.loads(message['data'])
            payload = event_data['payload']
            
            # Persist the payload (annotations) into MongoDB collection
            collection.insert_one(payload)
            print(f" [DB] Saved annotations for {payload['image_id']} to MongoDB")
            
            # Note: In a real system, this loop would run forever.
            # For testing purposes, you might want to break after one message.
            break