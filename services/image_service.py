import redis
import json
import time

def publish_event(topic, payload):
    """
    Constructs an event with metadata and publishes it to a Redis topic.
    
    Args:
        topic (str): The name of the channel to publish to.
        payload (dict): The actual data content to be sent.
    """
    # Initialize Redis client connection
    # Localhost is used as the default host for both local dev and GitHub Actions
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # Build the event schema as required by the project specifications
    event = {
        "topic": topic,
        "event_id": f"evt_{int(time.time())}", # Unique ID based on epoch time
        "payload": payload,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()) # ISO 8601 format
    }
    
    # Serialize the dictionary to a JSON string and publish to Redis
    r.publish(topic, json.dumps(event))
    
    # Logging the action for debugging purposes
    print(f" [x] Sent event to {topic}")