import redis
import json
import time

def publish_image_submitted(image_id, image_path):
    """
    Publishes an 'image.submitted' event after a user uploads an image.
    """
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # Updated payload with real business data
    payload = {
        "image_id": image_id,
        "image_path": image_path,
        "status": "uploaded"
    }
    
    event = {
        "topic": "image.submitted",
        "event_id": f"evt_{int(time.time())}",
        "payload": payload,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    
    r.publish("image.submitted", json.dumps(event))
    print(f" [x] Event 'image.submitted' published for {image_id}")
    