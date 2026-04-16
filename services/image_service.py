import redis
import json
import time

def publish_event(topic, payload):
    # Connect to Redis (GitHub Actions Address if: localhost)
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    event = {
        "topic": topic,
        "event_id": f"evt_{int(time.time())}",
        "payload": payload,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }
    
    r.publish(topic, json.dumps(event))
    print(f" [x] Sent event to {topic}")