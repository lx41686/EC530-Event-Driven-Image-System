import redis
import json

def listen_for_events(topic):
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe(topic)
    
    print(f" [*] Waiting for messages in {topic}. To exit press CTRL+C")
    for message in pubsub.listen():
        if message['type'] == 'message':
            data = json.loads(message['data'])
            print(f" [v] Received event: {data['event_id']}")
            # Example: Return the data for testing purposes. In a real application, you might process it instead.
            return data 