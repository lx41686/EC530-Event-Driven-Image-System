import redis
import json

def listen_for_events(topic):
    """
    Subscribes to a Redis topic and waits for a single message.
    """
    # Initialize Redis client connection
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # Create a Pub/Sub object to manage subscriptions
    pubsub = r.pubsub()
    
    # Subscribe to the specified channel
    pubsub.subscribe(topic)
    
    # Loop to listen for messages on the subscribed channel
    for message in pubsub.listen():
        # Redis will send a confirmation message when the connection is established, we want to skip it, only process messages with type 'message'
        if message['type'] == 'message':
            # Convert the received JSON string back to a Python dictionary
            data = json.loads(message['data'])
            return data # Return immediately upon receiving a message, for unit testing purposes