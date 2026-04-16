import pytest
import time
import threading
from services.image_service import publish_event
from services.db_service import listen_for_events

def test_basic_pubsub():
    """
    Test case to verify the basic Publish-Subscribe functionality using Redis.
    It ensures that a message published on a specific topic is correctly 
    received by a subscriber.
    """
    topic = "test.topic"
    payload = {"message": "hello world"}
    
    # Storage for the received data from the background thread
    results = []

    def receiver():
        """
        Internal helper function to run the listener.
        """
        data = listen_for_events(topic)
        results.append(data)

    # Use a separate thread to listen for events because the listener is blocking.
    # Think of this as turning on a radio in the background to wait for a broadcast.
    thread = threading.Thread(target=receiver)
    thread.daemon = True # Ensure thread exits when the main program ends
    thread.start()
    
    # Give the subscriber a brief moment to establish a connection with Redis.
    time.sleep(1)
    
    # Trigger the event by publishing the payload to the specified topic.
    publish_event(topic, payload)
    
    # Wait for the background thread to finish processing the message.
    thread.join(timeout=5)
    
    # Assertions: Verify if the message was received and if the content is correct.
    assert len(results) > 0, "No message was received by the subscriber."
    assert results[0]['payload']['message'] == "hello world", "The received payload does not match the sent data."