import redis
import json
import time
import random

# Redis configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379


def publish_mock_event(r, image_id, is_valid=True):
    """
    Constructs and publishes a single mock image.submitted event.

    Args:
        r: Redis client instance.
        image_id (str): Unique identifier for the image.
        is_valid (bool): If False, sends a corrupted payload to test system robustness.
    """
    topic = "image.submitted"

    if is_valid:
        # Standard valid payload
        payload = {
            "image_id": f"bulk_{image_id}",
            "image_path": f"/tmp/mock/path/image_{image_id}.jpg",
            "filename": f"image_{image_id}.jpg",
            "status": "batch_injected"
        }
    else:
        # Corrupted payload (missing critical fields) to test error handling
        payload = {
            "error_test": "this_is_invalid_data",
            "note": "testing_system_stability"
        }

    event = {
        "topic": topic,
        "event_id": f"evt_bulk_{int(time.time())}_{image_id}",
        "payload": payload,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

    r.publish(topic, json.dumps(event))
    print(f" [Splitter] Sent {'VALID' if is_valid else 'INVALID'} event: {image_id}")


def run_stress_test(total_events=20):
    """
    Injects a burst of events into the Redis pipeline to simulate high load.
    """
    print(f"--- Starting Event Splitter Stress Test ({total_events} events) ---")

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

        for i in range(total_events):
            # 90% chance of valid data, 10% chance of invalid data
            is_valid = random.random() > 0.1
            publish_mock_event(r, i, is_valid=is_valid)

            # Small delay to simulate rapid-fire arrival without overwhelming the console
            time.sleep(0.1)

        print(f"--- Stress Test Finished: {total_events} events pushed ---")

    except redis.ConnectionError:
        print(" [ERROR] Could not connect to Redis. Is it running?")


if __name__ == "__main__":
    # Execute the stress test with 20 events by default
    run_stress_test(20)