import redis
import json
import time

def start_inference_worker():
    """
    Listens to 'image.submitted' and simulates AI object detection.
    """
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe("image.submitted")
    
    print(" [*] Inference Service started. Waiting for images...")
    for message in pubsub.listen():
        if message['type'] == 'message':
            data = json.loads(message['data'])
            img_id = data['payload']['image_id']
            
            # Simulate AI processing time
            time.sleep(1) 
            
            # Mocked AI detection results (Annotations)
            inference_results = {
                "image_id": img_id,
                "objects": [
                    {"label": "cat", "confidence": 0.95, "bbox": [10, 20, 50, 50]},
                    {"label": "tree", "confidence": 0.88, "bbox": [100, 200, 30, 40]}
                ]
            }
            
            # Publish completion event
            completion_event = {
                "topic": "inference.completed",
                "event_id": f"inf_{int(time.time())}",
                "payload": inference_results,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }
            r.publish("inference.completed", json.dumps(completion_event))
            print(f" [v] Inference completed for {img_id}")
            