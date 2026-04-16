import pytest
import time
import threading
from services.image_service import publish_image_submitted
from pymongo import MongoClient

def test_full_pipeline():
    """
    End-to-end test: Image Upload -> Inference -> MongoDB
    """
    # 1. Clear MongoDB before test
    mongo_client = MongoClient('localhost', 27017)
    db = mongo_client['image_system']
    collection = db['annotations']
    collection.delete_many({}) # Clear old data

    # 2. Start Services as background threads (In real life these are separate processes)
    from services.inference_service import start_inference_worker
    from services.db_service import start_db_worker

    # Start workers
    t1 = threading.Thread(target=start_inference_worker, daemon=True)
    t2 = threading.Thread(target=start_db_worker, daemon=True)
    t1.start()
    t2.start()
    
    time.sleep(2) # Give them time to subscribe

    # 3. Trigger the start of the chain
    publish_image_submitted("test_img_456", "/path/to/cat.jpg")

    # 4. Wait for the chain to complete
    time.sleep(3)

    # 5. Verification: Check if MongoDB has the data
    result = collection.find_one({"image_id": "test_img_456"})
    assert result is not None
    assert result['objects'][0]['label'] == "cat"
    print("!!! Full Pipeline Test Passed !!!")