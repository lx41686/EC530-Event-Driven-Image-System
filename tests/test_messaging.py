import pytest
import time
import threading
from pymongo import MongoClient
from services.image_service import publish_event


def test_full_pipeline():
    """
    Integration test for the entire pipeline.
    Ensures that services can process a single event and exit gracefully.
    """
    # 1. Setup Database
    mongo_client = MongoClient('localhost', 27017)
    db = mongo_client['image_system']
    collection = db['annotations']
    collection.delete_many({})

    # 2. Start Workers with 'run_once' flag
    from services.inference_service import start_inference_worker
    from services.db_service import start_db_worker

    # IMPORTANT: We pass args=(True,) to the worker functions.
    # This sets run_once=True, ensuring the threads exit after one message.
    # Note: Ensure your inference_service.py also accepts this parameter!
    inference_thread = threading.Thread(target=start_inference_worker, args=(True,), daemon=True)
    db_thread = threading.Thread(target=start_db_worker, args=(True,), daemon=True)

    inference_thread.start()
    db_thread.start()

    time.sleep(2)

    # 3. Trigger Event
    test_image_id = "test_img_456"
    publish_event("image.submitted", {"image_id": test_image_id, "image_path": "/path/to/cat.jpg"})

    # 4. Wait for processing
    time.sleep(4)

    # 5. Verification
    result = collection.find_one({"image_id": test_image_id})
    assert result is not None
    print(f"!!! Test Passed: {test_image_id} stored successfully !!!")

    # Because of run_once=True, the threads will finish now,
    # allowing Pytest to exit cleanly.