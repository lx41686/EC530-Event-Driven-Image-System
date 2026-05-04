import pytest
import time
import threading
import os
from pymongo import MongoClient
from services.image_service import publish_event


def test_full_pipeline():
    """
    Integration test for the new split-database architecture.
    Validates FAISS (Vector) and MongoDB (Document) storage.
    """
    # 1. Environment Setup & Data Sanitization
    mongo_client = MongoClient('localhost', 27017)
    db = mongo_client['image_system']
    # Ensure we use the object-level collection as per requirements
    collection = db['object_annotations']
    collection.delete_many({})

    # Remove stale FAISS index to ensure a clean test environment
    if os.path.exists("vector.index"):
        os.remove("vector.index")

    # 2. Service Initialization (Dynamic Import & Threading)
    # Ensure all services support the 'run_once' parameter for CI/CD exit
    from services.inference_service import start_inference_worker
    from services.db_service import start_db_service
    from services.vector_service import start_vector_service

    # Spawn background threads for the three core microservices
    inference_thread = threading.Thread(target=start_inference_worker, args=(True,), daemon=True)
    db_thread = threading.Thread(target=start_db_service, args=(True,), daemon=True)
    vector_thread = threading.Thread(target=start_vector_service, args=(True,), daemon=True)

    inference_thread.start()
    db_thread.start()
    vector_thread.start()

    # Grace period for Redis Pub/Sub subscriptions to stabilize
    time.sleep(2)

    # 3. Execution: Simulate User Input
    test_image_id = "test_img_999"
    publish_event("image.submitted", {"image_id": test_image_id, "image_path": "/path/to/test.jpg"})

    # 4. Processing Window
    # Allow sufficient time for Inference -> Messaging -> Multi-DB Storage
    time.sleep(5)

    # 5. Multimodal Verification
    # A. Validate MongoDB (Document Store)
    obj_result = collection.find_one({"image_id": test_image_id})
    assert obj_result is not None
    assert "object_id" in obj_result
    assert "location" in obj_result
    print(f" [PASSED] MongoDB stored object: {obj_result['object_id']}")

    # B. Validate FAISS (Vector Store)
    assert os.path.exists("vector.index")
    print(" [PASSED] FAISS index file created.")

    print(f"!!! Full Pipeline Test Passed: {test_image_id} processed across all DBs !!!")