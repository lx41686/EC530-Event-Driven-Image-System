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
    # 1. Setup Database & Clean up
    mongo_client = MongoClient('localhost', 27017)
    db = mongo_client['image_system']
    # 注意：这里改成了你新定义的集合名称
    collection = db['object_annotations']
    collection.delete_many({})

    # 清理旧的 FAISS 索引文件以确保测试是纯净的
    if os.path.exists("vector.index"):
        os.remove("vector.index")

    # 2. Import and Start ALL 3 Workers with 'run_once'
    # 注意：你需要确保 vector_service.py 也支持 run_once 参数（见下方说明）
    from services.inference_service import start_inference_worker
    from services.db_service import start_db_service
    from services.vector_service import start_vector_service

    # 启动三个服务线程
    inference_thread = threading.Thread(target=start_inference_worker, args=(True,), daemon=True)
    db_thread = threading.Thread(target=start_db_service, args=(True,), daemon=True)
    vector_thread = threading.Thread(target=start_vector_service, args=(True,), daemon=True)

    inference_thread.start()
    db_thread.start()
    vector_thread.start()

    time.sleep(2)  # 等待订阅就绪

    # 3. Trigger Event
    test_image_id = "test_img_999"
    publish_event("image.submitted", {"image_id": test_image_id, "image_path": "/path/to/test.jpg"})

    # 4. Wait for processing (Inference + DB + Vector storage)
    time.sleep(5)

    # 5. Verification
    # A. 验证 MongoDB (Document DB)
    obj_result = collection.find_one({"image_id": test_image_id})
    assert obj_result is not None
    assert "object_id" in obj_result
    assert "lat_long" in obj_result
    print(f" [PASSED] MongoDB stored object: {obj_result['object_id']}")

    # B. 验证 FAISS (Vector DB)
    assert os.path.exists("vector.index")
    print(" [PASSED] FAISS index file created.")

    print(f"!!! Full Pipeline Test Passed: {test_image_id} processed across all DBs !!!")