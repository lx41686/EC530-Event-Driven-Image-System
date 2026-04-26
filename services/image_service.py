import redis
import json
import time
import os
import shutil
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse

# Initialize FastAPI application with a descriptive title
app = FastAPI(title="Image Upload Service")

# Configuration for Redis connection and local storage
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
UPLOAD_DIR = "images"

# Create the local upload directory if it does not exist to prevent FileNotFoundError
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)


def publish_event(topic: str, payload: dict):
    """
    Constructs a standardized event envelope and publishes it to a Redis topic.

    Args:
        topic (str): The Redis channel name (e.g., 'image.submitted').
        payload (dict): The actual business data content to be sent.
    """
    # Initialize Redis client with automatic response decoding
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Create the standard event schema as required by project specifications
    event = {
        "topic": topic,
        "event_id": f"evt_{int(time.time())}",  # Unique event ID based on Unix timestamp
        "payload": payload,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())  # ISO 8601 UTC format
    }

    # Serialize the event dictionary to a JSON string and publish
    r.publish(topic, json.dumps(event))
    print(f" [x] Published event '{topic}' with ID {event['event_id']}")


@app.post("/upload/")
async def upload_image(file: UploadFile = File(...)):
    """
    HTTP POST endpoint that receives an image file, stores it locally,
    and triggers the downstream event-driven pipeline.

    Args:
        file (UploadFile): The image file uploaded via form-data.

    Returns:
        JSONResponse: Success status with image metadata or an error message.
    """
    try:
        # 1. Define the destination path within the local 'images/' directory
        file_path = os.path.join(UPLOAD_DIR, file.filename)

        # 2. Persist the uploaded file to the local filesystem
        # We use shutil.copyfileobj to efficiently handle the file stream
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # 3. Prepare metadata for the 'image.submitted' event
        # This payload allows downstream services (like Inference) to find the file
        payload = {
            "image_id": f"img_{int(time.time())}",
            "image_path": os.path.abspath(file_path),  # Absolute path for cross-service accessibility
            "filename": file.filename,
            "status": "stored_locally"
        }

        # 4. Trigger the first step of the asynchronous processing chain
        publish_event("image.submitted", payload)

        # Return success response to the client
        return {
            "status": "success",
            "image_id": payload["image_id"],
            "local_path": file_path
        }

    except Exception as e:
        # Catch unexpected errors and return a 500 Internal Server Error response
        return JSONResponse(
            status_code=500,
            content={"message": f"Upload process failed: {str(e)}"}
        )

# Execution instruction:
# Open terminal in the project root and run:
# uvicorn services.image_service:app --reload
    