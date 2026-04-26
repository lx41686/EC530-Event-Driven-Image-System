# EC530-Event-Driven-Image-System

## Project Status:
Currently, the system has evolved from a basic messaging skeleton into a fully functional, asynchronous image processing pipeline. It now supports file uploads, handles corrupted data gracefully, and includes automated stress-testing tools (event_splitter.py).

### Architectural Decisions
- **Asynchronous Flow**: Image submission and inference are decoupled via Redis Pub/Sub to ensure high throughput and fault tolerance.
- **Document Store**: MongoDB is used for storing rich, unstructured image annotations (labels, bboxes) due to its flexible schema.
- **RESTful Entry Point (FastAPI)**: Instead of mock scripts, we use a FastAPI-based `Image Service`. This provides a standard HTTP interface for image uploads and stores files in a local persistent directory.
- **Decoupled Event Stream (Redis)**: Services communicate via Redis Pub/Sub. This ensures that the `Inference Service` can scale independently of the `Image Service`.
- **Schema Integrity**: Every event follows a strict envelope (`topic`, `event_id`, `payload`, `timestamp`). We fixed data mapping to ensure `numeric_id` and `vector` (embeddings) are preserved for next week's FAISS integration.
- **Service Robustness**: The `Inference Service` implements error-handling logic (try-except blocks) to skip corrupted payloads without crashing the entire worker.
- **Lifecycle Control**: Services support a `run_once` flag, allowing the same code to run indefinitely in production while exiting cleanly during automated Pytest runs.

### Project Structure
- `services/`: Core logic for Image Upload (FastAPI), AI Inference (Mock), and Database Persistence.
- `tests/`: End-to-end integration tests using Pytest.
- `scripts/`: Utility tools like the `event_splitter` for load testing.
- `images/`: Local storage for successfully uploaded image files.

### How to Run Tests

**1. Prerequisites**

Ensure you have **Redis** and **MongoDB** running locally:

`brew services start redis/nbrew services start mongodb-community`

**2. Environment Setup**
`conda activate ec530
pip install -r requirements.txt
export PYTHONPATH=$(pwd)`

**3. Start the Pipeline**
Open three separate terminals and run:
**Terminal 1: Image Upload Service (FastAPI)**
`uvicorn services.image_service:app --reload`
**Terminal 2: Inference Worker**
`python services/inference_service.py`
**Terminal 3: MongoDB Worker**
`python services/db_service.py`

We can now upload images via the Swagger UI at http://127.0.0.1:8000/docs.

### High-Load Stress Test
Simulate a burst of 20 image submissions (including corrupted data) to verify system stability:
`python scripts/event_splitter.py`

### TO DO:
- [ ] Integrate FAISS for vector similarity search.
- [ ] Implement a Query Service to retrieve images by label or visual similarity.
- [ ] Replace mock inference with a real lightweight CV model.
