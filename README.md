# EC530-Event-Driven-Image-System

## Project Status: Messaging & Infrastructure Completed

### Architectural Decisions
- **Asynchronous Flow**: Image submission and inference are decoupled via Redis Pub/Sub to ensure high throughput and fault tolerance.
- **Document Store**: MongoDB is used for storing rich, unstructured image annotations (labels, bboxes) due to its flexible schema.
- **Event Schema**: All messages follow a standard envelope including `topic`, `event_id`, `payload`, and `timestamp`.

### How to Run Tests
1. Ensure Redis and MongoDB are running.
2. `conda activate ec530`
3. `export PYTHONPATH=$(pwd)`
4. `python -m pytest`
