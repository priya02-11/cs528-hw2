import os
import json
import time
import functions_framework
from flask import Request
from google.cloud import storage
from google.cloud import logging as cloud_logging
from google.cloud import pubsub_v1


BUCKET_NAME = os.environ.get("BUCKET_NAME", "priya-cc-hw2")

# topic for Pub/Sub + Logging 
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "evident-gecko-486418-c7")
TOPIC_ID = os.environ.get("TOPIC_ID", "hw3-error-logs")

storage_client = storage.Client()

# Cloud Logging 
try:
    logging_client = cloud_logging.Client(project=PROJECT_ID)
    logger = logging_client.logger("hw3-file-service")
except Exception as e:
    logger = None
    print(json.dumps({"severity": "WARNING", "message": f"Cloud Logging init failed: {e}"}))

# Pub/Sub 
try:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
except Exception as e:
    publisher = None
    topic_path = None
    print(json.dumps({"severity": "WARNING", "message": f"Pub/Sub init failed: {e}"}))


def publish_error(entry: dict):
    """Publish only error events to Pub/Sub (doesn't break request if publish fails)."""
    if not publisher or not topic_path:
        return
    try:
        publisher.publish(topic_path, json.dumps(entry).encode("utf-8"))
    except Exception as e:
        print(json.dumps({"severity": "ERROR", "message": f"Pub/Sub publish failed: {e}"}))


def log_struct(status, method, path, severity="INFO", error_type=None):
    entry = {
        "status": status,
        "method": method,
        "path": path,
        "error_type": error_type,
        "timestamp": time.time(),
    }

    # structured log to Cloud Logging
    if logger:
        logger.log_struct(entry, severity=severity)

    # print log 
    print(json.dumps({**entry, "severity": severity}))

    # publish errors to Pub/Sub
    if status in [404, 500, 501]:
        publish_error(entry)


@functions_framework.http
def serve_file(request: Request):
    method = request.method
    raw_path = request.path.lstrip("/")  

    # 501 for non-GET
    if method != "GET":
        log_struct(501, method, raw_path or "/", severity="ERROR", error_type="NOT_IMPLEMENTED")
        return (
            json.dumps({"error": "Not Implemented", "message": f"{method} not supported"}),
            501,
            {"Content-Type": "application/json"},
        )

    # 400 for empty path
    if not raw_path:
        log_struct(400, method, "/", severity="WARNING", error_type="EMPTY_PATH")
        return (
            json.dumps({"error": "Bad Request", "message": "No file specified"}),
            400,
            {"Content-Type": "application/json"},
        )

    # If URL includes bucket name, strip it out
    file_path = raw_path
    if raw_path.startswith(BUCKET_NAME + "/"):
        file_path = raw_path[len(BUCKET_NAME) + 1:]  # -> "19716.html"

    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(file_path)

        # 404 for non-existent file
        
        if not blob.exists(storage_client):
            log_struct(404, method, file_path, severity="ERROR", error_type="NOT_FOUND")
            return (
                json.dumps({"error": "Not Found", "message": f"{file_path} not found"}),
                404,
                {"Content-Type": "application/json"},
            )

        # 200 return file content
        content = blob.download_as_bytes()
        log_struct(200, method, file_path, severity="INFO", error_type=None)

        return (content, 200, {"Content-Type": blob.content_type or "text/html"})

    except Exception as e:
        # include the exception message in logs (helpful for debugging)
        log_struct(500, method, file_path, severity="ERROR", error_type="INTERNAL_ERROR")
        return (
            json.dumps({"error": "Internal Server Error", "message": str(e)}),
            500,
            {"Content-Type": "application/json"},
        )
