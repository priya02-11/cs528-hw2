import os
import json
import time
import functions_framework
from flask import Request
from google.cloud import storage
from google.cloud import logging as cloud_logging

BUCKET_NAME = "priya-cc-hw2"

# Clients
storage_client = storage.Client()
logging_client = cloud_logging.Client()
logger = logging_client.logger("hw3-file-service")


def log_struct(status, method, path, severity="INFO", error_type=None):
    entry = {
        "status": status,
        "method": method,
        "path": path,
        "error_type": error_type,
        "timestamp": time.time(),
    }
    # structured log to Cloud Logging
    logger.log_struct(entry, severity=severity)
    # print log (also captured by cloud logging)
    print(json.dumps({**entry, "severity": severity}))


@functions_framework.http
def serve_file(request: Request):
    method = request.method
    raw_path = request.path.lstrip("/")  
    # Only GET is supported
    if method != "GET":
        log_struct(501, method, raw_path or "/", severity="ERROR", error_type="NOT_IMPLEMENTED")
        return (
            json.dumps({"error": "Not Implemented", "message": f"{method} not supported"}),
            501,
            {"Content-Type": "application/json"},
        )

    # Empty path -> 400
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
        file_path = raw_path[len(BUCKET_NAME) + 1:]  

    # Fetch file from GCS bucket
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(file_path)

        if not blob.exists():
            log_struct(404, method, file_path, severity="ERROR", error_type="NOT_FOUND")
            return (
                json.dumps({"error": "Not Found", "message": f"{file_path} not found"}),
                404,
                {"Content-Type": "application/json"},
            )

        content = blob.download_as_bytes()
        log_struct(200, method, file_path, severity="INFO")

        return (content, 200, {"Content-Type": blob.content_type or "text/html"})

    except Exception as e:
        log_struct(500, method, file_path, severity="ERROR", error_type="INTERNAL_ERROR")
        return (
            json.dumps({"error": "Internal Server Error", "message": str(e)}),
            500,
            {"Content-Type": "application/json"},
        )


   
