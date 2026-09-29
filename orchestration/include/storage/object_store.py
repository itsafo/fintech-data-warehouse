"""boto3 S3-compatible client for the MinIO bronze-raw archive.

Credentials come from the `minio_bronze` Airflow Connection (host/port =
the MinIO container on data_platform_net, login/password = the scoped
pipeline_writer service account Terraform creates in terraform/minio.tf --
never MinIO's root credentials).
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import boto3
from airflow.hooks.base import BaseHook

MINIO_CONN_ID = "minio_bronze"
DEFAULT_BUCKET = "bronze-raw"


def _connection():
    return BaseHook.get_connection(MINIO_CONN_ID)


def get_minio_client():
    conn = _connection()
    return boto3.client(
        "s3",
        endpoint_url=f"http://{conn.host}:{conn.port}",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="us-east-1",
    )


def archive_raw_payload(source_name: str, captured_at: datetime, payload: Any) -> str:
    """Writes the untouched API response as one JSON object. Returns its key."""
    conn = _connection()
    bucket = conn.extra_dejson.get("bucket", DEFAULT_BUCKET)
    object_key = f"{source_name}/{captured_at.isoformat()}.json"

    client = get_minio_client()
    client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=json.dumps(payload, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    return object_key
