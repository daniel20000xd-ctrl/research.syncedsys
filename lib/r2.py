"""Cloudflare R2 client — Python mirror of the photo pipeline's `lib/r2.ts`.

Same env vars and conventions as the main Syncedsys app so both write to the
one bucket:
  CF_ACCOUNT_ID         Cloudflare account id (endpoint host)
  R2_ACCESS_KEY_ID      R2 token access key
  R2_SECRET_ACCESS_KEY  R2 token secret
  R2_BUCKET_NAME        bucket (default 'syncedsys-storage')

R2 speaks the S3 API, so we use boto3 with region 'auto' and the account's
r2.cloudflarestorage.com endpoint — exactly the (region, endpoint, creds) triple
`getR2Client()` builds in r2.ts.
"""
from __future__ import annotations

import os

import boto3
from botocore.config import Config

R2_BUCKET = os.environ.get("R2_BUCKET_NAME") or "syncedsys-storage"

_client = None


def is_configured() -> bool:
    """True only if every R2 credential is present (else PDFs can't be stored)."""
    return all(
        os.environ.get(k)
        for k in ("CF_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")
    )


def get_client():
    """Return a cached, module-level boto3 S3 client pointed at R2."""
    global _client
    if _client is None:
        account = os.environ.get("CF_ACCOUNT_ID")
        access_key = os.environ.get("R2_ACCESS_KEY_ID")
        secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
        if not (account and access_key and secret_key):
            raise RuntimeError(
                "R2 not configured: set CF_ACCOUNT_ID, R2_ACCESS_KEY_ID and "
                "R2_SECRET_ACCESS_KEY (see .env.example)."
            )
        _client = boto3.client(
            "s3",
            region_name="auto",
            endpoint_url=f"https://{account}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )
    return _client


def upload_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    """Upload bytes to R2 under `key`; return the key (stored as a *_path column)."""
    get_client().put_object(Bucket=R2_BUCKET, Key=key, Body=data, ContentType=content_type)
    return key


def upload_text(key: str, text: str) -> str:
    """Upload UTF-8 text to R2 under `key`; return the key (stored as full_text_path)."""
    return upload_bytes(key, text.encode("utf-8"), "text/plain; charset=utf-8")


def download_bytes(key: str) -> bytes:
    """Fetch an object's bytes from R2."""
    return get_client().get_object(Bucket=R2_BUCKET, Key=key)["Body"].read()


def download_text(key: str) -> str:
    """Fetch an object from R2 and decode it as UTF-8 text."""
    return download_bytes(key).decode("utf-8", errors="replace")


def object_exists(key: str) -> bool:
    """True if an object already lives at `key` (lets us skip re-uploading PDFs)."""
    try:
        get_client().head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except Exception:  # noqa: BLE001 — 404 (and anything else) means "re-upload"
        return False
