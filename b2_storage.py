"""
Backblaze B2 S3-Compatible Multi-Bucket Storage for AIRstudy / QRank.
Supports Case A: Multi-Bucket across distinct Backblaze accounts (10 GB free per account).

Bucket 1 (Primary / Existing Archive):
- Contains ~1.17 lakh historical questions. Marked full for uploads.
- Read-fallback ensures 100% uninterrupted access to all past questions.

Bucket 2 (Secondary / Active Upload Pool):
- Active upload target for new textbooks and worksheets (capacity: 1 lakh / 100,000 files).
- Preserves Turso DB quota: 0 extra row reads during image uploads; in-memory count flushed per chunk.
"""
import os
import io
import time
from typing import Optional, Tuple

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    Config = None
    ClientError = Exception

# ─── Bucket 1 Configuration (Historical / Archive) ───
B2_RAW_ENDPOINT_1 = os.getenv("B2_ENDPOINT", "https://s3.us-west-004.backblazeb2.com")
B2_ENDPOINT_1 = f"https://{B2_RAW_ENDPOINT_1}" if B2_RAW_ENDPOINT_1 and not B2_RAW_ENDPOINT_1.startswith(("http://", "https://")) else B2_RAW_ENDPOINT_1
B2_REGION_1 = os.getenv("B2_REGION", "eu-central-003")
B2_KEY_ID_1 = os.getenv("B2_KEY_ID", "")
B2_APP_KEY_1 = os.getenv("B2_APP_KEY", "")
B2_BUCKET_1 = os.getenv("B2_BUCKET_NAME") or os.getenv("B2_BUCKET", "AIRstudy")

# ─── Bucket 2 Configuration (Active Upload Target) ───
B2_RAW_ENDPOINT_2 = os.getenv("B2_ENDPOINT_2", B2_RAW_ENDPOINT_1)
B2_ENDPOINT_2 = f"https://{B2_RAW_ENDPOINT_2}" if B2_RAW_ENDPOINT_2 and not B2_RAW_ENDPOINT_2.startswith(("http://", "https://")) else B2_RAW_ENDPOINT_2
B2_REGION_2 = os.getenv("B2_REGION_2", B2_REGION_1)
B2_KEY_ID_2 = os.getenv("B2_KEY_ID_2", B2_KEY_ID_1)
B2_APP_KEY_2 = os.getenv("B2_APP_KEY_2", B2_APP_KEY_1)
B2_BUCKET_2 = os.getenv("B2_BUCKET_NAME_2", "")

# ─── Global Settings ───
B2_CDN_URL = os.getenv("B2_CDN_URL", "").rstrip("/")
B2_PRIVATE_BUCKET = os.getenv("B2_PRIVATE_BUCKET", "true").lower() in ("true", "1", "yes")
B2_PRESIGNED_EXPIRY = int(os.getenv("B2_PRESIGNED_EXPIRY", "3600"))
MAX_FILES_PER_BUCKET = int(os.getenv("B2_MAX_FILES_PER_BUCKET", "100000"))

_client_1 = None
_client_2 = None

# In-memory tracking of images uploaded in the current runner process
_uploaded_in_session = 0

def get_b2_client_1():
    global _client_1
    if _client_1 is not None:
        return _client_1
    if not boto3 or not B2_KEY_ID_1 or not B2_APP_KEY_1:
        return None
    try:
        _client_1 = boto3.client(
            "s3",
            endpoint_url=B2_ENDPOINT_1,
            region_name=B2_REGION_1,
            aws_access_key_id=B2_KEY_ID_1,
            aws_secret_access_key=B2_APP_KEY_1,
            config=Config(signature_version="s3v4")
        )
        return _client_1
    except Exception as e:
        print(f"[B2 Storage] Failed to initialize B2 client 1: {e}")
        return None

def get_b2_client_2():
    global _client_2
    if _client_2 is not None:
        return _client_2
    # If Bucket 2 credentials not provided, fall back to Account 1
    key_id = B2_KEY_ID_2 or B2_KEY_ID_1
    app_key = B2_APP_KEY_2 or B2_APP_KEY_1
    endpoint = B2_ENDPOINT_2 or B2_ENDPOINT_1
    region = B2_REGION_2 or B2_REGION_1
    if not boto3 or not key_id or not app_key:
        return None
    try:
        _client_2 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=key_id,
            aws_secret_access_key=app_key,
            config=Config(signature_version="s3v4")
        )
        return _client_2
    except Exception as e:
        print(f"[B2 Storage] Failed to initialize B2 client 2: {e}")
        return None

def get_active_upload_target():
    """
    Returns (client, bucket_name) for new uploads.
    Defaults to Bucket 2 if configured, otherwise falls back to Bucket 1.
    """
    if B2_BUCKET_2:
        c2 = get_b2_client_2()
        if c2:
            return c2, B2_BUCKET_2
    return get_b2_client_1(), B2_BUCKET_1

def is_b2_configured() -> bool:
    return bool(get_active_upload_target()[0])

def generate_presigned_url(key: str, expires_in: int = B2_PRESIGNED_EXPIRY) -> Optional[str]:
    """
    Generates secure presigned GET URL.
    Checks Bucket 2 first, then Bucket 1 if not present.
    """
    # 1. Try Bucket 2 if configured
    if B2_BUCKET_2:
        c2 = get_b2_client_2()
        if c2:
            try:
                # Fast head_object check
                c2.head_object(Bucket=B2_BUCKET_2, Key=key)
                return c2.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": B2_BUCKET_2, "Key": key},
                    ExpiresIn=expires_in
                )
            except Exception:
                pass

    # 2. Try Bucket 1 (historical archive)
    c1 = get_b2_client_1()
    if c1 and B2_BUCKET_1:
        try:
            return c1.generate_presigned_url(
                "get_object",
                Params={"Bucket": B2_BUCKET_1, "Key": key},
                ExpiresIn=expires_in
            )
        except Exception as e:
            print(f"[B2 Storage] Presigned URL error for {key}: {e}")
    return None

def get_b2_file_bytes(key: str) -> Optional[Tuple[bytes, str]]:
    """
    Fetches raw bytes directly from private B2 storage with dual-bucket fallback.
    Returns (data, content_type) or None.
    """
    # 1. Try Bucket 2 (new questions)
    if B2_BUCKET_2:
        c2 = get_b2_client_2()
        if c2:
            try:
                resp = c2.get_object(Bucket=B2_BUCKET_2, Key=key)
                return resp["Body"].read(), resp.get("ContentType", "image/png")
            except Exception:
                pass

    # 2. Try Bucket 1 (existing 1.17 lakh questions)
    c1 = get_b2_client_1()
    if c1 and B2_BUCKET_1:
        try:
            resp = c1.get_object(Bucket=B2_BUCKET_1, Key=key)
            return resp["Body"].read(), resp.get("ContentType", "image/png")
        except Exception as e:
            print(f"[B2 Storage] Bucket 1 fetch failed for {key}: {e}")

    return None

def upload_question_image_b2(image_bytes: bytes, project_id: str, question_id: str, ext: str = "png") -> str:
    """
    Uploads question image directly to active Bucket 2 (or Bucket 1 fallback).
    Tracks in-memory file count with 0 Turso row reads.
    Returns the secure proxy route.
    """
    global _uploaded_in_session
    clean_qid = question_id.replace(" ", "_").replace("*", "")
    key = f"questions/{project_id}/{clean_qid}.{ext}"

    client, bucket = get_active_upload_target()
    if client and bucket:
        try:
            content_type = "image/png" if ext.lower() == "png" else "image/jpeg"
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=image_bytes,
                ContentType=content_type
            )
            _uploaded_in_session += 1

            if B2_PRIVATE_BUCKET:
                return f"/api/images/question/{project_id}/{clean_qid}.{ext}"

            if B2_CDN_URL:
                return f"{B2_CDN_URL}/{key}"
            endpoint = B2_ENDPOINT_2 if bucket == B2_BUCKET_2 else B2_ENDPOINT_1
            return f"{endpoint.rstrip('/')}/{bucket}/{key}"
        except Exception as e:
            print(f"[B2 Storage Upload Error on {bucket}]: {e}")

    # Local fallback if credentials missing
    local_dir = os.path.join("static", "uploads", "questions", project_id)
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, f"{clean_qid}.{ext}")
    with open(local_path, "wb") as f:
        f.write(image_bytes)
    return f"/static/uploads/questions/{project_id}/{clean_qid}.{ext}"

def get_session_upload_count() -> int:
    return _uploaded_in_session
