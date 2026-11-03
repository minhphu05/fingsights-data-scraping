from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("finsights.storage")

# Default OCI endpoints and namespaces from terraform outputs
DEFAULT_TOKYO_ENDPOINT = "https://nryqf437jjy5.compat.objectstorage.ap-tokyo-1.oraclecloud.com"
DEFAULT_TOKYO_BUCKET = "finsights-data-assets-tokyo"
DEFAULT_TOKYO_REGION = "ap-tokyo-1"
DEFAULT_TOKYO_NAMESPACE = "nryqf437jjy5"

DEFAULT_SINGAPORE_ENDPOINT = "https://axrxyxi3tkjc.compat.objectstorage.ap-singapore-1.oraclecloud.com"
DEFAULT_SINGAPORE_BUCKET = "finsights-data-assets-singapore"
DEFAULT_SINGAPORE_REGION = "ap-singapore-1"
DEFAULT_SINGAPORE_NAMESPACE = "axrxyxi3tkjc"
NEWS_SOURCE_CODES = {"cafef", "thanhnien", "tuoitre", "baomoi", "vnexpress"}


@dataclass
class BucketTarget:
    target_id: str
    region: str
    namespace: str
    bucket_name: str
    endpoint_url: str
    access_key_id: str | None
    secret_access_key: str | None


class OciS3Uploader:
    """Multi-account S3-compatible uploader for Oracle Cloud Object Storage."""

    def __init__(self):
        self.tokyo = BucketTarget(
            target_id="tokyo",
            region=os.getenv("OCI_TOKYO_S3_REGION", DEFAULT_TOKYO_REGION),
            namespace=DEFAULT_TOKYO_NAMESPACE,
            bucket_name=os.getenv("OCI_TOKYO_S3_BUCKET_NAME", DEFAULT_TOKYO_BUCKET),
            endpoint_url=os.getenv("OCI_TOKYO_S3_ENDPOINT_URL", DEFAULT_TOKYO_ENDPOINT),
            access_key_id=os.getenv("OCI_TOKYO_S3_ACCESS_KEY_ID") or os.getenv("OCI_S3_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("OCI_TOKYO_S3_SECRET_ACCESS_KEY") or os.getenv("OCI_S3_SECRET_ACCESS_KEY"),
        )
        self.singapore = BucketTarget(
            target_id="singapore",
            region=os.getenv("OCI_SINGAPORE_S3_REGION", DEFAULT_SINGAPORE_REGION),
            namespace=DEFAULT_SINGAPORE_NAMESPACE,
            bucket_name=os.getenv("OCI_SINGAPORE_S3_BUCKET_NAME", DEFAULT_SINGAPORE_BUCKET),
            endpoint_url=os.getenv("OCI_SINGAPORE_S3_ENDPOINT_URL", DEFAULT_SINGAPORE_ENDPOINT),
            access_key_id=os.getenv("OCI_SINGAPORE_S3_ACCESS_KEY_ID") or os.getenv("OCI_S3_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("OCI_SINGAPORE_S3_SECRET_ACCESS_KEY") or os.getenv("OCI_S3_SECRET_ACCESS_KEY"),
        )
        self._clients: dict[str, Any] = {}

    def get_target(self, target_name: str = "tokyo") -> BucketTarget:
        if target_name.lower() in ("singapore", "sin", "account2"):
            return self.singapore
        return self.tokyo

    def route_source_target(self, source_code: str) -> str:
        """Route sources between Tokyo (Account 1) and Singapore (Account 2) to balance storage."""
        code = source_code.casefold()
        if code in NEWS_SOURCE_CODES or code.endswith("_news") or "news" in code:
            return "tokyo"
        return "singapore"

    def _get_s3_client(self, target: BucketTarget):
        if target.target_id in self._clients:
            return self._clients[target.target_id]

        if not target.access_key_id or not target.secret_access_key:
            return None

        try:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                region_name=target.region,
                endpoint_url=target.endpoint_url,
                aws_access_key_id=target.access_key_id,
                aws_secret_access_key=target.secret_access_key,
                config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            )
            self._clients[target.target_id] = client
            return client
        except ImportError:
            logger.warning("boto3 is not installed. S3 upload will run in dry-run mode.")
            return None

    def upload_bytes(
        self,
        data: bytes,
        object_key: str,
        mime_type: str = "application/octet-stream",
        target_name: str = "tokyo",
    ) -> dict[str, Any]:
        """Upload raw bytes to OCI Object Storage bucket and return metadata."""
        target = self.get_target(target_name)
        content_sha256 = hashlib.sha256(data).hexdigest()
        size_bytes = len(data)

        etag = None
        client = self._get_s3_client(target)
        if client is not None:
            try:
                response = client.put_object(
                    Bucket=target.bucket_name,
                    Key=object_key,
                    Body=data,
                    ContentType=mime_type,
                )
                etag = response.get("ETag", "").strip('"')
                logger.info(f"Successfully uploaded {object_key} to {target.bucket_name} ({size_bytes} bytes)")
            except Exception as e:
                logger.error(f"Failed to upload {object_key} to OCI Object Storage: {e}")
                raise
        else:
            logger.info(
                f"[Dry-run / Local] Skipping upload of {object_key} to {target.bucket_name} "
                f"(S3 credentials not configured in environment)"
            )

        return {
            "uploaded": client is not None,
            "target": target.target_id,
            "region": target.region,
            "namespace": target.namespace,
            "bucket_name": target.bucket_name,
            "object_key": object_key,
            "object_key_sha256": hashlib.sha256(object_key.encode("utf-8")).hexdigest(),
            "mime_type": mime_type,
            "size_bytes": size_bytes,
            "content_sha256": content_sha256,
            "etag": etag,
            "storage_class": "Standard",
        }

    def upload_json(
        self,
        payload: Any,
        object_key: str,
        target_name: str = "tokyo",
    ) -> dict[str, Any]:
        """Serialize payload to UTF-8 JSON and upload to bucket."""
        raw_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        return self.upload_bytes(
            data=raw_bytes,
            object_key=object_key,
            mime_type="application/json",
            target_name=target_name,
        )

