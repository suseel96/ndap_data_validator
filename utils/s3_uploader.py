from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

import boto3


class S3CredentialsMode(str, Enum):
    Environment = "Use environment"
    Manual = "Enter manually"


@dataclass
class S3Uploader:
    mode: S3CredentialsMode
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    region_name: Optional[str] = None

    def _client(self):
        if self.mode == S3CredentialsMode.Manual:
            session = boto3.session.Session(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.region_name or "us-east-1",
            )
        else:
            session = boto3.session.Session(region_name=self.region_name)
        return session.client("s3")

    def upload_bytes(self, bucket: str, key: str, data_bytes: bytes, content_type: str = "application/octet-stream") -> str:
        if not bucket:
            raise ValueError("Bucket is required")
        if not key:
            raise ValueError("Key is required")
        client = self._client()
        client.put_object(Bucket=bucket, Key=key, Body=data_bytes, ContentType=content_type)
        return f"s3://{bucket}/{key}"


