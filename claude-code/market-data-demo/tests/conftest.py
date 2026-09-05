"""Shared fixtures: an in-memory S3 client fake and a tiny generated local tree."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

import pytest


class FakeS3Client:
    """The slice of boto3's S3 client the store uses, over a dict of keys.

    ``page_size`` forces pagination so the continuation-token loop is exercised.
    """

    def __init__(self, objects: Optional[Dict[str, bytes]] = None, page_size: int = 2) -> None:
        self.objects: Dict[str, bytes] = dict(objects or {})
        self.page_size = page_size
        self.calls: List[Dict[str, Any]] = []
        self.buckets = {"market-data"}

    # -- listing ---------------------------------------------------------------------

    def list_objects_v2(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(kwargs)
        prefix = kwargs.get("Prefix", "")
        delimiter = kwargs.get("Delimiter")
        token = kwargs.get("ContinuationToken")
        matching = sorted(key for key in self.objects if key.startswith(prefix))
        if delimiter:
            common: List[str] = []
            for key in matching:
                rest = key[len(prefix):]
                if delimiter in rest:
                    candidate = prefix + rest.split(delimiter, 1)[0] + delimiter
                    if candidate not in common:
                        common.append(candidate)
            start = int(token) if token else 0
            page = common[start : start + self.page_size]
            truncated = start + self.page_size < len(common)
            response: Dict[str, Any] = {"CommonPrefixes": [{"Prefix": p} for p in page], "IsTruncated": truncated}
            if truncated:
                response["NextContinuationToken"] = str(start + self.page_size)
            return response
        start = int(token) if token else 0
        page = matching[start : start + self.page_size]
        truncated = start + self.page_size < len(matching)
        response = {"Contents": [{"Key": key} for key in page], "IsTruncated": truncated}
        if truncated:
            response["NextContinuationToken"] = str(start + self.page_size)
        return response

    # -- writing ---------------------------------------------------------------------

    def put_object(self, Bucket: str, Key: str, Body: Any) -> None:  # noqa: N803 - boto3 casing
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.read()

    def head_bucket(self, Bucket: str) -> None:  # noqa: N803
        if Bucket not in self.buckets:
            raise RuntimeError("NoSuchBucket")

    def create_bucket(self, Bucket: str) -> None:  # noqa: N803
        self.buckets.add(Bucket)


@pytest.fixture
def fake_s3() -> FakeS3Client:
    return FakeS3Client()


@pytest.fixture
def days() -> List[dt.date]:
    # Mon 2026-08-31 .. Fri 2026-09-04 (a full week, no weekend inside).
    return [dt.date(2026, 8, 31) + dt.timedelta(days=i) for i in range(5)]


@pytest.fixture
def local_tree(tmp_path, days):
    """A generated tree: two symbols, the first three days of the week, one extra symbol dir shape."""
    pytest.importorskip("pyarrow")
    from market_data_demo.mockgen import generate

    root = tmp_path / "data"
    report = generate(root, symbols="AAPL,MSFT", start=days[0], end=days[2], seed=7)
    assert len(report.written) == 6
    # A "directory per symbol" file, the second accepted shape.
    nested = root / "2026" / "09" / "02" / "NVDA"
    nested.mkdir(parents=True)
    (nested / "part-0000.parquet").write_bytes((root / "2026" / "09" / "02" / "AAPL.parquet").read_bytes())
    # Junk that must be ignored.
    (root / "2026" / "09" / "02" / "_SUCCESS").write_text("")
    (root / "2026" / "09" / "02" / "notes.txt").write_text("ignore me")
    return root
