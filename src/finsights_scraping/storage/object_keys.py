from __future__ import annotations

import re
import uuid
from datetime import date


def new_asset_uid() -> str:
    """Stable logical identifier for one binary object or table export."""
    return str(uuid.uuid4())


def build_asset_key(*, domain: str, source_code: str, published_date: date,
                    article_uid: str, asset_uid: str, asset_kind: str, extension: str) -> str:
    safe_source = re.sub(r"[^a-z0-9-]+", "-", source_code.casefold()).strip("-")
    kind = {"image": "images", "audio": "audio", "table": "tables", "html": "html"}.get(asset_kind)
    if kind is None:
        raise ValueError(f"Unsupported object-storage asset kind: {asset_kind}")
    ext = re.sub(r"[^a-zA-Z0-9]+", "", extension).lower()
    if not ext:
        raise ValueError("A file extension is required for the storage key")
    uid = str(uuid.UUID(asset_uid))
    article = str(uuid.UUID(article_uid))
    return (
        f"raw/{domain}/{safe_source}/{published_date:%Y/%m/%d}/"
        f"{article}/{kind}/{uid}.{ext}"
    )

